"""
CTIS (EU Clinical Trials Information System) client.

Uses the public CTIS endpoints:
- POST /ctis-public-api/search (trial overview + pagination)
- GET  /ctis-public-api/retrieve/{ctNumber} (full trial detail)

The output is normalized to the same shape used by ClinicalTrials.gov
ingestion so both sources can be stored in the same local tables.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime
import re
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

from ingest.clinicaltrials import (
    _extract_pubmed_pmids,
    classify_study,
    is_pdac_core,
    pdac_match_reason,
)


CTIS_SEARCH_URL = "https://euclinicaltrials.eu/ctis-public-api/search"
CTIS_RETRIEVE_URL = "https://euclinicaltrials.eu/ctis-public-api/retrieve/{ct_number}"
CTIS_TRIAL_URL = (
    "https://euclinicaltrials.eu/search-for-clinical-trials/?lang=en&EUCT={ct_number}"
)
PUBMED_ESEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"

REQUEST_TIMEOUT = 45
RETRYABLE_STATUS_CODES = {403, 429, 500, 502, 503, 504}
DEFAULT_CTIS_PDAC_QUERY_TERMS = [
    "pancreatic",
    "pancreas",
    "pancreatic cancer",
    "pdac",
    "pancreatic adenocarcinoma",
    "ductal adenocarcinoma",
    "paad",
    "pancreatic ductal adenocarcinoma",
]
CTIS_ADDITIONAL_FOCUS_RULES = {
    "mixed_solid_tumor": [
        "solid tumor",
        "solid tumours",
        "pan-tumor",
        "pan tumor",
        "pan-tumour",
        "pan tumour",
        "basket trial",
    ],
    "neuroendocrine_signal": [
        "neuroendocrine",
        "gep-net",
        "gastro-entero-pancreatic",
        "gastro entero pancreatic",
        "pnet",
    ],
    "hepatobiliary_signal": [
        "hepatocellular",
        "cholangio",
        "hepatobiliary",
        "liver cancer",
    ],
}

CTIS_SEARCH_PAYLOAD_TEMPLATE: Dict[str, Any] = {
    "pagination": {"page": 1, "size": 20},
    "sort": {"property": "decisionDate", "direction": "DESC"},
    "searchCriteria": {
        "containAll": None,
        "containAny": None,
        "containNot": None,
        "title": None,
        "number": None,
        "status": None,
        "medicalCondition": None,
        "sponsor": None,
        "endPoint": None,
        "productName": None,
        "productRole": None,
        "populationType": None,
        "orphanDesignation": None,
        "msc": None,
        "ageGroupCode": None,
        "therapeuticAreaCode": None,
        "trialPhaseCode": None,
        "sponsorTypeCode": None,
        "gender": None,
        "protocolCode": None,
        "rareDisease": None,
        "pip": None,
        "haveOrphanDesignation": None,
        "hasStudyResults": None,
        "hasClinicalStudyReport": None,
        "isLowIntervention": None,
        "hasSeriousBreach": None,
        "hasUnexpectedEvent": None,
        "hasUrgentSafetyMeasure": None,
        "isTransitioned": None,
        "eudraCtCode": None,
        "trialRegion": None,
        "vulnerablePopulation": None,
        "mscStatus": None,
    },
}


def _clean(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _request_json(
    method: str,
    url: str,
    *,
    payload: Optional[Dict[str, Any]] = None,
    retries: int = 4,
) -> Dict[str, Any]:
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": "pdac-trial-atlas/1.3 (+local-ingestion)",
    }
    last_error: Optional[Exception] = None

    for attempt in range(retries):
        try:
            if method.upper() == "POST":
                response = requests.post(
                    url,
                    json=payload,
                    headers=headers,
                    timeout=REQUEST_TIMEOUT,
                )
            else:
                response = requests.get(
                    url,
                    headers=headers,
                    timeout=REQUEST_TIMEOUT,
                )

            if response.status_code in RETRYABLE_STATUS_CODES and attempt < retries - 1:
                time.sleep(1.2 * (attempt + 1))
                continue

            response.raise_for_status()
            return response.json()
        except Exception as exc:
            last_error = exc
            if attempt < retries - 1:
                time.sleep(1.2 * (attempt + 1))
                continue
            raise

    if last_error:
        raise last_error
    return {}


def _join_non_empty(values: Iterable[str], sep: str = " | ") -> str:
    out = []
    for value in values:
        text = _clean(value)
        if text:
            out.append(text)
    return sep.join(out)


def _resolve_query_terms(
    query_terms: Optional[List[str]] = None,
    medical_condition: Optional[str] = None,
) -> List[str]:
    if query_terms:
        terms = _uniq([_clean(term) for term in query_terms if _clean(term)])
        if terms:
            return terms
    if _clean(medical_condition):
        return [_clean(medical_condition)]
    return list(DEFAULT_CTIS_PDAC_QUERY_TERMS)


def _extract_additional_focus_tags(text: str) -> List[str]:
    lower = _clean(text).lower()
    tags = []
    for tag, terms in CTIS_ADDITIONAL_FOCUS_RULES.items():
        if any(term in lower for term in terms):
            tags.append(tag)
    return tags


def _uniq(values: Iterable[str]) -> List[str]:
    seen = set()
    out = []
    for value in values:
        text = _clean(value)
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def _nested(data: Any, path: List[Any], default: Any = "") -> Any:
    cur = data
    for key in path:
        if isinstance(cur, dict):
            cur = cur.get(key, default)
        elif isinstance(cur, list) and isinstance(key, int):
            if key < 0 or key >= len(cur):
                return default
            cur = cur[key]
        else:
            return default
        if cur is None:
            return default
    return cur


def normalize_ctis_date(value: Any) -> str:
    """
    Convert CTIS date-like values to YYYY-MM-DD when possible.
    """
    text = _clean(value)
    if not text:
        return ""

    # Typical CTIS overview format
    for fmt in ("%d/%m/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass

    # ISO timestamps in detail payload.
    try:
        iso = text.replace("Z", "+00:00")
        return datetime.fromisoformat(iso).date().isoformat()
    except ValueError:
        return text if len(text) == 10 and text[4] == "-" else ""


def normalize_ctis_phase(value: Any) -> str:
    text = _clean(value).lower()
    if not text:
        return "NA"

    ordered = []
    phase_map = [
        (r"\bphase\s*iv\b", "PHASE4"),
        (r"\bphase\s*iii\b", "PHASE3"),
        (r"\bphase\s*ii\b", "PHASE2"),
        (r"\bphase\s*i\b", "PHASE1"),
    ]
    for pattern, norm in phase_map:
        if re.search(pattern, text) and norm not in ordered:
            ordered.append(norm)

    if ordered:
        rank = {"PHASE1": 1, "PHASE2": 2, "PHASE3": 3, "PHASE4": 4}
        ordered = sorted(ordered, key=lambda item: rank.get(item, 99))
        return "/".join(ordered)
    return _clean(value).upper().replace(" ", "_")


def _map_ctis_study_type(phase_text: str, trial_category: str) -> str:
    merged = f"{_clean(phase_text)} {_clean(trial_category)}".lower()
    if "observational" in merged or "non-interventional" in merged:
        return "OBSERVATIONAL"
    if merged:
        return "INTERVENTIONAL"
    return "UNKNOWN"


def _is_pdac_candidate(text: str) -> bool:
    lower = _clean(text).lower()
    if not lower:
        return False

    if is_pdac_core(lower):
        return True

    has_pancreas = ("pancrea" in lower) or ("pdac" in lower)
    has_cancer_signal = any(
        token in lower
        for token in (
            "cancer",
            "adenocarcinoma",
            "carcinoma",
            "neoplasm",
            "tumor",
            "tumour",
        )
    )
    if not (has_pancreas and has_cancer_signal):
        return False

    if "pancreatitis" in lower and not has_cancer_signal:
        return False
    return True


def _pick_text_or_translation(item: Dict[str, Any], value_key: str, translation_key: str) -> str:
    direct = _clean(item.get(value_key))
    if direct:
        return direct

    for tr in item.get(translation_key, []) or []:
        translated = _clean(tr.get("attributeTranslation"))
        if translated:
            return translated
    return ""


def _extract_secondary_nct(details: Dict[str, Any]) -> str:
    sec = _nested(
        details,
        [
            "authorizedApplication",
            "authorizedPartI",
            "trialDetails",
            "clinicalTrialIdentifiers",
            "secondaryIdentifyingNumbers",
            "nctNumber",
        ],
        {},
    )
    if isinstance(sec, dict):
        return _clean(sec.get("number"))
    return ""


def _extract_pubmed_links(details: Dict[str, Any]) -> str:
    trial_details = _nested(
        details,
        ["authorizedApplication", "authorizedPartI", "trialDetails"],
        {},
    )
    urls = []
    for raw in trial_details.get("pubmedUrl", []) or []:
        link = _clean(raw)
        if not link:
            continue
        if not link.startswith("http"):
            link = f"https://pubmed.ncbi.nlm.nih.gov/{link.strip('/')}/"
        urls.append(link)
    for code in trial_details.get("pubmedCode", []) or []:
        pmid = _clean(code)
        if pmid.isdigit():
            urls.append(f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/")
    return _join_non_empty(_uniq(urls))


def _extract_pubmed_links_from_references(details: Dict[str, Any]) -> str:
    trial_details = _nested(
        details,
        ["authorizedApplication", "authorizedPartI", "trialDetails"],
        {},
    )
    references = trial_details.get("references", []) or []
    links = []
    for ref in references:
        if not isinstance(ref, dict):
            continue
        raw_text = " ".join(
            [
                _clean(ref.get("reference")),
                _clean(ref.get("url")),
                _clean(ref.get("title")),
                _clean(ref.get("citation")),
                _clean(ref.get("doi")),
            ]
        )
        if not raw_text:
            continue

        # PMID patterns.
        for pmid in re.findall(r"(?:pmid\s*[:#]?\s*|pubmed\/)(\d{5,10})", raw_text, flags=re.I):
            links.append(f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/")

        # DOI patterns.
        for doi in re.findall(r"(10\.\d{4,9}/[-._;()/:A-Z0-9]+)", raw_text, flags=re.I):
            links.append(f"https://doi.org/{doi}")

    return _join_non_empty(_uniq(links))


def _fetch_pubmed_links_by_title(title: str, max_links: int = 3) -> str:
    """
    Best-effort PubMed lookup for pure CTIS rows without NCT correlation.
    """
    text = _clean(title)
    if not text:
        return ""
    # Keep query bounded and specific.
    query = f"\"{text}\"[Title] AND (pancreatic OR pancreas OR PDAC)"
    try:
        resp = requests.get(
            PUBMED_ESEARCH_URL,
            params={
                "db": "pubmed",
                "retmode": "json",
                "retmax": max_links,
                "term": query,
            },
            timeout=20,
        )
        resp.raise_for_status()
        pmids = _extract_pubmed_pmids(resp.json())
        links = [f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/" for pmid in pmids]
        return _join_non_empty(_uniq(links))
    except Exception:
        return ""


def _extract_conditions(overview: Dict[str, Any], details: Dict[str, Any]) -> str:
    values = []
    values.append(_clean(overview.get("conditions")))

    part_i = _nested(details, ["authorizedApplication", "authorizedPartI"], {})
    for item in part_i.get("medicalConditions", []) or []:
        values.append(_pick_text_or_translation(item, "medicalCondition", "medicalConditionTranslations"))

    part_i_conditions = _nested(
        part_i,
        ["trialDetails", "trialInformation", "medicalCondition", "partIMedicalConditions"],
        [],
    )
    for item in part_i_conditions or []:
        if not isinstance(item, dict):
            continue
        values.append(_pick_text_or_translation(item, "medicalCondition", "medicalConditionTranslations"))

    return _join_non_empty(_uniq(values))


def _map_product_type(product: Dict[str, Any]) -> str:
    dictionary = product.get("productDictionaryInfo", {}) or {}
    substances = dictionary.get("productSubstances", []) or []
    origins = [_clean(x.get("actSubstOrigin") or x.get("substanceOrigin")).lower() for x in substances]
    if any("biological" in origin for origin in origins):
        return "BIOLOGICAL"
    if product.get("devices"):
        return "DEVICE"
    if dictionary.get("prodName") or product.get("productName"):
        return "DRUG"
    return "OTHER"


def _extract_interventions(overview: Dict[str, Any], details: Dict[str, Any]) -> Tuple[str, str]:
    values: List[str] = []
    types: List[str] = []
    part_i = _nested(details, ["authorizedApplication", "authorizedPartI"], {})

    for product in part_i.get("products", []) or []:
        if not isinstance(product, dict):
            continue
        intervention_type = _map_product_type(product)
        if intervention_type:
            types.append(intervention_type)

        dictionary = product.get("productDictionaryInfo", {}) or {}
        name = _clean(
            product.get("productName")
            or dictionary.get("prodName")
            or product.get("otherMedicinalProduct")
            or product.get("jsonActiveSubstanceNames")
        )
        if name:
            values.append(f"{intervention_type}: {name}" if intervention_type else name)

        for device in product.get("devices", []) or []:
            if not isinstance(device, dict):
                continue
            device_name = _clean(device.get("name") or device.get("deviceName"))
            if device_name:
                values.append(f"DEVICE: {device_name}")
                types.append("DEVICE")

    overview_product = _clean(overview.get("product"))
    if overview_product:
        if not any(overview_product in item for item in values):
            values.append(f"DRUG: {overview_product}")
            types.append("DRUG")

    unique_types = _uniq(types)
    return _join_non_empty(_uniq(values)), _join_non_empty(unique_types, sep=", ")


def _extract_endpoints(overview: Dict[str, Any], details: Dict[str, Any]) -> Tuple[str, str]:
    primary: List[str] = []
    secondary: List[str] = []

    trial_info = _nested(
        details,
        ["authorizedApplication", "authorizedPartI", "trialDetails", "trialInformation"],
        {},
    )
    endpoint = trial_info.get("endPoint", {}) or {}

    for item in endpoint.get("primaryEndPoints", []) or []:
        if not isinstance(item, dict):
            continue
        primary.append(_pick_text_or_translation(item, "endPoint", "endPointTranslations"))

    for item in endpoint.get("secondaryEndPoints", []) or []:
        if not isinstance(item, dict):
            continue
        secondary.append(_pick_text_or_translation(item, "endPoint", "endPointTranslations"))

    if not primary:
        primary.append(_clean(overview.get("primaryEndPoint")))
    if not secondary:
        secondary.append(_clean(overview.get("endPoint")))

    return _join_non_empty(_uniq(primary)), _join_non_empty(_uniq(secondary))


def _extract_eligibility(details: Dict[str, Any]) -> Tuple[str, str]:
    criteria = _nested(
        details,
        [
            "authorizedApplication",
            "authorizedPartI",
            "trialDetails",
            "trialInformation",
            "eligibilityCriteria",
        ],
        {},
    )
    inclusion = []
    exclusion = []

    for item in criteria.get("principalInclusionCriteria", []) or []:
        if not isinstance(item, dict):
            continue
        inclusion.append(
            _pick_text_or_translation(
                item,
                "principalInclusionCriteria",
                "principalInclusionCriteriaTranslations",
            )
        )

    for item in criteria.get("principalExclusionCriteria", []) or []:
        if not isinstance(item, dict):
            continue
        exclusion.append(
            _pick_text_or_translation(
                item,
                "principalExclusionCriteria",
                "principalExclusionCriteriaTranslations",
            )
        )

    return _join_non_empty(_uniq(inclusion)), _join_non_empty(_uniq(exclusion))


def _extract_locations(details: Dict[str, Any]) -> str:
    values = []
    parts_ii = _nested(details, ["authorizedApplication", "authorizedPartsII"], [])
    for part in parts_ii or []:
        if not isinstance(part, dict):
            continue
        for site in part.get("trialSites", []) or []:
            if not isinstance(site, dict):
                continue
            org = _nested(site, ["organisationAddressInfo", "organisation", "name"], "")
            city = _nested(site, ["organisationAddressInfo", "address", "city"], "")
            country = _nested(site, ["organisationAddressInfo", "address", "countryName"], "")
            place = ", ".join([x for x in [_clean(city), _clean(country)] if x])
            if _clean(org) and place:
                values.append(f"{_clean(org)} ({place})")
            elif _clean(org):
                values.append(_clean(org))
            elif place:
                values.append(place)
    return _join_non_empty(_uniq(values))


def _extract_titles_and_summaries(overview: Dict[str, Any], details: Dict[str, Any]) -> Tuple[str, str, str]:
    trial_details = _nested(
        details,
        ["authorizedApplication", "authorizedPartI", "trialDetails"],
        {},
    )
    identifiers = trial_details.get("clinicalTrialIdentifiers", {}) or {}
    trial_info = trial_details.get("trialInformation", {}) or {}
    objective = trial_info.get("trialObjective", {}) or {}

    title = _clean(
        identifiers.get("publicTitle")
        or identifiers.get("fullTitle")
        or overview.get("ctTitle")
    )
    brief_summary = _join_non_empty(
        [
            identifiers.get("publicTitle"),
            identifiers.get("shortTitle"),
            overview.get("ctTitle"),
        ]
    )
    detailed_description = _join_non_empty(
        [
            identifiers.get("fullTitle"),
            objective.get("mainObjective"),
            _join_non_empty(
                [
                    _clean(item.get("secondaryObjective"))
                    for item in objective.get("secondaryObjectives", []) or []
                    if isinstance(item, dict)
                ]
            ),
        ]
    )
    return title, brief_summary, detailed_description


def _extract_sponsor(overview: Dict[str, Any], details: Dict[str, Any]) -> str:
    sponsors = _nested(details, ["authorizedApplication", "authorizedPartI", "sponsors"], [])
    for sponsor in sponsors or []:
        if not isinstance(sponsor, dict):
            continue
        name = _nested(sponsor, ["organisation", "name"], "")
        if _clean(name):
            return _clean(name)
    return _clean(overview.get("sponsor"))


def _normalize_results_flag(overview: Dict[str, Any], details: Dict[str, Any]) -> str:
    value = _clean(overview.get("resultsFirstReceived")).lower()
    if value in {"yes", "true"}:
        return "yes"
    if value in {"no", "false"}:
        return "no"

    results = details.get("results", {}) or {}
    if (results.get("summaryResults") or []) or (results.get("laypersonResults") or []):
        return "yes"
    return "no"


def _build_classification_text(
    overview: Dict[str, Any],
    title: str,
    conditions: str,
    interventions: str,
    primary_outcomes: str,
    secondary_outcomes: str,
    detailed_description: str,
) -> str:
    blocks = [
        _clean(title),
        _clean(overview.get("ctTitle")),
        _clean(overview.get("conditions")),
        conditions,
        interventions,
        primary_outcomes,
        secondary_outcomes,
        detailed_description,
        _join_non_empty(overview.get("therapeuticAreas", []) or []),
        _clean(overview.get("product")),
    ]
    return _join_non_empty(blocks, sep=" ")


def iter_ctis_overviews(
    medical_condition: Optional[str] = None,
    query_terms: Optional[List[str]] = None,
    page_size: int = 100,
    max_records: Optional[int] = None,
) -> Iterable[Dict[str, Any]]:
    yielded = 0
    seen_ct_numbers = set()
    terms = _resolve_query_terms(query_terms=query_terms, medical_condition=medical_condition)

    for term in terms:
        page = 1
        while True:
            payload = deepcopy(CTIS_SEARCH_PAYLOAD_TEMPLATE)
            payload["pagination"]["page"] = page
            payload["pagination"]["size"] = page_size
            payload["searchCriteria"]["medicalCondition"] = term

            data = _request_json("POST", CTIS_SEARCH_URL, payload=payload)

            for trial in data.get("data", []) or []:
                if not isinstance(trial, dict):
                    continue
                ct_number = _clean(trial.get("ctNumber"))
                if not ct_number or ct_number in seen_ct_numbers:
                    continue
                seen_ct_numbers.add(ct_number)
                yield trial
                yielded += 1
                if max_records is not None and yielded >= max_records:
                    return

            if not data.get("pagination", {}).get("nextPage"):
                break
            page += 1


def fetch_ctis_trial_detail(ct_number: str) -> Dict[str, Any]:
    return _request_json("GET", CTIS_RETRIEVE_URL.format(ct_number=ct_number))


def fetch_trials_ctis_pdac(
    max_trials: Optional[int] = None,
    max_overview_records: Optional[int] = None,
    medical_condition: Optional[str] = None,
    query_terms: Optional[List[str]] = None,
    page_size: int = 100,
) -> List[Dict[str, str]]:
    """
    Fetch and normalize PDAC-relevant trials from CTIS.
    """
    normalized = []

    for overview in iter_ctis_overviews(
        medical_condition=medical_condition,
        query_terms=query_terms,
        page_size=page_size,
        max_records=max_overview_records,
    ):
        ct_number = _clean(overview.get("ctNumber"))
        if not ct_number:
            continue

        rough_text = _join_non_empty(
            [
                overview.get("ctTitle"),
                overview.get("conditions"),
                _join_non_empty(overview.get("therapeuticAreas", []) or []),
                overview.get("product"),
            ],
            sep=" ",
        )
        if not _is_pdac_candidate(rough_text):
            continue

        try:
            details = fetch_ctis_trial_detail(ct_number)
        except Exception:
            # Keep ingestion resilient: skip transiently broken detail rows.
            continue

        title, brief_summary, detailed_description = _extract_titles_and_summaries(overview, details)
        conditions = _extract_conditions(overview, details)
        interventions, intervention_types = _extract_interventions(overview, details)
        primary_outcomes, secondary_outcomes = _extract_endpoints(overview, details)
        inclusion_criteria, exclusion_criteria = _extract_eligibility(details)
        locations = _extract_locations(details)

        classification_text = _build_classification_text(
            overview=overview,
            title=title,
            conditions=conditions,
            interventions=interventions,
            primary_outcomes=primary_outcomes,
            secondary_outcomes=secondary_outcomes,
            detailed_description=detailed_description,
        )
        if not _is_pdac_candidate(classification_text):
            continue

        overview_phase = _clean(overview.get("trialPhase"))
        detail_phase = _clean(
            _nested(
                details,
                [
                    "authorizedApplication",
                    "authorizedPartI",
                    "trialDetails",
                    "trialInformation",
                    "trialCategory",
                    "trialPhase",
                ],
                "",
            )
        )
        phase_raw = overview_phase
        if ("phase" not in overview_phase.lower()) and detail_phase:
            phase_raw = detail_phase
        study_type = _map_ctis_study_type(
            phase_raw,
            _nested(
                details,
                [
                    "authorizedApplication",
                    "authorizedPartI",
                    "trialDetails",
                    "trialInformation",
                    "trialCategory",
                    "trialCategory",
                ],
                "",
            ),
        )
        classification = classify_study(study_type, classification_text)
        additional_focus = _extract_additional_focus_tags(classification_text)
        if additional_focus:
            classification["focus"] = _uniq(classification.get("focus", []) + additional_focus)
        match_reason = pdac_match_reason(classification_text)
        if match_reason == "unknown_match" and "pancrea" in classification_text.lower():
            match_reason = "generic_pancreatic_oncology"

        secondary_nct = _extract_secondary_nct(details)
        pubmed_links = _extract_pubmed_links(details)
        if not pubmed_links:
            pubmed_links = _extract_pubmed_links_from_references(details)
        if not pubmed_links and not secondary_nct:
            # Pure CTIS trial without NCT bridge: fallback to title-based PubMed search.
            pubmed_links = _fetch_pubmed_links_by_title(title or _clean(overview.get("ctTitle")), max_links=3)
        has_results = _normalize_results_flag(overview, details)
        if pubmed_links:
            has_results = "yes"

        admission_date = normalize_ctis_date(
            _clean(overview.get("decisionDateOverall"))
            or _clean(_nested(details, ["decisionDate"], ""))
        )
        last_update_date = normalize_ctis_date(
            _clean(overview.get("lastUpdated"))
            or _clean(overview.get("lastPublicationUpdate"))
            or _clean(_nested(details, ["publishDate"], ""))
        )
        primary_completion_date = ""
        results_last_update = normalize_ctis_date(_clean(overview.get("lastPublicationUpdate")))

        normalized.append(
            {
                "nct_id": ct_number,
                "source": "ctis",
                "secondary_id": secondary_nct,
                "trial_link": CTIS_TRIAL_URL.format(ct_number=ct_number),
                "title": title or _clean(overview.get("ctTitle")),
                "study_type": study_type,
                "phase": normalize_ctis_phase(phase_raw),
                "status": _clean(_nested(details, ["ctStatus"], "") or overview.get("ctStatus")).upper().replace(" ", "_"),
                "sponsor": _extract_sponsor(overview, details),
                "pdac_match_reason": match_reason,
                "study_design": classification["study_design"],
                "therapeutic_class": classification["therapeutic_class"],
                "focus_tags": ",".join(classification["focus"]) if classification["focus"] else "",
                "admission_date": admission_date,
                "last_update_date": last_update_date,
                "primary_completion_date": primary_completion_date,
                "has_results": has_results,
                "results_last_update": results_last_update,
                "pubmed_links": pubmed_links,
                "conditions": conditions,
                "interventions": interventions,
                "intervention_types": intervention_types,
                "primary_outcomes": primary_outcomes,
                "secondary_outcomes": secondary_outcomes,
                "inclusion_criteria": inclusion_criteria,
                "exclusion_criteria": exclusion_criteria,
                "locations": locations,
                "brief_summary": brief_summary,
                "detailed_description": detailed_description,
            }
        )

        if max_trials is not None and len(normalized) >= max_trials:
            break

    return normalized
