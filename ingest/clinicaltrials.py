"""
ClinicalTrials.gov API client.

Fetches pancreas-related trials using a broad query ("pancreas")
and applies a strict PDAC-focused classification layer to reduce noise.

Keeps:
- Pancreatic ductal adenocarcinoma (PDAC)
- Pancreas adenocarcinoma
- Pancreatic cancer (pancreas-specific)

Excludes:
- Unknown primary
- Generic solid tumors
- Non-oncological pancreas studies

OBSERVATIONAL studies are kept but explicitly tagged.
"""

import re
import requests
from typing import List, Dict, Optional, Tuple


BASE_URL = "https://clinicaltrials.gov/api/v2/studies"


def _pick_date(module: Dict, keys: List[str]) -> str:
    """
    Pick the first available date-like value from ClinicalTrials.gov modules.
    Supports either raw strings or {..., "date": "..."} structures.
    """
    for key in keys:
        value = module.get(key)
        if not value:
            continue
        if isinstance(value, dict):
            date_value = value.get("date")
            if date_value:
                return str(date_value)
        elif isinstance(value, str):
            return value
    return ""


def _extract_result_flags(protocol: Dict, derived: Dict, status_mod: Dict) -> Dict[str, str]:
    """
    Extract result-related signals if present in API payload.
    """
    has_results = ""

    # Most commonly exposed in derived/misc info modules.
    for container in (
        derived.get("miscInfoModule", {}),
        protocol.get("statusModule", {}),
        protocol.get("derivedSection", {}).get("miscInfoModule", {}),
    ):
        if isinstance(container, dict) and "hasResults" in container:
            raw = container.get("hasResults")
            if raw is True:
                has_results = "yes"
            elif raw is False:
                has_results = "no"
            elif raw is not None:
                has_results = str(raw).lower()
            break

    results_last_update = _pick_date(
        status_mod,
        [
            "resultsFirstPostDateStruct",
            "resultsFirstSubmitDate",
            "resultsFirstPostDate",
            "resultsFirstSubmitQcDate",
        ],
    )

    return {
        "has_results": has_results,
        "results_last_update": results_last_update,
    }


def _join_non_empty(values: List[str], sep: str = " | ") -> str:
    return sep.join([str(v).strip() for v in values if str(v).strip()])


def _extract_interventions(arms_mod: Dict) -> Tuple[str, str]:
    items = []
    intervention_types = []
    for intervention in arms_mod.get("interventions", []) or []:
        name = (intervention.get("name") or "").strip()
        kind = (intervention.get("type") or "").strip()
        if kind and kind not in intervention_types:
            intervention_types.append(kind)
        if name and kind:
            items.append(f"{kind}: {name}")
        elif name:
            items.append(name)
    return _join_non_empty(items), _join_non_empty(intervention_types, sep=", ")


def _extract_outcomes(outcomes_mod: Dict, key: str) -> str:
    values = []
    for item in outcomes_mod.get(key, []) or []:
        measure = (item.get("measure") or "").strip()
        time_frame = (item.get("timeFrame") or "").strip()
        description = (item.get("description") or "").strip()
        chunks = [measure]
        if time_frame:
            chunks.append(f"timeframe={time_frame}")
        if description:
            chunks.append(description)
        values.append(" ; ".join([c for c in chunks if c]))
    return _join_non_empty(values)


def _extract_eligibility(eligibility_mod: Dict) -> Tuple[str, str]:
    inclusion = (eligibility_mod.get("inclusionCriteria") or "").strip()
    exclusion = (eligibility_mod.get("exclusionCriteria") or "").strip()
    criteria = (eligibility_mod.get("eligibilityCriteria") or "").strip()

    # Some records only provide free-text criteria; split by heading markers.
    if criteria:
        if not inclusion:
            m = re.search(r"inclusion criteria\s*:?(.*?)(?:exclusion criteria\s*:|$)", criteria, re.I | re.S)
            if m:
                inclusion = m.group(1).strip()
        if not exclusion:
            m = re.search(r"exclusion criteria\s*:?(.*)$", criteria, re.I | re.S)
            if m:
                exclusion = m.group(1).strip()

    return inclusion, exclusion


def _extract_locations(contacts_mod: Dict) -> str:
    entries = []
    for loc in contacts_mod.get("locations", []) or []:
        facility = ""
        if isinstance(loc.get("facility"), dict):
            facility = (loc.get("facility", {}).get("name") or "").strip()
        else:
            facility = (loc.get("facility") or loc.get("facilityName") or "").strip()

        city = (loc.get("city") or "").strip()
        country = (loc.get("country") or "").strip()
        place = ", ".join([x for x in [city, country] if x])
        if facility and place:
            entries.append(f"{facility} ({place})")
        elif facility:
            entries.append(facility)
        elif place:
            entries.append(place)

    return _join_non_empty(entries)


# -------------------------------------------------------------------
# PDAC CORE FILTER
# -------------------------------------------------------------------

def is_pdac_core(title: str) -> bool:
    if not title:
        return False

    t = title.lower()

    pdac_terms = [
        "pancreatic ductal adenocarcinoma",
        "ductal adenocarcinoma of the pancreas",
        "pancreas adenocarcinoma",
        "pancreatic adenocarcinoma",
        "pdac",
        "pancreatic cancer",
    ]

    negative_terms = [
        "unknown primary",
        "solid tumor",
        "solid tumours",
        "multiple cancers",
        "various cancers",
        "different cancers",
        "non-pancreatic",
    ]

    if any(term in t for term in negative_terms):
        return False

    return any(term in t for term in pdac_terms)


# -------------------------------------------------------------------
# MATCH REASON (TRANSPARENCY)
# -------------------------------------------------------------------

def pdac_match_reason(title: str) -> str:
    t = title.lower()

    if "pancreatic ductal adenocarcinoma" in t:
        return "explicit_pdac"
    if "pdac" in t:
        return "pdac_acronym"
    if "pancreas adenocarcinoma" in t or "pancreatic adenocarcinoma" in t:
        return "adenocarcinoma_pancreas"
    if "pancreatic cancer" in t:
        return "generic_pancreatic_cancer"

    return "unknown_match"


# -------------------------------------------------------------------
# STUDY CLASSIFICATION
# -------------------------------------------------------------------

def classify_study(study_type: str, text: str) -> Dict[str, str]:
    t = (text or "").lower()

    classification = {
        "study_design": "unknown",
        "therapeutic_class": "unknown",
        "focus": [],
    }

    # Study design
    if study_type == "INTERVENTIONAL":
        classification["study_design"] = "interventional"
    elif study_type == "OBSERVATIONAL":
        classification["study_design"] = "observational"
    elif study_type == "EXPANDED_ACCESS":
        classification["study_design"] = "expanded_access"

    # Therapeutic class (count keyword hits to avoid accidental overwrite)
    therapeutic_signals = {
        "chemotherapy": [
            "chemotherapy", "gemcitabine", "folfirinox", "irinotecan", "oxaliplatin",
            "cisplatin", "nab-paclitaxel", "paclitaxel", "5-fu", "fluorouracil",
            "asparaginase", "folfox", "floxuridine", "docetaxel", "bleomycin",
        ],
        "immunotherapy": [
            "immunotherapy", "pd-1", "pd-l1", "ctla-4", "vaccine",
            "car-t", "t cell", "nivolumab", "pembrolizumab", "atezolizumab",
            "antibody", "nk cell", "nk cells", "natural killer", "oncolytic",
            "viral therapy", "stem cell", "mesenchymal stem cell",
        ],
        "targeted_therapy": [
            "kras", "egfr", "parp", "targeted", "inhibitor", "inhibition",
            "olaparib", "trametinib", "erlotinib", "selumetinib",
            "vorinostat", "binimetinib", "belzutifan", "braf", "ccx872",
            "warfarin", "metformin",
        ],
        "radiotherapy": ["radiation", "radiotherapy", "sbrt", "imrt"],
        "surgical": [
            "surgery", "resection", "pancreatectomy", "whipple",
            "mesenteric approach", "conventional approach",
        ],
        "locoregional_therapy": [
            "electroporation", "interstitial laser", "thermotherapy",
            "hepasphere", "percutaneous holmium", "digital subtraction angiography",
            "dsa", "hepatic artery infusional",
        ],
        "registry_program": [
            "registry", "database", "data management center", "survey", "case-vignette",
            "master protocol",
        ],
        "translational_research": [
            "organoid", "exosome", "serum-bank", "bioprinting", "microbiota",
            "perineural invasion", "microparticles",
        ],
        "supportive_care": [
            "pain", "acupuncture", "acupressure", "nutrition", "diet",
            "exercise", "fatigue", "psychosocial", "quality of life",
            "palliative", "supportive care", "prehabilitation", "training",
            "walking", "depression", "cachexia", "anorexia", "appetite",
            "sarcopenia", "pregabalin", "escitalopram", "engagement app",
            "app-based", "with app", "anxiety", "prophylaxis", "vte", "dalteparin",
            "ketamine", "diabetes",
        ],
    }

    class_scores = {
        cls: sum(1 for term in terms if term in t)
        for cls, terms in therapeutic_signals.items()
    }
    max_score = max(class_scores.values())
    if max_score > 0:
        tie_break_priority = [
            "locoregional_therapy",
            "surgical",
            "radiotherapy",
            "immunotherapy",
            "targeted_therapy",
            "chemotherapy",
            "supportive_care",
            "translational_research",
            "registry_program",
        ]
        candidates = {cls for cls, score in class_scores.items() if score == max_score}
        for cls in tie_break_priority:
            if cls in candidates:
                classification["therapeutic_class"] = cls
                break

    # Focus tags
    focus_rules = {
        "biomarker": [
            "biomarker", "ctdna", "circulating tumor", "genomic", "mutation",
            "diagnosis", "diagnostic", "ca19-9", "methylation", "microrna",
            "micro-rna", "mirna", "liquid biopsy", "portal vein sampling",
        ],
        "early_detection": [
            "screening", "early detection", "surveillance", "high-risk",
            "new onset diabetes", "risk model", "predictor",
        ],
        "imaging_diagnostics": [
            "imaging", "pet", "mri", "ct ", "ct/", "ultrasound", "eus", "radiomic",
        ],
        "liquid_biopsy": [
            "ctdna", "liquid biopsy", "circulating tumor", "blood biomarker",
            "portal vein sampling", "exosome",
        ],
        "genomics_precision": [
            "genomic", "mutation", "germline", "brca", "kras", "braf", "parp",
            "precision", "machine learning", "artificial intelligence",
        ],
        "hereditary_risk": [
            "family history", "hereditary", "germline", "high-risk individuals",
            "brca",
        ],
        "supportive_outcomes": [
            "quality of life", "pain", "fatigue", "anxiety", "depression",
            "appetite", "cachexia", "survival", "recurrence", "prognosis",
        ],
        "locoregional_procedure": [
            "electroporation", "thermotherapy", "ablation",
            "hepatic artery infusional", "seed implantation", "angiography",
        ],
        "microbiome_metabolic": [
            "microbiota", "metabolite", "diabetes", "steatosis",
        ],
        "registry_real_world": [
            "registry", "database", "survey", "case-vignette", "real-world",
        ],
        "advanced_disease": ["metastatic", "advanced", "unresectable"],
        "resectable_disease": ["resectable", "neoadjuvant", "adjuvant"],
        "line_of_therapy": ["first-line", "second-line", "refractory"],
    }

    for tag, terms in focus_rules.items():
        if any(term in t for term in terms):
            classification["focus"].append(tag)

    # Preserve order and remove duplicates.
    classification["focus"] = list(dict.fromkeys(classification["focus"]))

    # If focus tags exist but no explicit therapy signal, avoid "unknown"
    if classification["therapeutic_class"] == "unknown" and classification["focus"]:
        if "biomarker" in classification["focus"]:
            classification["therapeutic_class"] = "biomarker_diagnostics"
        elif classification["study_design"] == "observational":
            classification["therapeutic_class"] = "observational_non_therapeutic"
        else:
            classification["therapeutic_class"] = "context_classified"
    elif classification["therapeutic_class"] == "unknown":
        # Observational studies are often non-therapeutic registries/prognostic work.
        if classification["study_design"] == "observational":
            classification["therapeutic_class"] = "observational_non_therapeutic"
        else:
            classification["therapeutic_class"] = "context_classified"

    return classification


def build_classification_text(protocol: Dict) -> str:
    """
    Build richer free-text context for classification so we don't rely
    only on briefTitle.
    """
    id_mod = protocol.get("identificationModule", {})
    cond_mod = protocol.get("conditionsModule", {})
    arms_mod = protocol.get("armsInterventionsModule", {})

    parts = [
        id_mod.get("briefTitle", ""),
        id_mod.get("officialTitle", ""),
        id_mod.get("acronym", ""),
        " ".join(cond_mod.get("conditions", []) or []),
        " ".join(cond_mod.get("keywords", []) or []),
    ]

    for i in arms_mod.get("interventions", []) or []:
        parts.append(i.get("name", ""))
        parts.append(i.get("description", ""))

    for a in arms_mod.get("armGroups", []) or []:
        parts.append(a.get("label", ""))
        parts.append(a.get("description", ""))

    return " ".join(p for p in parts if p)


# -------------------------------------------------------------------
# MAIN FETCH FUNCTION
# -------------------------------------------------------------------

def fetch_trials_pancreas(max_records: Optional[int] = None) -> List[Dict]:
    params = {
        "query.term": "pancreas",
        "pageSize": 100,
        "format": "json",
    }

    all_studies = []
    next_page_token = None

    while True:
        if next_page_token:
            params["pageToken"] = next_page_token

        resp = requests.get(BASE_URL, params=params, timeout=30)
        resp.raise_for_status()

        data = resp.json()
        studies = data.get("studies", [])

        for s in studies:
            protocol = s.get("protocolSection", {})
            derived = s.get("derivedSection", {})

            id_mod = protocol.get("identificationModule", {})
            status_mod = protocol.get("statusModule", {})
            design_mod = protocol.get("designModule", {})
            sponsor_mod = protocol.get("sponsorCollaboratorsModule", {})
            cond_mod = protocol.get("conditionsModule", {})
            arms_mod = protocol.get("armsInterventionsModule", {})
            outcomes_mod = protocol.get("outcomesModule", {})
            eligibility_mod = protocol.get("eligibilityModule", {})
            contacts_mod = protocol.get("contactsLocationsModule", {})
            desc_mod = protocol.get("descriptionModule", {})

            nct_id = id_mod.get("nctId")
            title = id_mod.get("briefTitle", "")

            if not nct_id:
                continue

            # PDAC FILTER
            if not is_pdac_core(title):
                continue

            study_type = design_mod.get("studyType", "UNKNOWN")
            classification_text = build_classification_text(protocol)
            classification = classify_study(study_type, classification_text)
            admission_date = _pick_date(
                status_mod,
                [
                    "studyFirstSubmitDate",
                    "studyFirstPostDateStruct",
                    "studyFirstSubmitQcDate",
                    "startDateStruct",
                ],
            )
            last_update_date = _pick_date(
                status_mod,
                [
                    "lastUpdatePostDateStruct",
                    "lastUpdateSubmitDate",
                    "lastUpdateSubmitQcDate",
                    "completionDateStruct",
                ],
            )
            result_flags = _extract_result_flags(protocol, derived, status_mod)
            has_results = result_flags["has_results"]
            if not has_results:
                has_results = "yes" if result_flags["results_last_update"] else "no"
            inclusion_criteria, exclusion_criteria = _extract_eligibility(eligibility_mod)
            interventions, intervention_types = _extract_interventions(arms_mod)

            all_studies.append(
                {
                    "nct_id": nct_id,
                    "title": title,
                    "study_type": study_type,
                    "phase": (
                        design_mod.get("phases", ["NA"])[0]
                        if design_mod.get("phases")
                        else "NA"
                    ),
                    "status": status_mod.get("overallStatus", "Unknown"),
                    "sponsor": sponsor_mod.get("leadSponsor", {}).get("name", "Unknown"),
                    "pdac_match_reason": pdac_match_reason(title),
                    "study_design": classification["study_design"],
                    "therapeutic_class": classification["therapeutic_class"],
                    "focus_tags": ",".join(classification["focus"]) if classification["focus"] else "",
                    "admission_date": admission_date,
                    "last_update_date": last_update_date,
                    "has_results": has_results,
                    "results_last_update": result_flags["results_last_update"],
                    "conditions": _join_non_empty(cond_mod.get("conditions", []) or []),
                    "interventions": interventions,
                    "intervention_types": intervention_types,
                    "primary_outcomes": _extract_outcomes(outcomes_mod, "primaryOutcomes"),
                    "secondary_outcomes": _extract_outcomes(outcomes_mod, "secondaryOutcomes"),
                    "inclusion_criteria": inclusion_criteria,
                    "exclusion_criteria": exclusion_criteria,
                    "locations": _extract_locations(contacts_mod),
                    "brief_summary": (desc_mod.get("briefSummary") or "").strip(),
                    "detailed_description": (desc_mod.get("detailedDescription") or "").strip(),
                }
            )

            if max_records is not None and len(all_studies) >= max_records:
                return all_studies

        next_page_token = data.get("nextPageToken")
        if not next_page_token:
            break

    return all_studies
