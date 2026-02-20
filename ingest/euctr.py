"""
Legacy EU Clinical Trials Register (EUCTR/EudraCT) client.

Uses the public EUCTR search download endpoint:
- GET /ctr-search/rest/download/summary?query=...&mode=current_page&page=N

The summary export is parsed and normalized to the same schema
used by other ingestion sources.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import re
import time
from typing import Dict, Iterable, List, Optional

import requests

from ingest.clinicaltrials import classify_study, is_pdac_core, pdac_match_reason


EUCTR_SUMMARY_URL = "https://www.clinicaltrialsregister.eu/ctr-search/rest/download/summary"

DEFAULT_EUCTR_QUERY_TERMS = [
    "pancreatic",
    "pancreas",
    "pancreatic cancer",
    "pdac",
    "pancreatic adenocarcinoma",
    "ductal adenocarcinoma",
]

REQUEST_TIMEOUT = 45
RETRYABLE_STATUS_CODES = {403, 429, 500, 502, 503, 504}

STATUS_RE = re.compile(r"\(([^)]+)\)")


@dataclass
class EuctrSummaryRow:
    eudract_number: str = ""
    sponsor_name: str = ""
    sponsor_protocol_number: str = ""
    full_title: str = ""
    start_date: str = ""
    medical_conditions: List[str] = field(default_factory=list)
    diseases: List[str] = field(default_factory=list)
    population_age: str = ""
    gender: str = ""
    trial_protocols: List[str] = field(default_factory=list)
    link: str = ""


def _clean(value: Optional[str]) -> str:
    if value is None:
        return ""
    return str(value).strip()


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


def _request_summary(query: str, page: int, retries: int = 4) -> str:
    last_error: Optional[Exception] = None
    params = {"query": query, "mode": "current_page", "page": page}

    for attempt in range(retries):
        try:
            resp = requests.get(EUCTR_SUMMARY_URL, params=params, timeout=REQUEST_TIMEOUT)
            if resp.status_code in RETRYABLE_STATUS_CODES and attempt < retries - 1:
                time.sleep(1.2 * (attempt + 1))
                continue
            resp.raise_for_status()
            return resp.text
        except Exception as exc:
            last_error = exc
            if attempt < retries - 1:
                time.sleep(1.2 * (attempt + 1))
                continue
            raise

    if last_error:
        raise last_error
    return ""


def parse_summary_text(text: str) -> List[EuctrSummaryRow]:
    rows: List[EuctrSummaryRow] = []
    current: Optional[EuctrSummaryRow] = None

    for raw in text.splitlines():
        line = raw.strip()
        if not line:
            continue

        if line.startswith("EudraCT Number:"):
            if current and current.eudract_number:
                rows.append(current)
            current = EuctrSummaryRow(
                eudract_number=_clean(line.split(":", 1)[1]),
            )
            continue

        if not current:
            continue

        if ":" not in line:
            continue

        key, value = line.split(":", 1)
        key = _clean(key).lower()
        value = _clean(value)
        if key == "sponsor name":
            current.sponsor_name = value
        elif key == "sponsor protocol number":
            current.sponsor_protocol_number = value
        elif key == "full title":
            current.full_title = value
        elif key == "start date":
            current.start_date = value
        elif key == "medical condition":
            if value:
                current.medical_conditions.append(value)
        elif key == "disease":
            if value:
                current.diseases.append(value)
        elif key == "population age":
            current.population_age = value
        elif key == "gender":
            current.gender = value
        elif key == "trial protocol":
            if value:
                current.trial_protocols.append(value)
        elif key == "link":
            current.link = value

    if current and current.eudract_number:
        rows.append(current)

    return rows


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


def _normalize_status(protocols: List[str]) -> str:
    if not protocols:
        return "NA"
    statuses = []
    for protocol in protocols:
        match = STATUS_RE.search(protocol)
        if match:
            status = _clean(match.group(1)).upper().replace(" ", "_")
            if status:
                statuses.append(status)
    statuses = _uniq(statuses)
    if not statuses:
        return "NA"
    if len(statuses) == 1:
        return statuses[0]
    return "/".join(sorted(statuses))


def _build_classification_text(row: EuctrSummaryRow) -> str:
    parts = [
        row.full_title,
        " ".join(row.medical_conditions),
        " ".join(row.diseases),
        row.sponsor_name,
    ]
    return " ".join([p for p in parts if p])


def iter_euctr_summaries(
    query: str,
    *,
    max_trials: Optional[int] = None,
    max_pages: Optional[int] = None,
    sleep_seconds: float = 0.2,
) -> Iterable[EuctrSummaryRow]:
    page = 1
    yielded = 0
    while True:
        if max_pages is not None and page > max_pages:
            break
        text = _request_summary(query, page)
        rows = parse_summary_text(text)
        if not rows:
            break
        for row in rows:
            yield row
            yielded += 1
            if max_trials is not None and yielded >= max_trials:
                return
        page += 1
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)


def fetch_trials_euctr_pdac(
    *,
    max_trials: Optional[int] = None,
    max_pages: Optional[int] = None,
    query_terms: Optional[List[str]] = None,
    sleep_seconds: float = 0.2,
) -> List[Dict[str, str]]:
    terms = _uniq(query_terms or DEFAULT_EUCTR_QUERY_TERMS)
    merged: Dict[str, EuctrSummaryRow] = {}

    for term in terms:
        for row in iter_euctr_summaries(
            term,
            max_trials=max_trials,
            max_pages=max_pages,
            sleep_seconds=sleep_seconds,
        ):
            key = _clean(row.eudract_number)
            if not key:
                continue
            existing = merged.get(key)
            if not existing:
                merged[key] = row
                continue
            existing.medical_conditions = _uniq(existing.medical_conditions + row.medical_conditions)
            existing.diseases = _uniq(existing.diseases + row.diseases)
            existing.trial_protocols = _uniq(existing.trial_protocols + row.trial_protocols)
            if not existing.link:
                existing.link = row.link

    normalized: List[Dict[str, str]] = []
    for row in merged.values():
        classification_text = _build_classification_text(row)
        if not _is_pdac_candidate(classification_text):
            continue

        classification = classify_study("UNKNOWN", classification_text)
        match_reason = pdac_match_reason(classification_text)
        status = _normalize_status(row.trial_protocols)
        trial_link = row.link or f"https://www.clinicaltrialsregister.eu/ctr-search/search?query=eudract_number:{row.eudract_number}"

        brief_summary_parts = []
        if row.population_age:
            brief_summary_parts.append(f"Population Age: {row.population_age}")
        if row.gender:
            brief_summary_parts.append(f"Gender: {row.gender}")
        if row.trial_protocols:
            brief_summary_parts.append(f"Trial Protocols: {' | '.join(row.trial_protocols)}")
        brief_summary = " | ".join(brief_summary_parts)

        normalized.append(
            {
                "nct_id": row.eudract_number,
                "source": "euctr",
                "secondary_id": "",
                "trial_link": trial_link,
                "title": row.full_title,
                "study_type": "UNKNOWN",
                "phase": "NA",
                "status": status,
                "sponsor": row.sponsor_name or "Unknown",
                "pdac_match_reason": match_reason,
                "study_design": classification["study_design"],
                "therapeutic_class": classification["therapeutic_class"],
                "focus_tags": ",".join(classification["focus"]) if classification["focus"] else "",
                "admission_date": row.start_date,
                "last_update_date": "",
                "primary_completion_date": "",
                "has_results": "no",
                "results_last_update": "",
                "pubmed_links": "",
                "conditions": " | ".join(_uniq(row.medical_conditions + row.diseases)),
                "interventions": "",
                "intervention_types": "",
                "primary_outcomes": "",
                "secondary_outcomes": "",
                "inclusion_criteria": "",
                "exclusion_criteria": "",
                "locations": "",
                "brief_summary": brief_summary,
                "detailed_description": "",
            }
        )

    return normalized
