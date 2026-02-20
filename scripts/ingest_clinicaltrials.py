"""
Ingest PDAC-related clinical trials from ClinicalTrials.gov,
and CTIS (EU Clinical Trials Information System), then store them
in the local database.

This script does NOT reclassify trials.
All semantic classification is done upstream (ingest modules).
"""

import os
import re
import sqlite3
from datetime import datetime, date
from difflib import SequenceMatcher
import xml.etree.ElementTree as ET

import numpy as np
import pandas as pd
import requests
from typing import Optional
from ingest.clinicaltrials import fetch_trials_pancreas, _fetch_pubmed_links_by_nct
from ingest.ctis import fetch_trials_ctis_pdac
from db.session import SessionLocal, init_db
from db.models import ClinicalTrial, ClinicalTrialDetails, ClinicalTrialPublication
from sqlalchemy import text

PUBMED_SUMMARY_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"
PUBMED_FETCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
PUBMED_ESEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"

PUBLICATION_METHOD_CONFIDENCE = {
    "pubmed_link": 98,
    "doi_reference": 95,
    "nct_exact": 92,
    "secondary_nct_exact": 90,
    "title_fuzzy": 72,
}


def as_na(value):
    """
    Normalize missing/blank values to 'NA' for consistent downstream UX.
    """
    if value is None:
        return "NA"
    if isinstance(value, str) and not value.strip():
        return "NA"
    return value


def _clean(value) -> str:
    if value is None:
        return ""
    return str(value).strip()


def is_na(value) -> bool:
    return value is None or (isinstance(value, str) and value.strip().lower() in {"", "na"})


def _split_values(value: str, sep: str) -> list[str]:
    if is_na(value):
        return []
    return [x.strip() for x in str(value).split(sep) if x and x.strip() and x.strip().lower() != "na"]


def _merge_values(a: str, b: str, sep: str = " | ") -> str:
    out = []
    seen = set()
    for raw in _split_values(a, sep) + _split_values(b, sep):
        key = raw.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(raw)
    return sep.join(out)


def _join_non_empty(values, sep: str = " | ") -> str:
    return sep.join([_clean(v) for v in values if _clean(v)])


def _parse_date_key(value: str) -> str:
    if is_na(value):
        return ""
    value = str(value).strip()
    return value if re.match(r"^\d{4}(-\d{2}){0,2}$", value) else ""


def _parse_date(value: str) -> Optional[date]:
    if is_na(value):
        return None
    raw = str(value).strip()
    for fmt in ("%Y-%m-%d", "%Y-%m", "%Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except Exception:
            continue
    return None


def _parse_pubmed_date(value: str) -> Optional[date]:
    if not value:
        return None
    raw = str(value).strip()
    for fmt in ("%Y %b %d", "%Y %b", "%Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except Exception:
            continue
    return _parse_date(raw)


def _extract_pmids(pubmed_links: str) -> list[str]:
    if is_na(pubmed_links):
        return []
    pmids = []
    for part in str(pubmed_links).split("|"):
        match = re.search(r"(\d{5,10})", part)
        if match:
            pmids.append(match.group(1))
    return list(dict.fromkeys(pmids))


def _normalize_doi(raw: str) -> str:
    text = _clean(raw)
    if not text:
        return ""
    text = text.strip()
    text = re.sub(r"^https?://(dx\.)?doi\.org/", "", text, flags=re.I)
    text = re.sub(r"^doi:\s*", "", text, flags=re.I)
    return text.strip()


def _extract_dois(value: str) -> list[str]:
    if is_na(value):
        return []
    dois = []
    for token in str(value).split("|"):
        token_clean = token.strip()
        if not token_clean:
            continue
        if "doi.org/" in token_clean.lower() or token_clean.lower().startswith("doi:"):
            doi = _normalize_doi(token_clean)
            if doi:
                dois.append(doi)
            continue
        for doi in re.findall(r"(10\.\d{4,9}/[-._;()/:A-Z0-9]+)", token_clean, flags=re.I):
            norm = _normalize_doi(doi)
            if norm:
                dois.append(norm)
    return list(dict.fromkeys(dois))


def _parse_nct_tokens(value: str) -> list[str]:
    if is_na(value):
        return []
    tokens = []
    for token in re.findall(r"(NCT\d+)", str(value), flags=re.I):
        tokens.append(token.upper())
    return list(dict.fromkeys(tokens))


def _extract_pubmed_pmids(payload: dict) -> list[str]:
    if not isinstance(payload, dict):
        return []
    rows = payload.get("esearchresult", {}) or {}
    idlist = rows.get("idlist", []) or []
    return [str(item).strip() for item in idlist if str(item).strip().isdigit()]


def _search_pubmed_pmids(term: str, max_links: int = 5) -> list[str]:
    query = _clean(term)
    if not query:
        return []
    try:
        resp = requests.get(
            PUBMED_ESEARCH_URL,
            params={
                "db": "pubmed",
                "retmode": "json",
                "retmax": max_links,
                "term": query,
            },
            timeout=25,
        )
        resp.raise_for_status()
        return _extract_pubmed_pmids(resp.json())
    except Exception:
        return []


def _extract_summary_doi(summary_row: dict) -> str:
    article_ids = summary_row.get("articleids", []) or []
    for item in article_ids:
        if not isinstance(item, dict):
            continue
        if _clean(item.get("idtype")).lower() == "doi":
            doi = _normalize_doi(_clean(item.get("value")))
            if doi:
                return doi

    elocation = _clean(summary_row.get("elocationid"))
    if elocation:
        doi = _normalize_doi(elocation)
        if doi:
            return doi
    return ""


def _fetch_pubmed_summary(pmids: list[str]) -> dict[str, dict[str, str]]:
    if not pmids:
        return {}
    try:
        resp = requests.get(
            PUBMED_SUMMARY_URL,
            params={
                "db": "pubmed",
                "retmode": "json",
                "id": ",".join(pmids),
            },
            timeout=20,
        )
        resp.raise_for_status()
        payload = resp.json().get("result", {}) or {}
        out = {}
        for pmid in pmids:
            row = payload.get(pmid) or {}
            out[pmid] = {
                "publication_date_raw": _clean(row.get("pubdate") or row.get("epubdate")),
                "publication_title": _clean(row.get("title")),
                "journal": _clean(row.get("fulljournalname") or row.get("source")),
                "doi": _extract_summary_doi(row),
            }
        return out
    except Exception:
        return {}


def _fetch_pubmed_mesh_terms(pmids: list[str]) -> list[str]:
    if not pmids:
        return []
    try:
        resp = requests.get(
            PUBMED_FETCH_URL,
            params={
                "db": "pubmed",
                "retmode": "xml",
                "id": ",".join(pmids),
            },
            timeout=25,
        )
        resp.raise_for_status()
        root = ET.fromstring(resp.text)
    except Exception:
        return []

    terms = []
    for descriptor in root.findall(".//MeshHeading/DescriptorName"):
        if descriptor.text:
            terms.append(descriptor.text.strip())
    return list(dict.fromkeys([t for t in terms if t]))


def _score_therapeutic_class(
    existing: str,
    focus_tags: str,
    mesh_terms: list[str],
) -> str:
    existing_norm = (existing or "").strip().lower()
    scores = {}

    if existing_norm and existing_norm not in {"unknown", "context_classified", "na"}:
        scores[existing_norm] = scores.get(existing_norm, 0) + 2

    tag_text = " ".join([t.strip() for t in (focus_tags or "").split(",") if t.strip()]).lower()
    mesh_text = " ".join(mesh_terms or []).lower()
    combined = f"{tag_text} {mesh_text}".strip()

    tag_to_class = {
        "biomarker": "biomarker_diagnostics",
        "early_detection": "biomarker_diagnostics",
        "imaging_diagnostics": "biomarker_diagnostics",
        "liquid_biopsy": "biomarker_diagnostics",
        "genomics_precision": "targeted_therapy",
        "supportive_outcomes": "supportive_care",
        "locoregional_procedure": "locoregional_therapy",
        "registry_real_world": "registry_program",
    }
    for tag in (focus_tags or "").split(","):
        key = tag.strip().lower()
        if key in tag_to_class:
            cls = tag_to_class[key]
            scores[cls] = scores.get(cls, 0) + 1

    mesh_signals = {
        "chemotherapy": ["chemotherapy", "antineoplastic"],
        "immunotherapy": ["immunotherapy", "immune checkpoint", "vaccines"],
        "targeted_therapy": ["molecular targeted", "protein kinase", "parp", "egfr", "kras", "braf", "inhibitor"],
        "radiotherapy": ["radiotherapy", "radiation"],
        "surgical": ["surgery", "surgical procedures", "pancreatectomy", "resection"],
        "locoregional_therapy": ["ablation", "electroporation", "embolization", "intra-arterial"],
        "supportive_care": ["palliative care", "quality of life", "pain", "supportive care"],
        "biomarker_diagnostics": ["biomarker", "diagnostic", "screening", "early detection", "imaging"],
    }
    for cls, terms in mesh_signals.items():
        if any(term in combined for term in terms):
            scores[cls] = scores.get(cls, 0) + 2

    if not scores:
        return existing_norm or "context_classified"

    max_score = max(scores.values())
    candidates = {cls for cls, score in scores.items() if score == max_score}
    priority = [
        "locoregional_therapy",
        "surgical",
        "radiotherapy",
        "immunotherapy",
        "targeted_therapy",
        "chemotherapy",
        "supportive_care",
        "biomarker_diagnostics",
        "registry_program",
    ]
    for cls in priority:
        if cls in candidates:
            return cls
    return sorted(candidates)[0]


def backfill_pubmed_publication_dates(session, max_lookups: int = 200) -> int:
    if max_lookups <= 0:
        return 0
    candidates = (
        session.query(ClinicalTrial)
        .filter(ClinicalTrial.pubmed_links.is_not(None))
        .filter(ClinicalTrial.pubmed_links != "")
        .filter(ClinicalTrial.pubmed_links != "NA")
        .filter(
            (ClinicalTrial.publication_date.is_(None))
            | (ClinicalTrial.publication_date == "")
            | (ClinicalTrial.publication_date == "NA")
        )
        .order_by(ClinicalTrial.nct_id.asc())
        .limit(max_lookups)
        .all()
    )

    updated = 0
    for trial in candidates:
        pmids = _extract_pmids(trial.pubmed_links)
        if not pmids:
            continue
        summaries = _fetch_pubmed_summary(pmids)
        dates = []
        for pmid in pmids:
            summary = summaries.get(pmid, {}) or {}
            parsed = _parse_pubmed_date(summary.get("publication_date_raw"))
            if parsed:
                dates.append(parsed)
        if not dates:
            continue
        trial.publication_date = min(dates).isoformat()
        updated += 1

    session.commit()
    return updated


def _assign_method(
    method_by_pmid: dict[str, tuple[str, int]],
    pmid: str,
    method: str,
    confidence: Optional[int] = None,
) -> None:
    conf = (
        int(confidence)
        if confidence is not None
        else int(PUBLICATION_METHOD_CONFIDENCE.get(method, 70))
    )
    current = method_by_pmid.get(pmid)
    if current is None or conf > current[1]:
        method_by_pmid[pmid] = (method, conf)


def _build_title_query(title: str, sponsor: str, admission_date: str) -> str:
    title_text = _clean(title)
    if not title_text:
        return ""
    query = f"({title_text}[Title]) AND (pancreatic OR pancreas OR PDAC)"
    year = _clean(admission_date)[:4] if _clean(admission_date)[:4].isdigit() else ""
    if year:
        query += f" AND ({year}[Date - Publication] : 3000[Date - Publication])"
    sponsor_text = _clean(sponsor)
    if sponsor_text and sponsor_text.lower() not in {"na", "unknown"}:
        # Sponsor affinity is a soft refinement that still keeps broad recall.
        query += f" AND ({sponsor_text}[Affiliation] OR {sponsor_text}[Corporate Author])"
    return query


def ensure_publications_table(session) -> None:
    session.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS trial_publications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                nct_id TEXT NOT NULL,
                pmid TEXT,
                doi TEXT,
                publication_date TEXT,
                publication_title TEXT,
                journal TEXT,
                match_method TEXT,
                confidence INTEGER,
                FOREIGN KEY(nct_id) REFERENCES clinical_trials(nct_id)
            )
            """
        )
    )
    session.execute(
        text(
            "CREATE INDEX IF NOT EXISTS idx_trial_publications_nct_id ON trial_publications(nct_id)"
        )
    )
    session.execute(
        text(
            "CREATE INDEX IF NOT EXISTS idx_trial_publications_pmid ON trial_publications(pmid)"
        )
    )
    session.execute(
        text(
            "CREATE INDEX IF NOT EXISTS idx_trial_publications_doi ON trial_publications(doi)"
        )
    )
    session.commit()


def rebuild_trial_publications(
    session,
    *,
    max_nct_lookups: int = 400,
    max_title_lookups: int = 300,
    max_doi_lookups: int = 200,
    max_links_per_trial: int = 5,
) -> dict[str, int]:
    ensure_publications_table(session)
    session.query(ClinicalTrialPublication).delete()
    session.commit()

    nct_lookup_cache: dict[str, list[str]] = {}
    title_lookup_cache: dict[str, list[str]] = {}
    doi_lookup_cache: dict[str, list[str]] = {}
    summary_cache: dict[str, dict[str, str]] = {}

    nct_lookups_used = 0
    title_lookups_used = 0
    doi_lookups_used = 0
    publication_rows = 0
    trials_with_publications = 0

    trials = session.query(ClinicalTrial).order_by(ClinicalTrial.nct_id.asc()).all()
    for trial in trials:
        method_by_pmid: dict[str, tuple[str, int]] = {}
        raw_dois = set(_extract_dois(trial.pubmed_links))

        # 1) Existing row links are highest-confidence seeds.
        for pmid in _extract_pmids(trial.pubmed_links):
            _assign_method(method_by_pmid, pmid, "pubmed_link")

        # 2) Exact NCT lookups (primary + secondary) to expand publication coverage.
        nct_tokens = []
        if _clean(trial.nct_id).upper().startswith("NCT"):
            nct_tokens.append(_clean(trial.nct_id).upper())
        nct_tokens.extend(_parse_nct_tokens(trial.secondary_id))
        nct_tokens = list(dict.fromkeys(nct_tokens))
        for idx, nct_token in enumerate(nct_tokens):
            if len(method_by_pmid) >= max_links_per_trial:
                break
            cached = nct_lookup_cache.get(nct_token)
            if cached is None:
                if nct_lookups_used >= max_nct_lookups:
                    break
                cached = _search_pubmed_pmids(f"{nct_token}[si]", max_links=max_links_per_trial)
                nct_lookup_cache[nct_token] = cached
                nct_lookups_used += 1
            for pmid in cached:
                method = "nct_exact" if idx == 0 else "secondary_nct_exact"
                _assign_method(method_by_pmid, pmid, method)

        # 3) DOI resolution when available.
        for doi in list(raw_dois):
            if len(method_by_pmid) >= max_links_per_trial:
                break
            cached = doi_lookup_cache.get(doi)
            if cached is None:
                if doi_lookups_used >= max_doi_lookups:
                    continue
                cached = _search_pubmed_pmids(f"{doi}[AID]", max_links=max_links_per_trial)
                doi_lookup_cache[doi] = cached
                doi_lookups_used += 1
            for pmid in cached:
                _assign_method(method_by_pmid, pmid, "doi_reference")

        # 4) Title fallback for sparse rows.
        if not method_by_pmid and max_title_lookups > 0:
            title_query = _build_title_query(trial.title, trial.sponsor, trial.admission_date)
            if title_query:
                cached = title_lookup_cache.get(title_query)
                if cached is None and title_lookups_used < max_title_lookups:
                    cached = _search_pubmed_pmids(title_query, max_links=max_links_per_trial * 2)
                    title_lookup_cache[title_query] = cached
                    title_lookups_used += 1
                elif cached is None:
                    cached = []

                if cached:
                    summary_missing = [pmid for pmid in cached if pmid not in summary_cache]
                    if summary_missing:
                        summary_cache.update(_fetch_pubmed_summary(summary_missing))

                    trial_title = _clean(trial.title).lower()
                    for pmid in cached:
                        summary = summary_cache.get(pmid, {}) or {}
                        candidate_title = _clean(summary.get("publication_title")).lower()
                        if not trial_title or not candidate_title:
                            similarity = 0.0
                        else:
                            similarity = SequenceMatcher(None, trial_title, candidate_title).ratio()
                        if similarity < 0.38:
                            continue
                        confidence = int(60 + similarity * 30)
                        _assign_method(
                            method_by_pmid,
                            pmid,
                            "title_fuzzy",
                            confidence=confidence,
                        )

        # Metadata fetch for publication rows.
        pmids = list(method_by_pmid.keys())[:max_links_per_trial]
        summary_missing = [pmid for pmid in pmids if pmid not in summary_cache]
        if summary_missing:
            summary_cache.update(_fetch_pubmed_summary(summary_missing))

        inserted_keys = set()
        for pmid in pmids:
            summary = summary_cache.get(pmid, {}) or {}
            method, confidence = method_by_pmid.get(pmid, ("title_fuzzy", 70))
            publication_date = ""
            parsed = _parse_pubmed_date(summary.get("publication_date_raw"))
            if parsed:
                publication_date = parsed.isoformat()
            doi = _normalize_doi(summary.get("doi"))
            if doi:
                raw_dois.add(doi)

            key = (pmid, doi)
            if key in inserted_keys:
                continue
            inserted_keys.add(key)

            session.add(
                ClinicalTrialPublication(
                    nct_id=trial.nct_id,
                    pmid=pmid,
                    doi=doi or None,
                    publication_date=publication_date or "NA",
                    publication_title=_clean(summary.get("publication_title")) or "NA",
                    journal=_clean(summary.get("journal")) or "NA",
                    match_method=method,
                    confidence=confidence,
                )
            )
            publication_rows += 1

        # Keep DOI-only records when no PMID mapping exists.
        pmid_set = set(pmids)
        for doi in sorted(raw_dois):
            if not doi:
                continue
            if any(doi == _normalize_doi(summary_cache.get(pmid, {}).get("doi")) for pmid in pmid_set):
                continue
            session.add(
                ClinicalTrialPublication(
                    nct_id=trial.nct_id,
                    pmid=None,
                    doi=doi,
                    publication_date="NA",
                    publication_title="NA",
                    journal="NA",
                    match_method="doi_reference",
                    confidence=PUBLICATION_METHOD_CONFIDENCE["doi_reference"],
                )
            )
            publication_rows += 1

        if pmids or raw_dois:
            trials_with_publications += 1

    session.commit()
    return {
        "publication_rows": publication_rows,
        "trials_with_publications": trials_with_publications,
        "nct_lookups_used": nct_lookups_used,
        "title_lookups_used": title_lookups_used,
        "doi_lookups_used": doi_lookups_used,
    }


def refresh_trial_publication_summary(session) -> int:
    updated = 0
    trials = session.query(ClinicalTrial).order_by(ClinicalTrial.nct_id.asc()).all()
    for trial in trials:
        pubs = (
            session.query(ClinicalTrialPublication)
            .filter(ClinicalTrialPublication.nct_id == trial.nct_id)
            .order_by(ClinicalTrialPublication.confidence.desc(), ClinicalTrialPublication.publication_date.asc())
            .all()
        )
        if not pubs:
            continue

        if (trial.has_results or "").strip().lower() != "yes":
            trial.has_results = "yes"

        pmids = []
        for pub in pubs:
            pmid = _clean(pub.pmid)
            if pmid and pmid.isdigit():
                pmids.append(pmid)
        pubmed_links = _join_non_empty(
            [f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/" for pmid in list(dict.fromkeys(pmids))]
        )
        if pubmed_links:
            trial.pubmed_links = pubmed_links

        dates = []
        for pub in pubs:
            parsed = _parse_date(pub.publication_date)
            if parsed:
                dates.append(parsed)
        if dates:
            trial.publication_date = min(dates).isoformat()
        updated += 1

    session.commit()
    return updated


def improve_therapeutic_class_ensemble(session, max_lookups: int = 200) -> int:
    if max_lookups <= 0:
        return 0

    candidates = (
        session.query(ClinicalTrial)
        .filter(
            (ClinicalTrial.therapeutic_class.is_(None))
            | (ClinicalTrial.therapeutic_class == "")
            | (ClinicalTrial.therapeutic_class == "NA")
            | (ClinicalTrial.therapeutic_class == "unknown")
            | (ClinicalTrial.therapeutic_class == "context_classified")
        )
        .order_by(ClinicalTrial.nct_id.asc())
        .limit(max_lookups)
        .all()
    )

    updated = 0
    for trial in candidates:
        mesh_terms = []
        pmids = [
            _clean(pub.pmid)
            for pub in session.query(ClinicalTrialPublication)
            .filter(ClinicalTrialPublication.nct_id == trial.nct_id)
            .all()
            if _clean(pub.pmid).isdigit()
        ]
        if not pmids:
            pmids = _extract_pmids(trial.pubmed_links)
        if pmids:
            mesh_terms = _fetch_pubmed_mesh_terms(pmids[:5])
        new_class = _score_therapeutic_class(
            trial.therapeutic_class,
            trial.focus_tags,
            mesh_terms,
        )
        if new_class and new_class != (trial.therapeutic_class or "").strip().lower():
            trial.therapeutic_class = new_class
            updated += 1

    session.commit()
    return updated


def compute_signal_fields(session) -> int:
    bind = session.get_bind()
    conn = None
    try:
        if getattr(bind, "url", None) is not None and bind.url.get_backend_name() == "sqlite":
            conn = sqlite3.connect(bind.url.database or "pdac_trials.db")
        else:
            conn = bind.raw_connection()
        df = pd.read_sql_query(
            """
            SELECT
                nct_id,
                phase,
                status,
                pubmed_links,
                primary_completion_date,
                publication_date
            FROM clinical_trials
            """,
            conn,
        )
    finally:
        if conn is not None:
            conn.close()

    if df.empty:
        return 0

    phase_raw = df["phase"].fillna("").astype(str).str.lower()
    phase_1 = phase_raw.str.contains(r"phase\s*i\b|phase\s*1", regex=True)
    phase_2 = phase_raw.str.contains(r"phase\s*ii\b|phase\s*2", regex=True)
    phase_3 = phase_raw.str.contains(r"phase\s*iii\b|phase\s*3", regex=True)
    phase_4 = phase_raw.str.contains(r"phase\s*iv\b|phase\s*4", regex=True)
    phase_only_1 = phase_1 & ~(phase_2 | phase_3 | phase_4)
    phase_ge_2 = phase_2 | phase_3 | phase_4

    status_raw = df["status"].fillna("").astype(str).str.lower()
    status_terminal = status_raw.str.contains("completed|terminated", regex=True)

    pubmed_raw = df["pubmed_links"].fillna("").astype(str).str.strip()
    has_pubmed = (pubmed_raw != "") & (pubmed_raw.str.upper() != "NA")

    primary_dt = pd.to_datetime(df["primary_completion_date"], errors="coerce")
    pub_dt = pd.to_datetime(df["publication_date"], errors="coerce")
    lag_days_raw = (pub_dt - primary_dt).dt.days
    # Keep publication lag focused on post-completion publication timing.
    # Negative values are treated as anomalies and not stored as lag.
    lag_days = lag_days_raw.where(lag_days_raw >= 0)

    now = pd.Timestamp.utcnow()
    if getattr(now, "tzinfo", None) is not None:
        now = now.tz_localize(None)
    age_days = (now.normalize() - primary_dt).dt.days
    older_than_5y = age_days >= (365 * 5)
    no_pubmed = ~has_pubmed

    evidence_strength = np.select(
        [
            status_terminal & no_pubmed & older_than_5y,
            phase_3 & has_pubmed,
            phase_2 & has_pubmed,
            phase_only_1,
        ],
        ["very_low", "high", "medium", "low"],
        default="unknown",
    )

    dead_end = np.where(
        phase_ge_2 & status_terminal & no_pubmed & older_than_5y,
        "yes",
        "no",
    )

    updates = pd.DataFrame(
        {
            "nct_id": df["nct_id"],
            "evidence_strength": evidence_strength,
            "publication_lag_days": lag_days,
            "dead_end": dead_end,
        }
    )

    updated = 0
    for row in updates.itertuples(index=False):
        trial = session.get(ClinicalTrial, row.nct_id)
        if not trial:
            continue
        trial.evidence_strength = row.evidence_strength
        if pd.isna(row.publication_lag_days):
            trial.publication_lag_days = None
        else:
            trial.publication_lag_days = int(row.publication_lag_days)
        trial.dead_end = row.dead_end
        updated += 1

    session.commit()
    return updated


def merge_ctis_overlaps(session):
    """
    De-duplicate CTIS rows already linked to a ClinicalTrials.gov NCT ID.

    Rule:
    - if CTIS row has secondary_id=NCTxxxxx and that NCT exists as a row,
      merge CTIS enrichment into that NCT row and remove CTIS duplicate row.
    """
    ctis_trials = (
        session.query(ClinicalTrial)
        .filter(ClinicalTrial.source == "ctis")
        .filter(ClinicalTrial.secondary_id.like("NCT%"))
        .all()
    )

    merged_count = 0
    for eu_trial in ctis_trials:
        nct_id = (eu_trial.secondary_id or "").strip()
        if not nct_id:
            continue
        us_trial = session.get(ClinicalTrial, nct_id)
        if not us_trial:
            continue
        if us_trial is eu_trial:
            continue

        # Mark this row as correlated with CTIS and keep the EU id as alternate id.
        us_trial.source = "clinicaltrials.gov+ctis"
        us_trial.secondary_id = as_na(_merge_values(us_trial.secondary_id, eu_trial.nct_id, sep=", "))
        us_trial.trial_link = as_na(_merge_values(us_trial.trial_link, eu_trial.trial_link, sep=" | "))

        if is_na(us_trial.sponsor) and not is_na(eu_trial.sponsor):
            us_trial.sponsor = eu_trial.sponsor
        if is_na(us_trial.status) and not is_na(eu_trial.status):
            us_trial.status = eu_trial.status
        if is_na(us_trial.study_type) and not is_na(eu_trial.study_type):
            us_trial.study_type = eu_trial.study_type
        if is_na(us_trial.phase) and not is_na(eu_trial.phase):
            us_trial.phase = eu_trial.phase
        if is_na(us_trial.study_design) and not is_na(eu_trial.study_design):
            us_trial.study_design = eu_trial.study_design

        us_trial.intervention_types = as_na(
            _merge_values(us_trial.intervention_types, eu_trial.intervention_types, sep=", ")
        )
        us_trial.focus_tags = as_na(
            _merge_values(us_trial.focus_tags, eu_trial.focus_tags, sep=",")
        )
        us_trial.pubmed_links = as_na(
            _merge_values(us_trial.pubmed_links, eu_trial.pubmed_links, sep=" | ")
        )

        if (eu_trial.has_results or "").strip().lower() == "yes":
            us_trial.has_results = "yes"
        us_results_date = _parse_date_key(us_trial.results_last_update)
        eu_results_date = _parse_date_key(eu_trial.results_last_update)
        if eu_results_date and (not us_results_date or eu_results_date > us_results_date):
            us_trial.results_last_update = eu_results_date

        us_last_update = _parse_date_key(us_trial.last_update_date)
        eu_last_update = _parse_date_key(eu_trial.last_update_date)
        if eu_last_update and (not us_last_update or eu_last_update > us_last_update):
            us_trial.last_update_date = eu_last_update

        if (us_trial.therapeutic_class or "").strip().lower() in {"", "na", "context_classified"}:
            if not is_na(eu_trial.therapeutic_class):
                us_trial.therapeutic_class = eu_trial.therapeutic_class
        if (us_trial.pdac_match_reason or "").strip().lower() in {"", "na", "unknown_match"}:
            if not is_na(eu_trial.pdac_match_reason):
                us_trial.pdac_match_reason = eu_trial.pdac_match_reason

        us_details = session.get(ClinicalTrialDetails, us_trial.nct_id)
        if not us_details:
            us_details = ClinicalTrialDetails(nct_id=us_trial.nct_id)
            session.add(us_details)
        eu_details = session.get(ClinicalTrialDetails, eu_trial.nct_id)

        if eu_details:
            us_details.conditions = as_na(_merge_values(us_details.conditions, eu_details.conditions, sep=" | "))
            us_details.interventions = as_na(
                _merge_values(us_details.interventions, eu_details.interventions, sep=" | ")
            )
            us_details.primary_outcomes = as_na(
                _merge_values(us_details.primary_outcomes, eu_details.primary_outcomes, sep=" | ")
            )
            us_details.secondary_outcomes = as_na(
                _merge_values(us_details.secondary_outcomes, eu_details.secondary_outcomes, sep=" | ")
            )
            us_details.locations = as_na(_merge_values(us_details.locations, eu_details.locations, sep=" | "))
            if is_na(us_details.inclusion_criteria) and not is_na(eu_details.inclusion_criteria):
                us_details.inclusion_criteria = eu_details.inclusion_criteria
            if is_na(us_details.exclusion_criteria) and not is_na(eu_details.exclusion_criteria):
                us_details.exclusion_criteria = eu_details.exclusion_criteria
            if is_na(us_details.brief_summary) and not is_na(eu_details.brief_summary):
                us_details.brief_summary = eu_details.brief_summary
            if is_na(us_details.detailed_description) and not is_na(eu_details.detailed_description):
                us_details.detailed_description = eu_details.detailed_description

            session.delete(eu_details)

        session.delete(eu_trial)
        merged_count += 1

    session.commit()
    return merged_count


def ensure_columns(session):
    """
    Lightweight SQLite schema migration for new metadata columns.
    """
    required = {
        "source": "TEXT",
        "secondary_id": "TEXT",
        "trial_link": "TEXT",
        "admission_date": "TEXT",
        "last_update_date": "TEXT",
        "has_results": "TEXT",
        "results_last_update": "TEXT",
        "pubmed_links": "TEXT",
        "intervention_types": "TEXT",
        "primary_completion_date": "TEXT",
        "publication_date": "TEXT",
        "publication_lag_days": "INTEGER",
        "evidence_strength": "TEXT",
        "dead_end": "TEXT",
    }
    rows = session.execute(text("PRAGMA table_info(clinical_trials)")).fetchall()
    existing = {row[1] for row in rows}
    for col, col_type in required.items():
        if col not in existing:
            session.execute(text(f"ALTER TABLE clinical_trials ADD COLUMN {col} {col_type}"))
    session.commit()


def ensure_details_table_and_backfill(session):
    """
    Create details table (via SQL for existing DBs) and backfill from any
    legacy columns still present in `clinical_trials`.
    """
    session.execute(
        text(
            """
            CREATE TABLE IF NOT EXISTS clinical_trial_details (
                nct_id TEXT PRIMARY KEY,
                conditions TEXT,
                interventions TEXT,
                primary_outcomes TEXT,
                secondary_outcomes TEXT,
                inclusion_criteria TEXT,
                exclusion_criteria TEXT,
                locations TEXT,
                brief_summary TEXT,
                detailed_description TEXT,
                FOREIGN KEY(nct_id) REFERENCES clinical_trials(nct_id)
            )
            """
        )
    )

    # Backfill once from legacy wide-table columns if they still exist.
    rows = session.execute(text("PRAGMA table_info(clinical_trials)")).fetchall()
    existing = {row[1] for row in rows}
    legacy = [
        "conditions",
        "interventions",
        "primary_outcomes",
        "secondary_outcomes",
        "inclusion_criteria",
        "exclusion_criteria",
        "locations",
        "brief_summary",
        "detailed_description",
    ]
    if all(col in existing for col in legacy):
        session.execute(
            text(
                """
                INSERT INTO clinical_trial_details (
                    nct_id, conditions, interventions, primary_outcomes, secondary_outcomes,
                    inclusion_criteria, exclusion_criteria, locations, brief_summary, detailed_description
                )
                SELECT
                    c.nct_id, c.conditions, c.interventions, c.primary_outcomes, c.secondary_outcomes,
                    c.inclusion_criteria, c.exclusion_criteria, c.locations, c.brief_summary, c.detailed_description
                FROM clinical_trials c
                LEFT JOIN clinical_trial_details d ON d.nct_id = c.nct_id
                WHERE d.nct_id IS NULL
                """
            )
        )

    session.commit()


def enrich_pubmed_links(session, max_lookups: int = 200):
    """
    Best-effort PubMed enrichment:
    - find trials without PubMed links
    - query PubMed by NCT ID
    - set has_results=yes when papers exist
    """
    if max_lookups <= 0:
        return 0, 0

    candidates = (
        session.query(ClinicalTrial)
        .filter(
            (ClinicalTrial.pubmed_links.is_(None))
            | (ClinicalTrial.pubmed_links == "")
            | (ClinicalTrial.pubmed_links == "NA")
        )
        .order_by(ClinicalTrial.nct_id.asc())
        .limit(max_lookups)
        .all()
    )

    enriched = 0
    updated_results = 0

    for t in candidates:
        lookup_id = ""
        nct_id = (t.nct_id or "").strip()
        secondary_id = (t.secondary_id or "").strip()
        if nct_id.startswith("NCT"):
            lookup_id = nct_id
        elif secondary_id.startswith("NCT"):
            lookup_id = secondary_id
        if not lookup_id:
            continue

        links = _fetch_pubmed_links_by_nct(lookup_id, max_links=3)
        if not links:
            continue
        t.pubmed_links = links
        enriched += 1
        if (t.has_results or "").strip().lower() != "yes":
            t.has_results = "yes"
            updated_results += 1

    session.commit()
    return enriched, updated_results


# -------------------------------------------------------------------
# Main ingestion routine
# -------------------------------------------------------------------

def run():
    """
    Main ingestion flow:
    - fetch pancreas-related trials (already filtered & classified)
    - upsert into DB
    """

    init_db()
    session = SessionLocal()
    ensure_columns(session)
    ensure_details_table_and_backfill(session)
    ensure_publications_table(session)

    print("Fetching PDAC-related trials from ClinicalTrials.gov ...")
    ctgov_studies = fetch_trials_pancreas()

    include_ctis = os.getenv("INGEST_CTIS", "1").strip().lower() not in {"0", "false", "no"}
    ctis_studies = []
    if include_ctis:
        print("Fetching PDAC-related trials from CTIS (EU) ...")
        ctis_max_trials = os.getenv("CTIS_MAX_TRIALS")
        ctis_max_overview = os.getenv("CTIS_MAX_OVERVIEW")
        ctis_medical_condition = os.getenv("CTIS_MEDICAL_CONDITION", "").strip() or None
        ctis_query_terms_raw = os.getenv("CTIS_QUERY_TERMS", "").strip()
        ctis_query_terms = (
            [term.strip() for term in ctis_query_terms_raw.split(",") if term.strip()]
            if ctis_query_terms_raw
            else None
        )
        ctis_page_size = int(os.getenv("CTIS_PAGE_SIZE", "100"))
        ctis_studies = fetch_trials_ctis_pdac(
            max_trials=int(ctis_max_trials) if ctis_max_trials else None,
            max_overview_records=int(ctis_max_overview) if ctis_max_overview else None,
            medical_condition=ctis_medical_condition,
            query_terms=ctis_query_terms,
            page_size=ctis_page_size,
        )
    studies = ctgov_studies + ctis_studies

    inserted = 0
    updated = 0

    for s in studies:
        nct_id = s["nct_id"]

        trial = session.get(ClinicalTrial, nct_id)
        if not trial:
            trial = ClinicalTrial(nct_id=nct_id)
            session.add(trial)
            inserted += 1
        else:
            updated += 1

        # -----------------------------
        # Core fields
        # -----------------------------
        source = (s.get("source") or "").strip().lower()
        trial.source = as_na(source if source else "clinicaltrials.gov")
        trial.secondary_id = as_na(s.get("secondary_id"))
        trial.trial_link = as_na(
            s.get("trial_link")
            or (f"https://clinicaltrials.gov/study/{nct_id}" if nct_id.startswith("NCT") else "")
        )
        trial.title = as_na(s.get("title"))
        trial.study_type = as_na(s.get("study_type"))
        trial.phase = as_na(s.get("phase"))
        trial.status = as_na(s.get("status"))
        trial.sponsor = as_na(s.get("sponsor"))
        trial.admission_date = as_na(s.get("admission_date"))
        trial.last_update_date = as_na(s.get("last_update_date"))
        trial.primary_completion_date = as_na(s.get("primary_completion_date"))
        trial.has_results = as_na(s.get("has_results"))
        trial.results_last_update = as_na(s.get("results_last_update"))
        trial.pubmed_links = as_na(s.get("pubmed_links"))
        trial.intervention_types = as_na(s.get("intervention_types"))
        if "publication_date" in s:
            trial.publication_date = as_na(s.get("publication_date"))
        if "publication_lag_days" in s:
            trial.publication_lag_days = s.get("publication_lag_days")
        if "evidence_strength" in s:
            trial.evidence_strength = as_na(s.get("evidence_strength"))
        if "dead_end" in s:
            trial.dead_end = as_na(s.get("dead_end"))

        # -----------------------------
        # Semantic classification
        # -----------------------------
        trial.study_design = as_na(s.get("study_design"))
        trial.therapeutic_class = as_na(s.get("therapeutic_class"))
        trial.focus_tags = as_na(s.get("focus_tags"))
        trial.pdac_match_reason = as_na(s.get("pdac_match_reason"))

        details = session.get(ClinicalTrialDetails, nct_id)
        if not details:
            details = ClinicalTrialDetails(nct_id=nct_id)
            session.add(details)
        details.conditions = as_na(s.get("conditions"))
        details.interventions = as_na(s.get("interventions"))
        details.primary_outcomes = as_na(s.get("primary_outcomes"))
        details.secondary_outcomes = as_na(s.get("secondary_outcomes"))
        details.inclusion_criteria = as_na(s.get("inclusion_criteria"))
        details.exclusion_criteria = as_na(s.get("exclusion_criteria"))
        details.locations = as_na(s.get("locations"))
        details.brief_summary = as_na(s.get("brief_summary"))
        details.detailed_description = as_na(s.get("detailed_description"))

    session.commit()

    pubmed_lookup_limit = int(os.getenv("PUBMED_LOOKUP_LIMIT", "200"))
    enriched, results_fixed = enrich_pubmed_links(session, max_lookups=pubmed_lookup_limit)
    merged_ctis = merge_ctis_overlaps(session)
    pubmed_date_limit = int(os.getenv("PUBMED_DATE_LOOKUP_LIMIT", "200"))
    mesh_lookup_limit = int(os.getenv("PUBMED_MESH_LOOKUP_LIMIT", "200"))
    pubmed_nct_lookup_limit = int(os.getenv("PUBMED_NCT_LOOKUP_LIMIT", "400"))
    pubmed_title_lookup_limit = int(os.getenv("PUBMED_TITLE_LOOKUP_LIMIT", "300"))
    pubmed_doi_lookup_limit = int(os.getenv("PUBMED_DOI_LOOKUP_LIMIT", "200"))
    pubmed_per_trial_limit = int(os.getenv("PUBMED_PER_TRIAL_LINK_LIMIT", "5"))
    publication_stats = rebuild_trial_publications(
        session,
        max_nct_lookups=pubmed_nct_lookup_limit,
        max_title_lookups=pubmed_title_lookup_limit,
        max_doi_lookups=pubmed_doi_lookup_limit,
        max_links_per_trial=pubmed_per_trial_limit,
    )
    publication_rows_refreshed = refresh_trial_publication_summary(session)
    pubmed_dates = backfill_pubmed_publication_dates(session, max_lookups=pubmed_date_limit)
    mesh_updated = improve_therapeutic_class_ensemble(session, max_lookups=mesh_lookup_limit)
    signal_updated = compute_signal_fields(session)

    print(f"\nTrials processed: {len(studies)}")
    print(f"ClinicalTrials.gov rows: {len(ctgov_studies)}")
    if include_ctis:
        print(f"CTIS rows: {len(ctis_studies)}")
    print(f"New trials inserted: {inserted}")
    print(f"Existing trials updated: {updated}")
    print(f"CTIS↔NCT overlaps merged: {merged_ctis}")
    print(f"PubMed links enriched (this run): {enriched}")
    print(f"has_results corrected from PubMed links: {results_fixed}")
    print(
        "Publication index: "
        f"rows={publication_stats['publication_rows']}, "
        f"trials={publication_stats['trials_with_publications']}, "
        f"nct_lookups={publication_stats['nct_lookups_used']}, "
        f"title_lookups={publication_stats['title_lookups_used']}, "
        f"doi_lookups={publication_stats['doi_lookups_used']}"
    )
    print(f"Publication summary refreshed: {publication_rows_refreshed}")
    print(f"PubMed publication dates added: {pubmed_dates}")
    print(f"Therapeutic class updated via ensemble: {mesh_updated}")
    print(f"Signal fields updated: {signal_updated}")

    session.close()


# -------------------------------------------------------------------
# Entry point
# -------------------------------------------------------------------

if __name__ == "__main__":
    run()
