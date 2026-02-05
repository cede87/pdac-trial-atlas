"""
Ingest PDAC-related clinical trials from ClinicalTrials.gov,
and CTIS (EU Clinical Trials Information System), then store them
in the local database.

This script does NOT reclassify trials.
All semantic classification is done upstream (ingest modules).
"""

import os
import re

from ingest.clinicaltrials import fetch_trials_pancreas, _fetch_pubmed_links_by_nct
from ingest.ctis import fetch_trials_ctis_pdac
from db.session import SessionLocal, init_db
from db.models import ClinicalTrial, ClinicalTrialDetails
from sqlalchemy import text


def as_na(value):
    """
    Normalize missing/blank values to 'NA' for consistent downstream UX.
    """
    if value is None:
        return "NA"
    if isinstance(value, str) and not value.strip():
        return "NA"
    return value


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


def _parse_date_key(value: str) -> str:
    if is_na(value):
        return ""
    value = str(value).strip()
    return value if re.match(r"^\d{4}(-\d{2}){0,2}$", value) else ""


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
        trial.has_results = as_na(s.get("has_results"))
        trial.results_last_update = as_na(s.get("results_last_update"))
        trial.pubmed_links = as_na(s.get("pubmed_links"))
        trial.intervention_types = as_na(s.get("intervention_types"))

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

    print(f"\nTrials processed: {len(studies)}")
    print(f"ClinicalTrials.gov rows: {len(ctgov_studies)}")
    if include_ctis:
        print(f"CTIS rows: {len(ctis_studies)}")
    print(f"New trials inserted: {inserted}")
    print(f"Existing trials updated: {updated}")
    print(f"CTIS↔NCT overlaps merged: {merged_ctis}")
    print(f"PubMed links enriched (this run): {enriched}")
    print(f"has_results corrected from PubMed links: {results_fixed}")

    session.close()


# -------------------------------------------------------------------
# Entry point
# -------------------------------------------------------------------

if __name__ == "__main__":
    run()
