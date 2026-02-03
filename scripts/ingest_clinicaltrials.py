"""
Ingest PDAC-related clinical trials from ClinicalTrials.gov,
and store them in the local database.

This script does NOT reclassify trials.
All semantic classification is done upstream (clinicaltrials.py).
"""

from ingest.clinicaltrials import fetch_trials_pancreas
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


def ensure_columns(session):
    """
    Lightweight SQLite schema migration for new metadata columns.
    """
    required = {
        "admission_date": "TEXT",
        "last_update_date": "TEXT",
        "has_results": "TEXT",
        "results_last_update": "TEXT",
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

    studies = fetch_trials_pancreas()

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
        trial.title = as_na(s.get("title"))
        trial.study_type = as_na(s.get("study_type"))
        trial.phase = as_na(s.get("phase"))
        trial.status = as_na(s.get("status"))
        trial.sponsor = as_na(s.get("sponsor"))
        trial.admission_date = as_na(s.get("admission_date"))
        trial.last_update_date = as_na(s.get("last_update_date"))
        trial.has_results = as_na(s.get("has_results"))
        trial.results_last_update = as_na(s.get("results_last_update"))
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

    print(f"\nTrials processed: {len(studies)}")
    print(f"New trials inserted: {inserted}")
    print(f"Existing trials updated: {updated}")

    session.close()


# -------------------------------------------------------------------
# Entry point
# -------------------------------------------------------------------

if __name__ == "__main__":
    run()
