"""
Ingest PDAC-related clinical trials from ClinicalTrials.gov,
store them in the database, and print a readable preview table.

This script does NOT reclassify trials.
All semantic classification is done upstream (clinicaltrials.py).
"""

from ingest.clinicaltrials import fetch_trials_pancreas
from db.session import SessionLocal, init_db
from db.models import ClinicalTrial

from tabulate import tabulate


# -------------------------------------------------------------------
# Main ingestion routine
# -------------------------------------------------------------------

def run():
    """
    Main ingestion flow:
    - fetch pancreas-related trials (already filtered & classified)
    - upsert into DB
    - print a preview table
    """

    init_db()
    session = SessionLocal()

    print("Fetching PDAC-related trials from ClinicalTrials.gov ...")

    studies = fetch_trials_pancreas(max_records=1000)

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
        trial.title = s.get("title")
        trial.study_type = s.get("study_type")
        trial.phase = s.get("phase")
        trial.status = s.get("status")
        trial.sponsor = s.get("sponsor")

        # -----------------------------
        # Semantic classification
        # -----------------------------
        trial.study_design = s.get("study_design")
        trial.therapeutic_class = s.get("therapeutic_class")
        trial.focus_tags = s.get("focus_tags")
        trial.pdac_match_reason = s.get("pdac_match_reason")
        trial.noise_flags = s.get("noise_flags")

    session.commit()

    print(f"\nTrials processed: {len(studies)}")
    print(f"New trials inserted: {inserted}")
    print(f"Existing trials updated: {updated}")

    # ----------------------------------------------------------------
    # Console preview table (readable & useful)
    # ----------------------------------------------------------------

    rows = (
        session.query(
            ClinicalTrial.nct_id,
            ClinicalTrial.title,
            ClinicalTrial.study_type,
            ClinicalTrial.phase,
            ClinicalTrial.therapeutic_class,
            ClinicalTrial.focus_tags,
        )
        .order_by(ClinicalTrial.therapeutic_class, ClinicalTrial.phase)
        .limit(40)
        .all()
    )

    print("\nPDAC trials (classified preview):\n")
    print(
        tabulate(
            rows,
            headers=[
                "NCT",
                "Title",
                "StudyType",
                "Phase",
                "TherapeuticClass",
                "Tags",
            ],
            tablefmt="github",
            maxcolwidths=[12, 60, 14, 10, 18, 30],
        )
    )

    session.close()


# -------------------------------------------------------------------
# Entry point
# -------------------------------------------------------------------

if __name__ == "__main__":
    run()
