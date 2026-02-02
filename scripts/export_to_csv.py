"""
Export clinical trials table to CSV for manual inspection and validation.
"""

import csv
from db.session import SessionLocal
from db.models import ClinicalTrial


OUTPUT_FILE = "pdac_trials_export.csv"


def run():
    db = SessionLocal()

    trials = (
        db.query(ClinicalTrial)
        .order_by(ClinicalTrial.nct_id)
        .all()
    )

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        # Header
        writer.writerow([
            "nct_id",
            "title",
            "study_type",
            "phase",
            "status",
            "sponsor",
            "classification",
            "classification_reason",
        ])

        for t in trials:
            writer.writerow([
                t.nct_id,
                t.title,
                t.study_type,
                t.phase,
                t.status,
                t.sponsor,
                t.classification,
                t.classification_reason,
            ])

    db.close()
    print(f"CSV exported: {OUTPUT_FILE}")


if __name__ == "__main__":
    run()
