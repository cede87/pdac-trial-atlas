"""
Export PDAC clinical trials table to CSV for manual inspection,
validation, and downstream analysis.
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

        # --------------------------------------------------
        # CSV Header (explicit & human-readable)
        # --------------------------------------------------
        writer.writerow([
            "nct_id",
            "title",
            "study_type",
            "study_design",
            "phase",
            "status",
            "sponsor",
            "therapeutic_class",
            "focus_tags",
            "pdac_match_reason",
            "noise_flags",
        ])

        # --------------------------------------------------
        # Rows
        # --------------------------------------------------
        for t in trials:
            writer.writerow([
                t.nct_id,
                t.title,
                t.study_type,
                t.study_design,
                t.phase,
                t.status,
                t.sponsor,
                t.therapeutic_class,
                t.focus_tags,
                t.pdac_match_reason,
                t.noise_flags,
            ])

    db.close()
    print(f"CSV exported successfully → {OUTPUT_FILE}")


if __name__ == "__main__":
    run()
