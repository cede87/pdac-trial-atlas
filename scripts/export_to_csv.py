"""
Export PDAC clinical trials table to CSV for manual inspection,
validation, and downstream analysis.
"""

import csv
from db.session import SessionLocal
from db.models import ClinicalTrial, ClinicalTrialDetails


OUTPUT_FILE = "pdac_trials_export.csv"


def run():
    db = SessionLocal()

    trials = db.query(ClinicalTrial).order_by(ClinicalTrial.nct_id).all()

    with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)

        # --------------------------------------------------
        # CSV Header (explicit & human-readable)
        # --------------------------------------------------
        writer.writerow([
            "nct_id",
            "source",
            "secondary_id",
            "trial_link",
            "title",
            "study_type",
            "study_design",
            "phase",
            "status",
            "sponsor",
            "admission_date",
            "last_update_date",
            "primary_completion_date",
            "has_results",
            "results_last_update",
            "pubmed_links",
            "publication_date",
            "publication_lag_days",
            "evidence_strength",
            "dead_end",
            "conditions",
            "interventions",
            "intervention_types",
            "primary_outcomes",
            "secondary_outcomes",
            "inclusion_criteria",
            "exclusion_criteria",
            "locations",
            "brief_summary",
            "detailed_description",
            "therapeutic_class",
            "focus_tags",
            "pdac_match_reason",
        ])

        # --------------------------------------------------
        # Rows
        # --------------------------------------------------
        for t in trials:
            d = db.get(ClinicalTrialDetails, t.nct_id)
            writer.writerow([
                t.nct_id,
                t.source,
                t.secondary_id,
                t.trial_link,
                t.title,
                t.study_type,
                t.study_design,
                t.phase,
                t.status,
                t.sponsor,
                t.admission_date,
                t.last_update_date,
                t.primary_completion_date,
                t.has_results,
                t.results_last_update,
                t.pubmed_links,
                t.publication_date,
                t.publication_lag_days,
                t.evidence_strength,
                t.dead_end,
                (d.conditions if d else "NA"),
                (d.interventions if d else "NA"),
                t.intervention_types,
                (d.primary_outcomes if d else "NA"),
                (d.secondary_outcomes if d else "NA"),
                (d.inclusion_criteria if d else "NA"),
                (d.exclusion_criteria if d else "NA"),
                (d.locations if d else "NA"),
                (d.brief_summary if d else "NA"),
                (d.detailed_description if d else "NA"),
                t.therapeutic_class,
                t.focus_tags,
                t.pdac_match_reason,
            ])

    db.close()
    print(f"CSV exported successfully → {OUTPUT_FILE}")


if __name__ == "__main__":
    run()
