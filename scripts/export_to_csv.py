"""
Export PDAC clinical trials table to CSV for manual inspection,
validation, and downstream analysis.
"""

import csv
from db.session import SessionLocal
from db.models import ClinicalTrial, ClinicalTrialDetails, ClinicalTrialPublication
from sqlalchemy import text


OUTPUT_FILE = "pdac_trials_export.csv"


def run():
    db = SessionLocal()

    db.execute(
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
                is_full_match TEXT
            )
            """
        )
    )
    trials = db.query(ClinicalTrial).order_by(ClinicalTrial.nct_id).all()
    publication_columns = {
        row[1] for row in db.execute(text("PRAGMA table_info(trial_publications)")).fetchall()
    }
    if "is_full_match" not in publication_columns:
        db.execute(text("ALTER TABLE trial_publications ADD COLUMN is_full_match TEXT"))
        db.execute(
            text(
                "UPDATE trial_publications SET is_full_match = CASE WHEN COALESCE(confidence, 0) >= 80 THEN 'yes' ELSE 'no' END"
            )
        )
        publication_columns.add("is_full_match")
        db.commit()
    has_full_match_column = "is_full_match" in publication_columns

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
            "publication_scan_date",
            "publication_lag_days",
            "evidence_strength",
            "dead_end",
            "publication_count",
            "publication_match_methods",
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
            pubs_query = (
                db.query(ClinicalTrialPublication)
                .filter(ClinicalTrialPublication.nct_id == t.nct_id)
            )
            if has_full_match_column:
                pubs_query = pubs_query.filter(ClinicalTrialPublication.is_full_match == "yes")
            pubs = pubs_query.all()
            publication_count = len(pubs)
            publication_match_methods = ",".join(
                sorted(
                    {
                        (p.match_method or "").strip()
                        for p in pubs
                        if (p.match_method or "").strip()
                    }
                )
            ) or "NA"
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
                t.publication_scan_date,
                t.publication_lag_days,
                t.evidence_strength,
                t.dead_end,
                publication_count,
                publication_match_methods,
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
