"""
Generate a quick QA report for PDAC trial classification quality.
"""

import argparse
from collections import Counter

from db.models import ClinicalTrial
from db.session import SessionLocal


def print_section(title: str):
    print(f"\n=== {title} ===")


def run(limit: int):
    db = SessionLocal()
    trials = db.query(ClinicalTrial).all()
    total = len(trials)

    classes = Counter((t.therapeutic_class or "missing") for t in trials)
    focus_pairs = Counter((t.focus_tags or "none") for t in trials)

    unknown = [t for t in trials if (t.therapeutic_class or "") == "unknown"]
    unknown_with_tags = [t for t in unknown if (t.focus_tags or "").strip()]

    mismatch_biomarker = [
        t for t in trials
        if (t.therapeutic_class == "biomarker_diagnostics")
        and ("biomarker" not in (t.focus_tags or ""))
    ]
    mismatch_nontherapeutic_interventional = [
        t for t in trials
        if (t.therapeutic_class == "observational_non_therapeutic")
        and (t.study_type == "INTERVENTIONAL")
    ]

    print_section("Overview")
    print(f"total_trials: {total}")
    print(f"unknown_total: {len(unknown)}")
    print(f"unknown_with_tags: {len(unknown_with_tags)}")

    print_section("Therapeutic Class Distribution")
    for cls, count in classes.most_common():
        print(f"{cls}: {count}")

    print_section(f"Top Focus Tag Patterns (top {limit})")
    for tags, count in focus_pairs.most_common(limit):
        print(f"{tags}: {count}")

    print_section(f"Unknown Sample (top {limit})")
    for t in unknown[:limit]:
        print(f"{t.nct_id} | {t.study_type} | {t.phase} | {t.title}")

    print_section("Consistency Checks")
    print(f"biomarker_class_without_biomarker_tag: {len(mismatch_biomarker)}")
    print(
        "observational_non_therapeutic_with_interventional_study_type: "
        f"{len(mismatch_nontherapeutic_interventional)}"
    )

    db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PDAC classification QA report")
    parser.add_argument("--limit", type=int, default=10, help="Sample size per section")
    args = parser.parse_args()
    run(limit=args.limit)
