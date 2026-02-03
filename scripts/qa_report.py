"""
Generate a quick QA report for PDAC trial classification quality.
"""

import argparse
import re
from collections import Counter

from db.models import ClinicalTrial, ClinicalTrialDetails
from db.session import SessionLocal


def print_section(title: str):
    print(f"\n=== {title} ===")


DATE_RE = re.compile(r"^\d{4}(-\d{2}){0,2}$")
ALLOWED_RESULTS = {"yes", "no", "na"}
KNOWN_INTERVENTION_TYPES = {
    "BEHAVIORAL",
    "BIOLOGICAL",
    "COMBINATION_PRODUCT",
    "DIAGNOSTIC_TEST",
    "DIETARY_SUPPLEMENT",
    "DEVICE",
    "DRUG",
    "GENETIC",
    "OTHER",
    "PROCEDURE",
    "RADIATION",
}


def _is_na(value: str) -> bool:
    if value is None:
        return True
    return str(value).strip().lower() in {"", "na"}


def _is_valid_date_or_na(value: str) -> bool:
    if _is_na(value):
        return True
    return bool(DATE_RE.match(str(value).strip()))


def _split_csv(value: str):
    if _is_na(value):
        return []
    return [item.strip() for item in str(value).split(",") if item.strip()]


def run(limit: int, strict: bool):
    db = SessionLocal()
    trials = db.query(ClinicalTrial).all()
    details = db.query(ClinicalTrialDetails).all()
    total = len(trials)

    classes = Counter((t.therapeutic_class or "missing") for t in trials)
    focus_pairs = Counter((t.focus_tags or "none") for t in trials)
    phase_dist = Counter((t.phase or "missing") for t in trials)
    result_dist = Counter((t.has_results or "missing").lower() for t in trials)

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
    unknown_match_reason = [
        t for t in trials if (t.pdac_match_reason or "").strip().lower() == "unknown_match"
    ]
    trial_ids = {t.nct_id for t in trials}
    detail_ids = {d.nct_id for d in details}
    missing_detail_ids = trial_ids - detail_ids
    orphan_detail_ids = detail_ids - trial_ids
    missing_details = [t for t in trials if t.nct_id in missing_detail_ids]
    orphan_details = [d for d in details if d.nct_id in orphan_detail_ids]

    invalid_results = [
        t for t in trials if (t.has_results or "").strip().lower() not in ALLOWED_RESULTS
    ]

    invalid_dates = {
        "admission_date": [t for t in trials if not _is_valid_date_or_na(t.admission_date)],
        "last_update_date": [t for t in trials if not _is_valid_date_or_na(t.last_update_date)],
        "results_last_update": [t for t in trials if not _is_valid_date_or_na(t.results_last_update)],
    }

    intervention_type_issues = []
    for t in trials:
        tokens = _split_csv(t.intervention_types)
        bad = [token for token in tokens if token not in KNOWN_INTERVENTION_TYPES]
        if bad:
            intervention_type_issues.append((t.nct_id, bad))

    print_section("Overview")
    print(f"total_trials: {total}")
    print(f"details_rows: {len(details)}")
    print(f"missing_details_rows: {len(missing_details)}")
    print(f"orphan_details_rows: {len(orphan_details)}")
    print(f"unknown_total: {len(unknown)}")
    print(f"unknown_with_tags: {len(unknown_with_tags)}")

    print_section("Therapeutic Class Distribution")
    for cls, count in classes.most_common():
        print(f"{cls}: {count}")

    print_section("Phase Distribution")
    for phase, count in phase_dist.most_common():
        print(f"{phase}: {count}")

    print_section("Result Availability Distribution")
    for key, count in result_dist.most_common():
        print(f"{key}: {count}")

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
    print(f"invalid_has_results_values: {len(invalid_results)}")
    print(f"invalid_intervention_types: {len(intervention_type_issues)}")
    print(f"unknown_pdac_match_reason: {len(unknown_match_reason)}")
    for field, bad in invalid_dates.items():
        print(f"invalid_{field}: {len(bad)}")

    if len(missing_details) > 0:
        print_section(f"Missing Details Sample (top {limit})")
        for t in missing_details[:limit]:
            print(f"{t.nct_id} | {t.title}")

    if len(invalid_results) > 0:
        print_section(f"Invalid has_results Sample (top {limit})")
        for t in invalid_results[:limit]:
            print(f"{t.nct_id} | has_results={t.has_results}")

    if len(intervention_type_issues) > 0:
        print_section(f"Invalid intervention_type Sample (top {limit})")
        for nct_id, bad_tokens in intervention_type_issues[:limit]:
            print(f"{nct_id} | bad_tokens={','.join(bad_tokens)}")

    if len(unknown_match_reason) > 0:
        print_section(f"Unknown Match Reason Sample (top {limit})")
        for t in unknown_match_reason[:limit]:
            print(f"{t.nct_id} | {t.title}")

    if strict:
        blocking_failures = (
            len(unknown)
            + len(unknown_with_tags)
            + len(mismatch_biomarker)
            + len(mismatch_nontherapeutic_interventional)
            + len(missing_details)
            + len(orphan_details)
            + len(invalid_results)
            + sum(len(v) for v in invalid_dates.values())
            + len(intervention_type_issues)
        )
        if blocking_failures:
            print_section("Strict Mode Result")
            print(f"FAILED: blocking_failures={blocking_failures}")
            db.close()
            raise SystemExit(1)
        print_section("Strict Mode Result")
        print("PASSED: no blocking failures found")

    db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PDAC classification QA report")
    parser.add_argument("--limit", type=int, default=10, help="Sample size per section")
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit with non-zero status if blocking QA failures are found.",
    )
    args = parser.parse_args()
    run(limit=args.limit, strict=args.strict)
