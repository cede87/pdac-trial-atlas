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
ALLOWED_SOURCES = {"clinicaltrials.gov", "ctis", "clinicaltrials.gov+ctis", "na"}


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


def _link_matches_source(source: str, link: str) -> bool:
    src = (source or "").strip().lower()
    url = (link or "").strip().lower()
    if src in {"", "na"}:
        return bool(url)
    if src == "clinicaltrials.gov":
        return "clinicaltrials.gov/study/" in url
    if src == "ctis":
        return "euclinicaltrials.eu/search-for-clinical-trials/" in url
    if src == "clinicaltrials.gov+ctis":
        return (
            "clinicaltrials.gov/study/" in url
            and "euclinicaltrials.eu/search-for-clinical-trials/" in url
        )
    return False


def run(limit: int, strict: bool):
    db = SessionLocal()
    trials = db.query(ClinicalTrial).all()
    details = db.query(ClinicalTrialDetails).all()
    total = len(trials)

    classes = Counter((t.therapeutic_class or "missing") for t in trials)
    source_dist = Counter(((t.source or "missing").strip().lower() or "missing") for t in trials)
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
    has_results_conflicts = [
        t for t in trials
        if (t.pubmed_links or "").strip().lower() not in {"", "na"}
        and (t.has_results or "").strip().lower() not in {"yes"}
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

    invalid_sources = [
        t
        for t in trials
        if (t.source or "").strip().lower() not in ALLOWED_SOURCES
    ]
    invalid_source_links = [
        t
        for t in trials
        if not _link_matches_source(t.source or "", t.trial_link or "")
    ]
    id_pat_nct = re.compile(r"^NCT\d+$")
    id_pat_eu = re.compile(r"^\d{4}-\d{6}-\d{2}-\d{2}$")
    invalid_secondary_ids = []
    for t in trials:
        if _is_na(t.secondary_id):
            continue
        parts = [x.strip() for x in str(t.secondary_id).split(",") if x.strip()]
        if not parts:
            continue
        bad = [x for x in parts if not (id_pat_nct.match(x) or id_pat_eu.match(x))]
        if bad:
            invalid_secondary_ids.append(t)
    ctis_trials = [t for t in trials if (t.source or "").strip().lower() == "ctis"]
    ctis_missing_key_fields = [
        t
        for t in ctis_trials
        if _is_na(t.sponsor) or _is_na(t.admission_date) or _is_na(t.last_update_date)
    ]
    ctis_broad_scope_flags = [
        t
        for t in ctis_trials
        if any(
            tag in (t.focus_tags or "")
            for tag in ("mixed_solid_tumor", "neuroendocrine_signal", "hepatobiliary_signal")
        )
    ]

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

    print_section("Source Distribution")
    for src, count in source_dist.most_common():
        print(f"{src}: {count}")

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
    print(f"has_results_conflicts_with_pubmed_links: {len(has_results_conflicts)}")
    print(f"invalid_intervention_types: {len(intervention_type_issues)}")
    print(f"invalid_source_values: {len(invalid_sources)}")
    print(f"invalid_source_trial_links: {len(invalid_source_links)}")
    print(f"invalid_secondary_ids: {len(invalid_secondary_ids)}")
    print(f"ctis_missing_key_fields: {len(ctis_missing_key_fields)}")
    print(f"ctis_broad_scope_signals: {len(ctis_broad_scope_flags)}")
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

    if len(has_results_conflicts) > 0:
        print_section(f"has_results Conflict Sample (top {limit})")
        for t in has_results_conflicts[:limit]:
            print(f"{t.nct_id} | has_results={t.has_results} | pubmed_links={t.pubmed_links}")

    if len(intervention_type_issues) > 0:
        print_section(f"Invalid intervention_type Sample (top {limit})")
        for nct_id, bad_tokens in intervention_type_issues[:limit]:
            print(f"{nct_id} | bad_tokens={','.join(bad_tokens)}")

    if len(invalid_sources) > 0:
        print_section(f"Invalid source values Sample (top {limit})")
        for t in invalid_sources[:limit]:
            print(f"{t.nct_id} | source={t.source}")

    if len(invalid_source_links) > 0:
        print_section(f"Invalid source trial_link Sample (top {limit})")
        for t in invalid_source_links[:limit]:
            print(f"{t.nct_id} | source={t.source} | link={t.trial_link}")

    if len(invalid_secondary_ids) > 0:
        print_section(f"Invalid secondary_id Sample (top {limit})")
        for t in invalid_secondary_ids[:limit]:
            print(f"{t.nct_id} | secondary_id={t.secondary_id}")

    if len(ctis_missing_key_fields) > 0:
        print_section(f"CTIS missing key fields Sample (top {limit})")
        for t in ctis_missing_key_fields[:limit]:
            print(
                f"{t.nct_id} | sponsor={t.sponsor} | admission_date={t.admission_date} | last_update_date={t.last_update_date}"
            )

    if len(ctis_broad_scope_flags) > 0:
        print_section(f"CTIS broad-scope signal Sample (top {limit})")
        for t in ctis_broad_scope_flags[:limit]:
            print(f"{t.nct_id} | tags={t.focus_tags} | {t.title}")

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
            + len(has_results_conflicts)
            + sum(len(v) for v in invalid_dates.values())
            + len(intervention_type_issues)
            + len(invalid_sources)
            + len(invalid_source_links)
            + len(invalid_secondary_ids)
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
