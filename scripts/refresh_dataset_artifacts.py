#!/usr/bin/env python3
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
import sqlite3

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "pdac_trials.db"
DATASET_DIR = ROOT / "dataset"

BOOL_FIELDS = {
    "has_publication",
    "is_post_2015",
    "is_phase_1_2_combined",
    "is_randomized",
    "is_multi_center",
    "is_multi_country",
    "is_combination_therapy",
    "is_top_10_sponsor",
    "is_novel_target",
    "is_literature_rich_trial_sparse",
    "dead_end",
    "journal_impact_flag",
}

NUMERIC_FIELDS = {
    "publication_count",
    "publication_lag_days",
    "source_count",
    "phase_numeric",
    "num_arms",
    "country_count",
    "start_year",
    "completion_year",
    "duration_months",
    "publication_delay_months",
    "years_since_start",
    "binary_success_label",
    "sponsor_trial_count_total",
    "sponsor_trial_count_last_5y",
    "sponsor_success_rate_historical",
    "target_trial_count_total",
    "target_trial_count_last_5y",
    "target_success_rate_historical",
    "target_literature_count_last_5y",
    "literature_trial_ratio",
}

MODEL_VIEW_COLUMNS = [
    "trial_uid",
    "binary_success_label",
    "start_year",
    "phase_numeric",
    "is_phase_1_2_combined",
    "num_arms",
    "is_randomized",
    "is_multi_center",
    "country_count",
    "is_multi_country",
    "intervention_type",
    "is_combination_therapy",
    "sponsor_type",
    "sponsor_trial_count_total",
    "sponsor_trial_count_last_5y",
    "sponsor_success_rate_historical",
    "is_top_10_sponsor",
    "target_primary",
    "target_category",
    "target_trial_count_total",
    "target_trial_count_last_5y",
    "target_success_rate_historical",
    "is_novel_target",
]

FEATURE_TEMPORAL_SCOPE = {
    "start_year": "pre_start",
    "phase_numeric": "pre_start",
    "is_phase_1_2_combined": "pre_start",
    "num_arms": "pre_start",
    "is_randomized": "pre_start",
    "is_multi_center": "pre_start",
    "country_count": "pre_start",
    "is_multi_country": "pre_start",
    "intervention_type": "pre_start",
    "is_combination_therapy": "pre_start",
    "sponsor_type": "pre_start",
    "sponsor_trial_count_total": "pre_start",
    "sponsor_trial_count_last_5y": "pre_start",
    "sponsor_success_rate_historical": "pre_start",
    "is_top_10_sponsor": "pre_start",
    "target_primary": "pre_start",
    "target_category": "pre_start",
    "target_trial_count_total": "pre_start",
    "target_trial_count_last_5y": "pre_start",
    "target_success_rate_historical": "pre_start",
    "is_novel_target": "pre_start",
    "trial_outcome_label": "post_outcome",
    "binary_success_label": "post_outcome",
    "has_publication": "post_outcome",
    "publication_year_first": "post_outcome",
    "publication_count": "post_outcome",
    "publication_delay_months": "post_outcome",
    "journal_impact_flag": "post_outcome",
    "dead_end": "post_outcome",
    "evidence_strength": "post_outcome",
    "publication_lag_days": "post_outcome",
    "publication_date": "post_outcome",
    "publication_scan_date": "post_outcome",
    "completion_year": "post_outcome",
    "duration_months": "post_outcome",
    "years_since_start": "post_outcome",
    "target_literature_count_last_5y": "post_outcome",
    "literature_trial_ratio": "post_outcome",
    "is_literature_rich_trial_sparse": "post_outcome",
    "trial_uid": "static",
    "source_count": "static",
    "sources_list": "static",
}


def _to_bool01(series: pd.Series) -> pd.Series:
    mapped = (
        series.astype(str)
        .str.strip()
        .str.lower()
        .replace(
            {
                "yes": "1",
                "true": "1",
                "1": "1",
                "no": "0",
                "false": "0",
                "0": "0",
                "na": pd.NA,
                "none": pd.NA,
                "nan": pd.NA,
                "": pd.NA,
            }
        )
    )
    return pd.to_numeric(mapped, errors="coerce").astype("Int64")


def load_ml_ready() -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)
    try:
        df = pd.read_sql_query(
            "SELECT * FROM clinical_trials_ml_ready ORDER BY trial_uid ASC", conn
        )
    finally:
        conn.close()
    return df


def normalize_types(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in BOOL_FIELDS:
        if col in df.columns:
            df[col] = _to_bool01(df[col])
    for col in NUMERIC_FIELDS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def add_feature_temporal_scope(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    mapping = json.dumps(FEATURE_TEMPORAL_SCOPE, sort_keys=True)
    df["feature_temporal_scope"] = mapping
    return df


def fill_na_for_csv(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            series = df[col]
            def _fmt_num(val):
                if pd.isna(val):
                    return "NA"
                try:
                    num = float(val)
                    if num.is_integer():
                        return str(int(num))
                    return str(num)
                except Exception:
                    return str(val)
            df[col] = series.map(_fmt_num)
        else:
            df[col] = (
                df[col]
                .astype(str)
                .replace({"<NA>": "NA", "nan": "NA", "None": "NA", "": "NA"})
            )
    return df


def build_yearly_metrics(df: pd.DataFrame) -> pd.DataFrame:
    metrics_cols = [
        "year",
        "trials_started",
        "trials_completed",
        "trials_terminated",
        "success_rate",
        "avg_duration_by_phase",
        "top_target_by_count",
        "new_targets_introduced",
    ]
    by_year = df.copy()
    if "start_year" not in by_year.columns:
        return pd.DataFrame(columns=metrics_cols)
    by_year["start_year"] = pd.to_numeric(by_year["start_year"], errors="coerce")
    by_year = by_year.dropna(subset=["start_year"])
    by_year["year"] = by_year["start_year"].astype(int)

    status_raw = by_year["status"].astype(str).str.upper()
    completed = status_raw.str.contains("COMPLETED", regex=False)
    terminated = status_raw.str.contains("TERMINATED|WITHDRAWN|SUSPENDED", regex=True)

    outcome = by_year.get("trial_outcome_label", pd.Series([], dtype=str)).astype(str).str.lower()
    success = outcome == "success"
    failure = outcome.isin(["failure", "completed_no_publication"])

    rows = []
    seen_targets = set()
    for year, group in by_year.groupby("year"):
        trials_started = len(group)
        trials_completed = int(completed.loc[group.index].sum())
        trials_terminated = int(terminated.loc[group.index].sum())

        success_count = int(success.loc[group.index].sum())
        failure_count = int(failure.loc[group.index].sum())
        denom = success_count + failure_count
        success_rate = round(success_count / denom, 3) if denom else "NA"

        phase_avg = (
            group.assign(duration_months=pd.to_numeric(group["duration_months"], errors="coerce"))
            .groupby("phase")["duration_months"]
            .mean()
            .dropna()
            .round(2)
            .to_dict()
        )

        target_counts = (
            group.get("target_primary", pd.Series([], dtype=str))
            .replace("NA", pd.NA)
            .dropna()
            .value_counts()
        )
        top_target = target_counts.idxmax() if not target_counts.empty else "NA"

        targets = set(
            group.get("target_primary", pd.Series([], dtype=str))
            .replace("NA", pd.NA)
            .dropna()
            .unique()
            .tolist()
        )
        new_targets = targets - seen_targets
        seen_targets |= targets

        rows.append(
            {
                "year": int(year),
                "trials_started": trials_started,
                "trials_completed": trials_completed,
                "trials_terminated": trials_terminated,
                "success_rate": success_rate,
                "avg_duration_by_phase": json.dumps(phase_avg, sort_keys=True),
                "top_target_by_count": top_target,
                "new_targets_introduced": len(new_targets),
            }
        )

    return pd.DataFrame(rows, columns=metrics_cols).sort_values("year")


def build_schema(df: pd.DataFrame, metrics_df: pd.DataFrame, model_df: pd.DataFrame) -> dict:
    known_descriptions = {
        "nct_id": "Primary trial identifier (NCT ID for ClinicalTrials.gov rows, EU CT number for CTIS/EUCTR-native rows).",
        "source": "Source registry: clinicaltrials.gov, ctis, euctr, or merged sources.",
        "secondary_id": "Secondary identifiers (comma-separated) when available.",
        "trial_link": "Source trial URL(s), separated by ' | ' when merged.",
        "title": "Trial title.",
        "study_type": "Study type (e.g., INTERVENTIONAL, OBSERVATIONAL).",
        "study_design": "Normalized study design label.",
        "phase": "Trial phase string.",
        "status": "Overall/recruitment status.",
        "sponsor": "Lead sponsor.",
        "admission_date": "First registration/posting date (YYYY-MM-DD) when available.",
        "last_update_date": "Last update date from source (YYYY-MM-DD) when available.",
        "primary_completion_date": "Primary completion date (YYYY-MM-DD) when available.",
        "has_results": "Best-effort result/publication flag (yes/no/NA).",
        "results_last_update": "Source result/update date (YYYY-MM-DD) when available.",
        "pubmed_links": "Pipe-separated PubMed links.",
        "publication_date": "Earliest linked publication date (YYYY-MM-DD) when available.",
        "publication_scan_date": "Last date publication linker scanned this trial (YYYY-MM-DD).",
        "publication_lag_days": "Publication date minus primary completion date, non-negative.",
        "evidence_strength": "Heuristic evidence level: high/medium/low/very_low/unknown.",
        "dead_end": "yes when phase>=2, terminal status, no publication, completion older than 5 years.",
        "publication_count": "Count of full-match publication records linked to this trial.",
        "publication_match_methods": "Comma-separated methods for full publication matches.",
        "conditions": "Trial conditions text.",
        "interventions": "Interventions text (type/name).",
        "intervention_types": "Comma-separated intervention type list.",
        "primary_outcomes": "Primary outcomes text.",
        "secondary_outcomes": "Secondary outcomes text.",
        "inclusion_criteria": "Inclusion criteria text.",
        "exclusion_criteria": "Exclusion criteria text.",
        "locations": "Locations/sites text.",
        "brief_summary": "Brief summary text.",
        "detailed_description": "Detailed description text.",
        "therapeutic_class": "Normalized therapeutic class.",
        "focus_tags": "Comma-separated focus tags.",
        "pdac_match_reason": "Reason why trial matched PDAC cohort.",
        "trial_uid": "Deduplicated unique trial identifier (NCT/EUCT or stable hash).",
        "source_count": "Number of distinct sources merged for this trial.",
        "sources_list": "Comma-separated sources contributing to the merged record.",
        "has_publication": "1/0 indicator derived from full-match publications.",
        "publication_year_first": "Year of earliest linked publication.",
        "journal_impact_flag": "1/0 if any linked publication is in a high-impact journal list.",
        "trial_outcome_label": "Outcome label: success/completed_no_publication/failure/ongoing/unknown.",
        "binary_success_label": "Binary outcome label: 1=success, 0=failure, NA otherwise.",
        "start_year": "Year derived from admission_date.",
        "completion_year": "Year derived from primary_completion_date.",
        "duration_months": "Months between admission_date and primary_completion_date (non-negative).",
        "publication_delay_months": "Months between primary_completion_date and publication_date (non-negative).",
        "is_post_2015": "1/0 if start_year >= 2015.",
        "years_since_start": "Years from start_year to dataset generation year.",
        "phase_numeric": "Numeric phase (e.g., 1.0, 2.0, 1.5 for Phase I/II).",
        "is_phase_1_2_combined": "1/0 if Phase I/II combined.",
        "num_arms": "Parsed number of study arms when available.",
        "is_randomized": "1/0 if study design indicates randomization.",
        "is_multi_center": "1/0 if study design indicates multi-center.",
        "country_count": "Unique country count derived from locations.",
        "is_multi_country": "1/0 if country_count > 1.",
        "intervention_type": "Primary intervention type (first in intervention_types).",
        "is_combination_therapy": "1/0 if multiple intervention types or combination phrasing.",
        "sponsor_normalized": "Normalized sponsor label for aggregation.",
        "sponsor_type": "Sponsor class: big_pharma/biotech/academic/unknown.",
        "sponsor_trial_count_total": "Prior trials for sponsor (start_year < current).",
        "sponsor_trial_count_last_5y": "Prior trials for sponsor in last 5 years.",
        "sponsor_success_rate_historical": "Historical sponsor success rate (success vs failure).",
        "is_top_10_sponsor": "1/0 if sponsor is in top 10 by prior count.",
        "target_primary": "Primary target/agent derived from interventions/tags/class.",
        "target_category": "Target category (therapeutic_class or intervention type).",
        "target_trial_count_total": "Prior trials for target (start_year < current).",
        "target_trial_count_last_5y": "Prior trials for target in last 5 years.",
        "target_success_rate_historical": "Historical target success rate (success vs failure).",
        "is_novel_target": "1/0 if no prior trials for target.",
        "target_literature_count_last_5y": "Prior linked publications in last 5 years for target.",
        "literature_trial_ratio": "Literature/trial ratio for last 5 years.",
        "is_literature_rich_trial_sparse": "1/0 if literature is high and trials are sparse.",
        "llm_context_block": "Plain-text context block for LLM use.",
        "feature_temporal_scope": "JSON map of engineered feature -> temporal scope (pre_start/static/post_outcome).",
    }

    return {
        "name": "PDAC Trial Atlas dataset",
        "version": "1.6",
        "generated_at_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ"),
        "files": {
            "pdac-trials.csv": {
                "path": "pdac-trials.csv",
                "format": "csv",
                "encoding": "UTF-8",
                "header": True,
                "delimiter": ",",
                "rows": int(len(df)),
            },
            "pdac-trials.parquet": {
                "path": "pdac-trials.parquet",
                "format": "parquet",
                "rows": int(len(df)),
            },
            "pdac_yearly_metrics.csv": {
                "path": "pdac_yearly_metrics.csv",
                "format": "csv",
                "rows": int(len(metrics_df)),
            },
            "pdac_trials_modeling_view.csv": {
                "path": "pdac_trials_modeling_view.csv",
                "format": "csv",
                "rows": int(len(model_df)),
            },
        },
        "columns": [
            {
                "name": col,
                "type": (
                    "boolean"
                    if col in BOOL_FIELDS
                    else "number"
                    if col in NUMERIC_FIELDS
                    else "string"
                ),
                "description": known_descriptions.get(col, f"{col} field"),
            }
            for col in df.columns
        ],
    }


def main() -> None:
    DATASET_DIR.mkdir(exist_ok=True)

    df = load_ml_ready()
    df = normalize_types(df)
    df = add_feature_temporal_scope(df)

    # Build modeling view from normalized data
    model_df = df.copy()
    missing = [col for col in MODEL_VIEW_COLUMNS if col not in model_df.columns]
    if missing:
        raise ValueError(f"Missing columns for modeling view: {missing}")
    model_df = model_df[MODEL_VIEW_COLUMNS]

    # CSV/Parquet output with consistent dtypes
    csv_df = fill_na_for_csv(df)
    csv_path = DATASET_DIR / "pdac-trials.csv"
    parquet_path = DATASET_DIR / "pdac-trials.parquet"
    csv_df.to_csv(csv_path, index=False, encoding="utf-8")
    df.to_parquet(parquet_path, index=False)

    # Modeling view (CSV only)
    model_csv = fill_na_for_csv(model_df)
    model_path = DATASET_DIR / "pdac_trials_modeling_view.csv"
    model_csv.to_csv(model_path, index=False, encoding="utf-8")

    # Yearly metrics
    metrics_df = build_yearly_metrics(df)
    metrics_path = DATASET_DIR / "pdac_yearly_metrics.csv"
    metrics_df.to_csv(metrics_path, index=False, encoding="utf-8")

    # Schema
    schema = build_schema(df, metrics_df, model_df)
    schema_path = DATASET_DIR / "schema.json"
    schema_path.write_text(json.dumps(schema, indent=2, ensure_ascii=False), encoding="utf-8")

    # Checksums
    sha_path = DATASET_DIR / "SHA256SUMS.txt"
    import subprocess

    subprocess.run(
        [
            "shasum",
            "-a",
            "256",
            str(csv_path),
            str(parquet_path),
            str(schema_path),
            str(metrics_path),
            str(model_path),
        ],
        check=True,
        stdout=sha_path.open("w", encoding="utf-8"),
    )


if __name__ == "__main__":
    main()
