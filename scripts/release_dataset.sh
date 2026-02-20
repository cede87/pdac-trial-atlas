#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

PYTHON_BIN="${PYTHON_BIN:-$ROOT_DIR/.venv/bin/python}"
if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="python3"
fi

DATASET_VERSION="${DATASET_VERSION:-1.4}"
RUN_FULL_INDEX="${RUN_FULL_INDEX:-1}"
CLEAR_PUBMED_CACHE="${CLEAR_PUBMED_CACHE:-0}"
RUN_TESTS="${RUN_TESTS:-1}"
RUN_QA="${RUN_QA:-1}"
RUN_EXPORT="${RUN_EXPORT:-1}"

PUBMED_NCT_LOOKUP_LIMIT="${PUBMED_NCT_LOOKUP_LIMIT:-2000}"
PUBMED_TITLE_LOOKUP_LIMIT="${PUBMED_TITLE_LOOKUP_LIMIT:-2000}"
PUBMED_DOI_LOOKUP_LIMIT="${PUBMED_DOI_LOOKUP_LIMIT:-1000}"
PUBMED_PER_TRIAL_LINK_LIMIT="${PUBMED_PER_TRIAL_LINK_LIMIT:-5}"
PUBMED_FULL_MATCH_MIN_CONFIDENCE="${PUBMED_FULL_MATCH_MIN_CONFIDENCE:-80}"
PUBMED_PUBLICATION_MODE="${PUBMED_PUBLICATION_MODE:-full}"
PUBMED_REFRESH_DAYS="${PUBMED_REFRESH_DAYS:-120}"
PUBMED_RETRY_DAYS_NO_MATCH="${PUBMED_RETRY_DAYS_NO_MATCH:-30}"
PUBMED_DATE_LOOKUP_LIMIT="${PUBMED_DATE_LOOKUP_LIMIT:-1200}"
PUBMED_MESH_LOOKUP_LIMIT="${PUBMED_MESH_LOOKUP_LIMIT:-1200}"

echo "==> Release dataset workflow (version: ${DATASET_VERSION})"

if [[ "$CLEAR_PUBMED_CACHE" == "1" ]]; then
  echo "==> Clearing PubMed caches"
  sqlite3 pdac_trials.db "DELETE FROM pubmed_search_cache; DELETE FROM pubmed_summary_cache;" || true
fi

if [[ "$RUN_FULL_INDEX" == "1" ]]; then
  echo "==> Running ingestion (mode=${PUBMED_PUBLICATION_MODE})"
  PYTHONPATH=. \
  PUBMED_NCT_LOOKUP_LIMIT="$PUBMED_NCT_LOOKUP_LIMIT" \
  PUBMED_TITLE_LOOKUP_LIMIT="$PUBMED_TITLE_LOOKUP_LIMIT" \
  PUBMED_DOI_LOOKUP_LIMIT="$PUBMED_DOI_LOOKUP_LIMIT" \
  PUBMED_PER_TRIAL_LINK_LIMIT="$PUBMED_PER_TRIAL_LINK_LIMIT" \
  PUBMED_FULL_MATCH_MIN_CONFIDENCE="$PUBMED_FULL_MATCH_MIN_CONFIDENCE" \
  PUBMED_PUBLICATION_MODE="$PUBMED_PUBLICATION_MODE" \
  PUBMED_REFRESH_DAYS="$PUBMED_REFRESH_DAYS" \
  PUBMED_RETRY_DAYS_NO_MATCH="$PUBMED_RETRY_DAYS_NO_MATCH" \
  PUBMED_DATE_LOOKUP_LIMIT="$PUBMED_DATE_LOOKUP_LIMIT" \
  PUBMED_MESH_LOOKUP_LIMIT="$PUBMED_MESH_LOOKUP_LIMIT" \
  "$PYTHON_BIN" scripts/ingest_clinicaltrials.py
fi

if [[ "$RUN_TESTS" == "1" ]]; then
  echo "==> Running test suite"
  PYTHONPATH=. "$PYTHON_BIN" -m pytest -q
fi

if [[ "$RUN_QA" == "1" ]]; then
  echo "==> Running strict QA"
  PYTHONPATH=. "$PYTHON_BIN" scripts/qa_report.py --strict --limit 20
fi

if [[ "$RUN_EXPORT" == "1" ]]; then
  echo "==> Exporting CSV from DB"
  PYTHONPATH=. "$PYTHON_BIN" scripts/export_to_csv.py
fi

echo "==> Building dataset artifacts"
mkdir -p dataset
cp pdac_trials_export.csv dataset/pdac-trials.csv

PYTHONPATH=. DATASET_VERSION="$DATASET_VERSION" "$PYTHON_BIN" - <<'PY'
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

dataset_dir = Path("dataset")
csv_path = dataset_dir / "pdac-trials.csv"
parquet_path = dataset_dir / "pdac-trials.parquet"
schema_path = dataset_dir / "schema.json"
version = os.getenv("DATASET_VERSION", "1.4")

df = pd.read_csv(csv_path, dtype=str).fillna("NA")
df.to_parquet(parquet_path, index=False)

known_descriptions = {
    "nct_id": "Primary trial identifier (NCT ID for ClinicalTrials.gov rows, EU CT number for CTIS-native rows).",
    "source": "Source registry: clinicaltrials.gov, ctis, or clinicaltrials.gov+ctis.",
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
}

schema = {
    "name": "PDAC Trial Atlas dataset",
    "version": version,
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
    },
    "columns": [
        {
            "name": col,
            "type": "string",
            "description": known_descriptions.get(col, f"{col} field"),
        }
        for col in df.columns
    ],
}

schema_path.write_text(json.dumps(schema, indent=2, ensure_ascii=False), encoding="utf-8")
PY

echo "==> Writing checksums"
shasum -a 256 \
  dataset/pdac-trials.csv \
  dataset/pdac-trials.parquet \
  dataset/schema.json > dataset/SHA256SUMS.txt

echo "==> Done"
echo "Artifacts:"
echo "  - dataset/pdac-trials.csv"
echo "  - dataset/pdac-trials.parquet"
echo "  - dataset/schema.json"
echo "  - dataset/SHA256SUMS.txt"
