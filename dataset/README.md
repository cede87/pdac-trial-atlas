# PDAC Trial Atlas dataset (v1.4)

This dataset is a curated, PDAC-focused view of clinical trials enriched with normalization, tagging,
and cross-registry correlation to support exploration and lightweight analysis.

## Purpose
- Provide a single, local-friendly table of PDAC-relevant clinical trials.
- Add normalized fields (therapeutic class, focus tags, intervention types, design) to speed up filtering and exploration.
- Merge correlated records across registries when the same trial appears in both ecosystems.

## Data sources
- ClinicalTrials.gov (USA/international registry) public portal via its public API endpoints.
- CTIS (EU Clinical Trials Information System) via programmatic retrieval.

Notes:
- CTIS coverage is primarily for EU/EEA trials submitted under EU CTR (public CTIS portal states coverage from 31 Jan 2022 onward).
- Historical EU trials in the legacy EudraCT register are not included in this export.

## Number of trials
- Total rows: 2486

- By source:
- clinicaltrials.gov: 2332
- ctis: 119
- clinicaltrials.gov+ctis: 35

## Files
- `pdac-trials.csv` — UTF-8 CSV with a stable header.
- `pdac-trials.parquet` — Parquet version of the same table (useful for analytics).
- `schema.json` — machine-readable schema for `pdac-trials.csv`.

## Field descriptions
| Column | Type | Description |
|---|---|---|
| nct_id | string | Primary trial identifier (NCT ID for ClinicalTrials.gov rows, EU CT number for CTIS-native rows). |
| source | string | Source registry: clinicaltrials.gov, ctis, or clinicaltrials.gov+ctis. |
| secondary_id | string | Secondary identifiers (comma-separated) when available. |
| trial_link | string | Source trial URL(s), separated by ' | ' when merged. |
| title | string | Trial title. |
| study_type | string | Study type (e.g., INTERVENTIONAL, OBSERVATIONAL). |
| study_design | string | Normalized study design label. |
| phase | string | Trial phase string. |
| status | string | Overall/recruitment status. |
| sponsor | string | Lead sponsor. |
| admission_date | string | First registration/posting date (YYYY-MM-DD) when available. |
| last_update_date | string | Last update date from source (YYYY-MM-DD) when available. |
| primary_completion_date | string | Primary completion date (YYYY-MM-DD) when available. |
| has_results | string | Best-effort result/publication flag (yes/no/NA). |
| results_last_update | string | Source result/update date (YYYY-MM-DD) when available. |
| pubmed_links | string | Pipe-separated PubMed links. |
| publication_date | string | Earliest linked publication date (YYYY-MM-DD) when available. |
| publication_scan_date | string | Last date publication linker scanned this trial (YYYY-MM-DD). |
| publication_lag_days | string | Publication date minus primary completion date, non-negative. |
| evidence_strength | string | Heuristic evidence level: high/medium/low/very_low/unknown. |
| dead_end | string | yes when phase>=2, terminal status, no publication, completion older than 5 years. |
| publication_count | string | Count of full-match publication records linked to this trial. |
| publication_match_methods | string | Comma-separated methods for full publication matches. |
| conditions | string | Trial conditions text. |
| interventions | string | Interventions text (type/name). |
| intervention_types | string | Comma-separated intervention type list. |
| primary_outcomes | string | Primary outcomes text. |
| secondary_outcomes | string | Secondary outcomes text. |
| inclusion_criteria | string | Inclusion criteria text. |
| exclusion_criteria | string | Exclusion criteria text. |
| locations | string | Locations/sites text. |
| brief_summary | string | Brief summary text. |
| detailed_description | string | Detailed description text. |
| therapeutic_class | string | Normalized therapeutic class. |
| focus_tags | string | Comma-separated focus tags. |
| pdac_match_reason | string | Reason why trial matched PDAC cohort. |

## Limitations
- This is not an authoritative or complete registry mirror; it is a PDAC-focused slice.
- PDAC inclusion is based on heuristic string matching and normalization rules; false positives/negatives are possible.
- Missing values are stored as `NA`.
- Publications/`has_results` are best-effort and may lag or miss papers not linked to an NCT identifier.
- CTIS records may not always correlate to an NCT ID; merging only occurs when an explicit link is present.

## How the data was generated
1. Run the ingestion pipeline to build/update the local SQLite database (`pdac_trials.db`).
2. Retrieve PDAC-focused studies from ClinicalTrials.gov and CTIS.
3. Apply de-duplication/merge when CTIS records reference an NCT identifier.
4. Apply normalization and tagging.
5. Export final dataset into CSV and Parquet with schema metadata.
6. Generation timestamp (UTC): 2026-02-20 11:22:48Z

## License
This dataset is released under CC BY 4.0. See `LICENSE-CC-BY-4.0.txt`.
