# PDAC Trial Atlas dataset (v1.6)

This dataset is a curated, PDAC-focused view of clinical trials enriched with normalization, tagging,
cross-registry correlation, and ML-ready features for modeling and trend analysis.

## Purpose
- Provide a single, local-friendly table of PDAC-relevant clinical trials.
- Add normalized fields (therapeutic class, focus tags, intervention types, design) to speed up filtering and exploration.
- Merge correlated records across registries when the same trial appears in both ecosystems.
- Provide a deduplicated ML-ready view for predictive modeling and research-gap analysis.

## Data sources
- ClinicalTrials.gov (USA/international registry) public portal via its public API endpoints.
- CTIS (EU Clinical Trials Information System) via programmatic retrieval.
- EUCTR (legacy EU Clinical Trials Register) via programmatic retrieval.

Notes:
- CTIS coverage is primarily for EU/EEA trials submitted under EU CTR (public CTIS portal states coverage from 31 Jan 2022 onward).
- EUCTR is the legacy EU register for historical trials (pre-CTR coverage).

## Number of trials
- Dataset table (`pdac-trials.csv`, ML-ready): **2,859** rows
  - clinicaltrials.gov: 2,286
  - euctr: 411
  - ctis: 109
  - clinicaltrials.gov+ctis: 38
  - clinicaltrials.gov+euctr: 9
  - ctis+euctr: 5
  - clinicaltrials.gov+ctis+euctr: 1

## Files
- `pdac-trials.csv` — UTF-8 CSV (ML-ready, deduplicated).
- `pdac-trials.parquet` — Parquet version of the same table (useful for analytics).
- `pdac_trials_modeling_view.csv` — modeling-safe feature subset (pre-start features only).
- `pdac_trials_modeling_view_clean.csv` — filtered modeling view with label-complete rows only.
- `schema.json` — machine-readable schema for `pdac-trials.csv`.
- `pdac_yearly_metrics.csv` — yearly aggregates for trend analysis.

## Field descriptions (ML-ready table)
| Column | Type | Description |
|---|---|---|
| nct_id | string | Primary trial identifier (NCT ID for ClinicalTrials.gov rows, EU CT number for CTIS/EUCTR-native rows). |
| source | string | Source registry: clinicaltrials.gov, ctis, euctr, or merged sources. |
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
| feature_temporal_scope | string | JSON map of engineered feature → temporal scope (pre_start/static/post_outcome). |

## ML-ready additional fields
| Column | Type | Description |
|---|---|---|
| trial_uid | string | Deduplicated unique trial identifier (NCT/EUCT or stable hash). |
| source_count | integer | Number of distinct sources merged for this trial. |
| sources_list | string | Comma-separated sources contributing to the merged record. |
| has_publication | string | yes/no based on full-match publications. |
| publication_year_first | integer | Year of the earliest linked publication. |
| journal_impact_flag | string | yes if any linked publication is in a high-impact journal list. |
| trial_outcome_label | string | success / completed_no_publication / failure / ongoing / unknown. |
| binary_success_label | integer | 1 = success, 0 = failure, NA otherwise. |
| start_year | integer | Year derived from admission_date. |
| completion_year | integer | Year derived from primary_completion_date. |
| duration_months | number | Months between admission_date and primary_completion_date (non-negative). |
| publication_delay_months | number | Months between primary_completion_date and publication_date (non-negative). |
| is_post_2015 | string | yes if start_year >= 2015. |
| years_since_start | integer | Years from start_year to dataset generation year. |
| phase_numeric | number | Numeric phase (e.g., 1.0, 2.0, 1.5 for Phase I/II). |
| is_phase_1_2_combined | string | yes if Phase I/II combined. |
| num_arms | integer | Parsed number of arms when available. |
| is_randomized | string | yes if study design indicates randomization. |
| is_multi_center | string | yes if study design indicates multi-center. |
| country_count | integer | Unique country count derived from locations. |
| is_multi_country | string | yes if country_count > 1. |
| intervention_type | string | Primary intervention type (first in intervention_types). |
| is_combination_therapy | string | yes if multiple intervention types or combination phrasing. |
| sponsor_normalized | string | Normalized sponsor label for aggregation. |
| sponsor_type | string | big_pharma / biotech / academic / unknown. |
| sponsor_trial_count_total | integer | Prior trials for sponsor (start_year < current). |
| sponsor_trial_count_last_5y | integer | Prior trials for sponsor in last 5 years. |
| sponsor_success_rate_historical | number | Prior sponsor success rate (success vs failure). |
| is_top_10_sponsor | string | yes if sponsor is in top 10 by prior count. |
| target_primary | string | Primary target/agent derived from interventions/tags/class. |
| target_category | string | Target category derived from intervention type (when available). |
| target_trial_count_total | integer | Prior trials for target (start_year < current). |
| target_trial_count_last_5y | integer | Prior trials for target in last 5 years. |
| target_success_rate_historical | number | Prior target success rate (success vs failure). |
| is_novel_target | string | yes if no prior trials for target. |
| target_literature_count_last_5y | integer | Prior linked publications in last 5 years for target. |
| literature_trial_ratio | number | Literature/trial ratio for last 5 years. |
| is_literature_rich_trial_sparse | string | yes if literature is high and trials are sparse. |
| llm_context_block | string | Plain-text block for LLM context and summarization. |

## Modeling-safe view (`pdac_trials_modeling_view.csv`)
This file includes **only pre-start features** and the outcome label needed for supervised learning.
It excludes post-outcome and leakage-prone fields (publications, lag, evidence strength, etc.).

## Data type normalization
To make modeling safer and consistent across CSV/Parquet:
- Boolean fields are encoded as `1`/`0` (or `NA`).
- Numeric fields are exported as numbers in Parquet and numeric strings in CSV.

## Feature temporal scope
`feature_temporal_scope` is a JSON map labeling engineered fields as:
- `pre_start`: safe to use for prediction at trial start.
- `post_outcome`: derived from or after outcomes (avoid for prediction).
- `static`: identifiers/metadata.

## Yearly metrics schema (`pdac_yearly_metrics.csv`)
| Column | Type | Description |
|---|---|---|
| year | integer | Start year. |
| trials_started | integer | Trials started in that year. |
| trials_completed | integer | Trials with completed status (started that year). |
| trials_terminated | integer | Trials terminated/withdrawn/suspended (started that year). |
| success_rate | number | Success/(success+failure) among trials started that year. |
| avg_duration_by_phase | string | JSON map of phase → average duration months. |
| top_target_by_count | string | Most common target in that year. |
| new_targets_introduced | integer | Count of targets first appearing that year. |

## Limitations
- This is not an authoritative or complete registry mirror; it is a PDAC-focused slice.
- PDAC inclusion is based on heuristic string matching and normalization rules; false positives/negatives are possible.
- Missing values are stored as `NA`.
- Publications/`has_results` are best-effort and may lag or miss papers not linked to an NCT identifier.
- Merging across registries is conservative and depends on identifier overlap and title normalization.

## How the data was generated
1. Run the ingestion pipeline to build/update the local SQLite database (`pdac_trials.db`).
2. Retrieve PDAC-focused studies from ClinicalTrials.gov, CTIS, and EUCTR.
3. Apply de-duplication/merge when CTIS/EUCTR records reference an NCT identifier.
4. Apply normalization and tagging.
5. Generate ML-ready dataset (`pdac-trials.csv`) with deduplication and engineered features.
6. Export CSV/Parquet with schema metadata.
7. Generate yearly metrics.
8. Generation timestamp (UTC): 2026-03-03 09:54:43Z

## License
This dataset is released under CC BY 4.0. See `LICENSE-CC-BY-4.0.txt`.
