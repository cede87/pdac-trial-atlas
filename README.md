# pdac-trial-atlas
An open, evidence-graded atlas of pancreatic ductal adenocarcinoma (PDAC) clinical trials. Focused on normalizing trial metadata, evidence gaps, and strategy-level patterns.

## Version

Current release: **v1.4**

## Disclaimer

This project is an independent, good-faith learning initiative intended to support exploration of publicly available PDAC clinical trial data.

It is **not** a medical product, clinical decision system, regulatory tool, or source of medical advice. It does not guarantee completeness, correctness, or fitness for clinical use. Any interpretation or downstream use of the data is the sole responsibility of the user.

No claims are made regarding outcomes, efficacy, safety, or research conclusions. The purpose of this work is to learn in public and contribute constructively, with humility, transparency, and a genuine intention to help the research community.

## Local dashboard

1. Install deps:
   `pip install -r requirements.txt`
2. Refresh data (optional but recommended):
   `PYTHONPATH=. python3 scripts/ingest_clinicaltrials.py`
3. Launch dashboard:
   `streamlit run frontend/dashboard.py`

The dashboard runs 100% local and reads from `pdac_trials.db`.
Ingestion now merges:
- ClinicalTrials.gov PDAC trials
- CTIS (EU Clinical Trials Information System) PDAC trials

During ingestion, PubMed links are enriched in two stages:
- direct NCT enrichment (`pubmed_links`)
- normalized publication index (`trial_publications`) with method + confidence

Legacy PubMed enrichment control:
- `PUBMED_LOOKUP_LIMIT=1000` increase direct NCT PubMed enrichment scope

Publication-index controls (v1.4):
- `PUBMED_NCT_LOOKUP_LIMIT=400` max NCT exact lookups into PubMed
- `PUBMED_TITLE_LOOKUP_LIMIT=300` max title-fallback lookups
- `PUBMED_DOI_LOOKUP_LIMIT=200` max DOI lookups
- `PUBMED_PER_TRIAL_LINK_LIMIT=5` max stored publication links per trial
- `PUBMED_FULL_MATCH_MIN_CONFIDENCE=80` minimum confidence for fuzzy/title matches to be treated as full matches
- `PUBMED_TITLE_YEAR_LOOKBACK=1` years before completion/admission date for title-based searches
- `PUBMED_TITLE_YEAR_LOOKAHEAD=12` years after completion/admission date for title-based searches
- `PUBMED_TITLE_KEYWORD_LIMIT=3` max keywords derived from conditions/interventions for title fallback
- `PUBMED_TITLE_KEYWORD_MIN_LEN=4` minimum keyword length for title fallback filters
- `PUBMED_PUBLICATION_MODE=incremental` (`incremental` or `full`, default: `incremental`)
- `PUBMED_REFRESH_DAYS=120` in incremental mode, only refresh trials with source updates in this window when they already have full matches
- `PUBMED_RETRY_DAYS_NO_MATCH=30` in incremental mode, retry scan interval for trials without full publication matches

Publication-index execution is prioritized toward higher-impact rows first (`phase >=2`, terminal status, no publication signal, older completion date) so limited lookup budgets are spent where evidence impact is highest.

### Incremental publication re-evaluation logic

In `PUBMED_PUBLICATION_MODE=incremental` the linker decides per trial whether to scan again:

1. If the trial has at least one full publication match:
   - scan again only when `last_update_date` is within `PUBMED_REFRESH_DAYS`.
2. If the trial has no full publication match:
   - scan when `last_update_date` is recent (`PUBMED_REFRESH_DAYS`), or
   - scan when `publication_scan_date` is older than `PUBMED_RETRY_DAYS_NO_MATCH`, or
   - scan when it has never been scanned (`publication_scan_date` is `NA`).
3. Otherwise the trial is skipped for that run.

This keeps previously discovered links and avoids repeating the same PubMed work every run.

Signal-enrichment controls (optional):
- `PUBMED_DATE_LOOKUP_LIMIT=500` PubMed publication-date backfill lookups per run
- `PUBMED_MESH_LOOKUP_LIMIT=500` PubMed MeSH-based therapeutic-class ensemble lookups per run

Recommended run modes:
- Fast incremental refresh (daily/regular):
  - `PYTHONPATH=. PUBMED_PUBLICATION_MODE=incremental python3 scripts/ingest_clinicaltrials.py`
- Full publication re-index (occasional deep refresh):
  - `PYTHONPATH=. PUBMED_PUBLICATION_MODE=full python3 scripts/ingest_clinicaltrials.py`

One-command dataset release build (CSV + Parquet + schema + checksums + zip):
- `./scripts/release_dataset.sh`
- Optional controls:
  - `RUN_FULL_INDEX=1` (default) or `0`
  - `RUN_TESTS=1` (default) or `0`
  - `RUN_QA=1` (default) or `0`
  - `DATASET_VERSION=1.4`
- Note: generated dataset artifacts (`dataset/README.md`, `dataset/schema.json`, CSV/Parquet, checksums, zip) are git-ignored and produced per release run.

CTIS controls (optional):
- `INGEST_CTIS=0` skip CTIS for a run
- `CTIS_QUERY_TERMS=pancreatic,pancreas,pdac,pancreatic cancer` to set custom CTIS search terms
- `CTIS_MEDICAL_CONDITION=pancreatic` to force a single CTIS medical condition term
- `CTIS_MAX_OVERVIEW=200` limit scanned overview rows
- `CTIS_MAX_TRIALS=100` limit normalized CTIS trials kept

EUCTR (legacy EU register) controls (optional):
- `INGEST_EUCTR=0` skip EUCTR ingestion for a run
- `EUCTR_QUERY_TERMS=pancreatic,pancreas,pdac,pancreatic cancer` to set custom EUCTR search terms
- `EUCTR_MAX_PAGES=50` limit fetched EUCTR result pages per term
- `EUCTR_MAX_TRIALS=1000` limit normalized EUCTR trials kept
- `EUCTR_PAGE_SLEEP=0.25` sleep (seconds) between EUCTR pages to avoid throttling

### Identifier model (important)

- `Trial ID` (table column `nct_id`) is always the **primary key row id**:
  - `NCT...` for native ClinicalTrials.gov rows
  - `YYYY-XXXXXX-XX-XX` for CTIS-native rows
- `NCT ID` is displayed in Explorer as a separate convenience column:
  - `NCT...` when available
  - `NA` when not available
- `secondary_id` is still stored internally for lineage/correlation, but is no longer shown in the main table.

### Cross-source de-duplication (CTIS ↔ ClinicalTrials.gov)

During ingestion, if a CTIS trial has `secondary_id = NCT...` and that NCT exists in the dataset:

1. CTIS row is merged into the NCT row.
2. Source becomes `clinicaltrials.gov+ctis`.
3. CTIS identifier is kept as alternate id in `secondary_id`.
4. Source links are merged in `trial_link` (`ctgov | ctis`).
5. CTIS duplicate row is removed.

This prevents duplicate entries for the same trial across registries.

## Deploy to Streamlit Community Cloud

This repo is now ready for Streamlit deployment:

- App entrypoint: `streamlit_app.py`
- Streamlit config: `.streamlit/config.toml`
- Python runtime: `runtime.txt` (Python 3.11)

Publish steps:

1. Push this repo to GitHub.
2. In Streamlit Community Cloud, create a new app from the repo.
3. Set main file path to: `streamlit_app.py`
4. Deploy.

On first run, click **Initialize dataset** in the app to fetch data from ClinicalTrials.gov and build `pdac_trials.db`.

## Testing

Run unit/regression tests:

`PYTHONPATH=. python3 -m pytest -q`

Run dataset QA (cross-field integrity + tag/match consistency):

`PYTHONPATH=. python3 scripts/qa_report.py --strict --limit 20`

Recommended full local validation flow:

1. `PYTHONPATH=. python3 scripts/ingest_clinicaltrials.py`
2. `PYTHONPATH=. python3 -m pytest -q`
3. `PYTHONPATH=. python3 scripts/qa_report.py --strict --limit 20`
4. `PYTHONPATH=. python3 scripts/export_to_csv.py`

Deep validation flow (source integrity + overlap + signal checks):

1. `PYTHONPATH=. INGEST_CTIS=1 python3 scripts/ingest_clinicaltrials.py`
2. `PYTHONPATH=. python3 -m pytest -q`
3. `PYTHONPATH=. python3 scripts/qa_report.py --strict --limit 20`
4. Validate no remaining CTIS↔NCT duplicates:

```sql
SELECT COUNT(*)
FROM clinical_trials ctis
JOIN clinical_trials us ON us.nct_id = ctis.secondary_id
WHERE ctis.source = 'ctis' AND ctis.secondary_id LIKE 'NCT%';
```

Expected result: `0`.

5. Validate signal heuristics:

```sql
-- Dead-end rows must be >= phase 2, completed/terminated, no PubMed, older than 5 years.
SELECT COUNT(*) AS dead_end_mismatch
FROM clinical_trials
WHERE LOWER(COALESCE(dead_end, '')) = 'yes'
  AND NOT (
    (LOWER(COALESCE(phase, '')) LIKE '%phase ii%' OR LOWER(COALESCE(phase, '')) LIKE '%phase2%'
      OR LOWER(COALESCE(phase, '')) LIKE '%phase 2%' OR LOWER(COALESCE(phase, '')) LIKE '%phase iii%'
      OR LOWER(COALESCE(phase, '')) LIKE '%phase3%' OR LOWER(COALESCE(phase, '')) LIKE '%phase 3%'
      OR LOWER(COALESCE(phase, '')) LIKE '%phase iv%' OR LOWER(COALESCE(phase, '')) LIKE '%phase4%'
      OR LOWER(COALESCE(phase, '')) LIKE '%phase 4%')
    AND (LOWER(COALESCE(status, '')) LIKE '%completed%' OR LOWER(COALESCE(status, '')) LIKE '%terminated%')
    AND (COALESCE(TRIM(pubmed_links), '') = '' OR UPPER(TRIM(pubmed_links)) = 'NA')
    AND DATE(primary_completion_date) <= DATE('now', '-5 years')
  );
```

Expected result: `0`.

6. Validate publication-index integrity:

```sql
SELECT COUNT(*) AS publication_orphans
FROM trial_publications p
LEFT JOIN clinical_trials c ON c.nct_id = p.nct_id
WHERE c.nct_id IS NULL;
```

Expected result: `0`.

7. Validate lag storage policy:

```sql
-- Negative lag is treated as anomaly and not stored in publication_lag_days.
SELECT COUNT(*) AS negative_lag_stored
FROM clinical_trials
WHERE publication_lag_days < 0;
```

Expected result: `0`.

## Signal extraction (v1.4)

The dataset now computes four signal fields focused on trial evidence quality:

- `evidence_strength`
- `publication_date`
- `publication_lag_days`
- `dead_end`

Heuristics used:

- `high`: phase 3 + linked PubMed paper
- `medium`: phase 2 + linked PubMed paper
- `low`: phase 1 only
- `very_low`: completed/terminated + no linked publication + primary completion older than 5 years
- otherwise: `unknown`

Dead-end rule:

- `dead_end = yes` when:
  - phase >= 2
  - status is completed/terminated
  - no linked publication
  - primary completion is older than 5 years

Publication lag rule:

- `publication_lag_days = publication_date - primary_completion_date`
- Negative lag values are treated as data anomalies (not stored as lag values) and surfaced in the Analytics data-quality cards.

Publication-link confidence rule:

- Exact link methods (`pubmed_link`, `nct_exact`, `secondary_nct_exact`, `doi_reference`) are treated as full matches.
- Fuzzy title matches (`title_fuzzy`) are treated as full matches only when confidence is `>= PUBMED_FULL_MATCH_MIN_CONFIDENCE` (default: `80`).
- Non-full fuzzy matches are kept in `trial_publications` for traceability, but are not propagated to trial-level `pubmed_links`, `publication_date`, `has_results`, or signal fields.

## Storage layout

- `clinical_trials` keeps compact fields for fast filtering/sorting (id, status, dates, class, tags, etc.).
- `clinical_trial_details` stores long-text fields (conditions, interventions, outcomes, eligibility, locations, summaries/descriptions).
- `trial_publications` stores normalized publication rows (`pmid`, `doi`, `publication_date`, `match_method`, `confidence`, `is_full_match`) and is used to compute publication coverage analytics.
- `pubmed_search_cache` and `pubmed_summary_cache` persist PubMed query/results cache so future ingestion runs reuse prior lookups instead of repeating network calls.
- Both tables are linked 1:1 via `nct_id`.
- In the dashboard Quick filters bar, `Origin` lets you filter by source (`clinicaltrials.gov`, `ctis`, or merged `clinicaltrials.gov+ctis`).
- In Explorer, `Trial ID` is the primary row ID and `NCT ID` is shown explicitly as a separate column.
- CTIS rows that map to an existing NCT (via CTIS secondary NCT ID) are automatically merged to avoid duplicates.
- In Explorer, table text selection/copy with mouse is enabled (AgGrid text selection).

## Data dictionary (current columns)

Below is what each field stores, expected values/patterns, and one quick example.

| Column | What it is | Possible values / format |
|---|---|---|
| `nct_id` | Primary trial key in this dataset | `NCT...` for ClinicalTrials.gov or `YYYY-NNNNNN-NN-NN` for CTIS |
| `source` | Source registry for the row | `clinicaltrials.gov`, `ctis`, `clinicaltrials.gov+ctis` |
| `secondary_id` | Optional alternate registry ID(s) | May include `NCT...` and/or EU CTIS codes |
| `trial_link` | Direct URL to the source trial page | ClinicalTrials.gov or CTIS URL |
| `title` | Brief trial title | Free text |
| `study_type` | Trial type from source | `INTERVENTIONAL`, `OBSERVATIONAL`, `EXPANDED_ACCESS`, `NA` |
| `study_design` | Normalized design label | `interventional`, `observational`, `expanded_access`, `unknown`, `NA` |
| `phase` | Trial phase | source values (`Phase 1`, `Phase 2`, `Phase 3`, `Phase 4`, combinations), `NA` |
| `status` | Recruitment/overall status | source statuses (`RECRUITING`, `COMPLETED`, etc.), `NA` |
| `sponsor` | Lead sponsor name | Free text / `NA` |
| `admission_date` | First registration/posting date used for the study | `YYYY-MM-DD` or `NA` |
| `last_update_date` | Last trial update date from source | `YYYY-MM-DD` or `NA` |
| `primary_completion_date` | Primary completion date from source | `YYYY-MM-DD` or `NA` |
| `has_results` | Result availability flag (source + PubMed correction) | `yes`, `no`, `NA` |
| `results_last_update` | Date linked to results posting/submission | `YYYY-MM-DD` or `NA` |
| `pubmed_links` | PubMed paper links found by NCT ID lookup | Pipe-separated PubMed URLs or `NA` |
| `publication_date` | Earliest publication date among linked PubMed papers | `YYYY-MM-DD` or `NA` |
| `publication_scan_date` | Last date when the publication linker scanned this trial | `YYYY-MM-DD` or `NA` |
| `publication_lag_days` | Days between publication and primary completion | Non-negative integer or `NA` |
| `publication_count` | Number of **full-match** publication rows linked to the trial (`trial_publications`) | Integer (`0+`) |
| `publication_match_methods` | Methods used for **full-match** publication linking | Comma-separated: `pubmed_link`, `nct_exact`, `secondary_nct_exact`, `doi_reference`, `title_fuzzy`, `NA` |
| `evidence_strength` | Heuristic evidence confidence level | `high`, `medium`, `low`, `very_low`, `unknown`, `NA` |
| `dead_end` | Trial likely ended without publication signal under rule set | `yes`, `no`, `NA` |
| `conditions` | Conditions list | Pipe-separated text or `NA` |
| `interventions` | Intervention entries with type and name | Pipe-separated `TYPE: name` or `NA` |
| `intervention_types` | **Separated intervention type(s)** | Comma-separated source types (`BEHAVIORAL`, `BIOLOGICAL`, `COMBINATION_PRODUCT`, `DEVICE`, `DIAGNOSTIC_TEST`, `DIETARY_SUPPLEMENT`, `DRUG`, `GENETIC`, `OTHER`, `PROCEDURE`, `RADIATION`) or `NA` |
| `primary_outcomes` | Primary outcome definitions | Pipe-separated text or `NA` |
| `secondary_outcomes` | Secondary outcome definitions | Pipe-separated text or `NA` |
| `inclusion_criteria` | Inclusion criteria text | Free text / `NA` |
| `exclusion_criteria` | Exclusion criteria text | Free text / `NA` |
| `locations` | Trial sites | Pipe-separated `site (city, country)` or `NA` |
| `brief_summary` | Source brief summary | Free text / `NA` |
| `detailed_description` | Source detailed description | Free text / `NA` |
| `therapeutic_class` | Normalized therapy class | `chemotherapy`, `immunotherapy`, `targeted_therapy`, `radiotherapy`, `surgical`, `locoregional_therapy`, `registry_program`, `translational_research`, `supportive_care`, `biomarker_diagnostics`, `observational_non_therapeutic`, `context_classified`, `NA` |
| `focus_tags` | Normalized tag set | Comma-separated tags or `NA` (includes CTIS scope flags like `mixed_solid_tumor`, `neuroendocrine_signal`, `hepatobiliary_signal`) |
| `pdac_match_reason` | Why the trial matched PDAC filter | `explicit_pdac`, `pdac_acronym`, `adenocarcinoma_pancreas`, `generic_pancreatic_cancer`, `generic_pancreatic_oncology`, `unknown_match`, `NA` |

## One full-row example (all fields)

```text
nct_id: NCT03859869
source: clinicaltrials.gov
secondary_id: NA
trial_link: https://clinicaltrials.gov/study/NCT03859869
title: Pancrelipase Delayed Release Capsules in Subjects With Exocrine Pancreatic Insufficiency Due to Pancreatic Cancer
study_type: INTERVENTIONAL
study_design: interventional
phase: Phase 3
status: COMPLETED
sponsor: AbbVie
admission_date: 2019-02-28
last_update_date: 2023-06-05
primary_completion_date: 2022-12-01
has_results: yes
results_last_update: 2023-06-05
publication_date: 2023-01-15
publication_lag_days: 45
publication_count: 2
publication_match_methods: pubmed_link,nct_exact
evidence_strength: medium
dead_end: no
conditions: Exocrine Pancreatic Insufficiency | Pancreatic Cancer
interventions: DRUG: Pancrelipase | DRUG: Placebo
intervention_types: DRUG
primary_outcomes: Change in Stool Fat From Baseline ... ; timeframe=Week 1
secondary_outcomes: NA
inclusion_criteria: Subject has EPI due to pancreatic cancer ...
exclusion_criteria: Subject has severe allergy to porcine proteins ...
locations: NA
brief_summary: This is a study in participants with Exocrine Pancreatic Insufficiency due to Pancreatic Cancer.
detailed_description: NA
therapeutic_class: supportive_care
focus_tags: supportive_outcomes,advanced_disease
pdac_match_reason: generic_pancreatic_cancer
```

## Current normalization notes (v1.4)

- CTIS uses multiple search terms by default (not only `pancreatic`) to improve capture.
- CTIS phases are normalized to `PHASE1`, `PHASE2`, `PHASE3`, `PHASE4` (+ combined values).
- CTIS date formats are normalized to `YYYY-MM-DD`.
- Broad-scope oncology signals are tagged in `focus_tags` (e.g., `mixed_solid_tumor`, `neuroendocrine_signal`, `hepatobiliary_signal`) for easier review/triage.
- Therapeutic class now uses ensemble scoring (existing class + focus tags + PubMed MeSH terms where available) to reduce unknown-like labels.
- Signal fields are recomputed each ingestion run and persisted in `clinical_trials`.
