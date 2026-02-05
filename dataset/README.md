# PDAC Trial Atlas dataset (v1.3)

This dataset is a curated, PDAC-focused view of clinical trials enriched with normalization, tagging, and cross-registry correlation to support exploration and lightweight analysis.

## Purpose

- Provide a single, local-friendly table of PDAC-relevant clinical trials.
- Add normalized fields (therapeutic class, focus tags, intervention types, design) to speed up filtering and exploration.
- Merge correlated records across registries when the same trial appears in both ecosystems.

## Data sources

- **ClinicalTrials.gov** (USA/international registry) public portal via its public API endpoints.
- **CTIS (EU Clinical Trials Information System)** via programmatic retrieval.

Notes:
- CTIS coverage is primarily for EU/EEA trials submitted under EU CTR (public CTIS portal states coverage from **31 Jan 2022** onward).
- Historical EU trials in the legacy EudraCT register are not included in this v1.3 export.

## Number of trials

- Total rows: **2,476**
- By source:
- `clinicaltrials.gov`: 2,324
- `ctis`: 117
- `clinicaltrials.gov+ctis`: 35

## Files

- `pdac-trials.csv` — UTF-8 CSV with a stable header.
- `pdac-trials.parquet` — Parquet version of the same table (useful for analytics).
- `schema.json` — machine-readable schema for `pdac-trials.csv`.

## Field descriptions

| Column | Type | Description |
|---|---|---|
| `nct_id` | string | Primary trial identifier. For ClinicalTrials.gov this is the NCT ID (e.g., NCT01234567). For CTIS-only trials, this is the EU CT number (e.g., 2022-500902-16-00). |
| `source` | string | Source registry for this record: clinicaltrials.gov, ctis, or clinicaltrials.gov+ctis (merged/correlated). |
| `secondary_id` | string | Secondary identifiers when available (comma-separated). For example, CTIS trials may list an NCT ID here. |
| `trial_link` | string | URL(s) to open the trial in its source registry. When merged, links may be separated by " \| ". |
| `title` | string | Trial title as provided by the registry. |
| `study_type` | string | Study type as reported/normalized (e.g., INTERVENTIONAL, OBSERVATIONAL). |
| `study_design` | string | Normalized study design classification derived from registry metadata. |
| `phase` | string | Trial phase (registry-provided, normalized where possible). |
| `status` | string | Recruitment/overall status (e.g., COMPLETED, RECRUITING). |
| `sponsor` | string | Lead sponsor organization name. |
| `admission_date` | string | Registry posting/registration date in ISO-8601 (YYYY-MM-DD) when available. |
| `last_update_date` | string | Last update date in ISO-8601 (YYYY-MM-DD) when available. |
| `has_results` | string | Whether results/publications were detected (yes/no). |
| `results_last_update` | string | Best-effort date associated with results availability/publication update (YYYY-MM-DD) when available. |
| `pubmed_links` | string | PubMed URL(s) linked to the trial (best-effort), separated by " \| ". |
| `conditions` | string | Conditions/medical indications field from the registry. |
| `interventions` | string | Interventions text extracted from the registry (may include type + name). |
| `intervention_types` | string | Comma-separated intervention type list (e.g., DRUG, RADIATION). |
| `primary_outcomes` | string | Primary outcome measures/endpoints text. |
| `secondary_outcomes` | string | Secondary outcome measures/endpoints text. |
| `inclusion_criteria` | string | Eligibility inclusion criteria text. |
| `exclusion_criteria` | string | Eligibility exclusion criteria text. |
| `locations` | string | Sites/locations text (best-effort) from the registry. |
| `brief_summary` | string | Brief summary/abstract text. |
| `detailed_description` | string | Longer detailed description text. |
| `therapeutic_class` | string | Normalized therapeutic strategy class (heuristic classification). |
| `focus_tags` | string | Comma-separated normalized tags describing trial focus (e.g., resectable_disease). |
| `pdac_match_reason` | string | Why the trial was included as PDAC-relevant (keyword/heuristic match reason). |

## Limitations

- This is **not** an authoritative or complete registry mirror; it is a PDAC-focused slice.
- PDAC inclusion is based on heuristic string matching and normalization rules; false positives/negatives are possible.
- Some fields may be missing or simplified depending on source availability; missing values are represented as `NA`.
- Publications/"has results" are best-effort and may lag or miss papers not linked to an NCT identifier.
- CTIS records may not always correlate to an NCT ID; merging only occurs when an explicit link is present.

## How the data was generated

1. Run the ingestion pipeline to build/update the local SQLite database (`pdac_trials.db`).
   - ClinicalTrials.gov PDAC-focused retrieval
   - CTIS PDAC-focused retrieval
   - De-duplication/merge for CTIS trials that reference an NCT identifier
   - Normalization and tagging
2. Export the final table from `pdac_trials.db` into CSV/Parquet.

Generation timestamp (UTC): **2026-02-05 20:24:09Z**

## License

This dataset is released under **CC BY 4.0**. See `LICENSE-CC-BY-4.0.txt`.
