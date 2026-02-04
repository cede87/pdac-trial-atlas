# pdac-trial-atlas
An open, evidence-graded atlas of pancreatic ductal adenocarcinoma (PDAC) clinical trials. Focused on normalizing trial metadata, evidence gaps, and strategy-level patterns.

## Version

Current release: **v1.2**

## Disclaimer

This project is an independent, good-faith learning initiative intended to support exploration of publicly available PDAC clinical trial data.

It is **not** a medical product, clinical decision system, regulatory tool, or source of medical advice. It does not guarantee completeness, correctness, or fitness for clinical use. Any interpretation or downstream use of the data is the sole responsibility of the user.

No claims are made regarding outcomes, efficacy, safety, or research conclusions. The purpose of this work is to learn in public and contribute constructively, with humility, transparency, and a genuine intention to help the research community.

## Local dashboard

1. Install deps:
   `pip install -r requirements.txt`
2. Refresh data (optional but recommended):
   `PYTHONPATH=. python scripts/ingest_clinicaltrials.py`
3. Launch dashboard:
   `streamlit run frontend/dashboard.py`

The dashboard runs 100% local and reads from `pdac_trials.db`.

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

`PYTHONPATH=. .venv/bin/python -m unittest discover -s tests -v`

Run dataset QA (cross-field integrity + tag/match consistency):

`PYTHONPATH=. .venv/bin/python scripts/qa_report.py --strict --limit 20`

Recommended full local validation flow:

1. `PYTHONPATH=. .venv/bin/python scripts/ingest_clinicaltrials.py`
2. `PYTHONPATH=. .venv/bin/python -m unittest discover -s tests -v`
3. `PYTHONPATH=. .venv/bin/python scripts/qa_report.py --strict --limit 20`
4. `PYTHONPATH=. .venv/bin/python scripts/export_to_csv.py`

## Storage layout

- `clinical_trials` keeps compact fields for fast filtering/sorting (id, status, dates, class, tags, etc.).
- `clinical_trial_details` stores long-text fields (conditions, interventions, outcomes, eligibility, locations, summaries/descriptions).
- Both tables are linked 1:1 via `nct_id`.

## Data dictionary (current columns)

Below is what each field stores, expected values/patterns, and one quick example.

| Column | What it is | Possible values / format |
|---|---|---|
| `nct_id` | ClinicalTrials.gov trial ID | `NCT` + digits |
| `title` | Brief trial title | Free text |
| `study_type` | Trial type from source | `INTERVENTIONAL`, `OBSERVATIONAL`, `EXPANDED_ACCESS`, `NA` |
| `study_design` | Normalized design label | `interventional`, `observational`, `expanded_access`, `unknown`, `NA` |
| `phase` | Trial phase | source values (`Phase 1`, `Phase 2`, `Phase 3`, `Phase 4`, combinations), `NA` |
| `status` | Recruitment/overall status | source statuses (`RECRUITING`, `COMPLETED`, etc.), `NA` |
| `sponsor` | Lead sponsor name | Free text / `NA` |
| `admission_date` | First registration/posting date used for the study | `YYYY-MM-DD` or `NA` |
| `last_update_date` | Last trial update date from source | `YYYY-MM-DD` or `NA` |
| `has_results` | Result availability flag | `yes`, `no`, `NA` |
| `results_last_update` | Date linked to results posting/submission | `YYYY-MM-DD` or `NA` |
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
| `focus_tags` | Normalized tag set | Comma-separated tags or `NA` |
| `pdac_match_reason` | Why the trial matched PDAC filter | `explicit_pdac`, `pdac_acronym`, `adenocarcinoma_pancreas`, `generic_pancreatic_cancer`, `unknown_match`, `NA` |

## One full-row example (all fields)

```text
nct_id: NCT03859869
title: Pancrelipase Delayed Release Capsules in Subjects With Exocrine Pancreatic Insufficiency Due to Pancreatic Cancer
study_type: INTERVENTIONAL
study_design: interventional
phase: Phase 3
status: COMPLETED
sponsor: AbbVie
admission_date: 2019-02-28
last_update_date: 2023-06-05
has_results: yes
results_last_update: 2023-06-05
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
