import unittest
from pathlib import Path
from unittest.mock import patch

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from db.models import ClinicalTrial, ClinicalTrialPublication
from db.session import Base
from scripts.ingest_clinicaltrials import (
    _search_pubmed_pmids,
    _build_title_query,
    rebuild_trial_publications,
    refresh_trial_publication_summary,
)


class PublicationIndexTests(unittest.TestCase):
    def setUp(self):
        self.db_path = Path("tests/tmp_publication_index.db")
        if self.db_path.exists():
            self.db_path.unlink()
        engine = create_engine(f"sqlite:///{self.db_path}", connect_args={"check_same_thread": False})
        Base.metadata.create_all(bind=engine)
        self.Session = sessionmaker(bind=engine, autocommit=False, autoflush=False)

    def tearDown(self):
        if self.db_path.exists():
            self.db_path.unlink()

    @patch("scripts.ingest_clinicaltrials._fetch_pubmed_summary")
    def test_rebuild_uses_existing_pubmed_links(self, mock_summary):
        mock_summary.return_value = {
            "12345678": {
                "publication_date_raw": "2024 Jan 03",
                "publication_title": "Pancreatic trial publication",
                "journal": "J Clin Oncol",
                "doi": "10.1000/j.jco.2024.01.003",
            }
        }
        session = self.Session()
        session.add(
            ClinicalTrial(
                nct_id="NCT10000001",
                title="PDAC Trial",
                pubmed_links="https://pubmed.ncbi.nlm.nih.gov/12345678/ | https://doi.org/10.1000/j.jco.2024.01.003",
                has_results="no",
            )
        )
        session.commit()

        stats = rebuild_trial_publications(
            session,
            max_nct_lookups=0,
            max_title_lookups=0,
            max_doi_lookups=0,
            max_links_per_trial=5,
        )
        self.assertGreaterEqual(stats["publication_rows"], 1)

        pubs = (
            session.query(ClinicalTrialPublication)
            .filter(ClinicalTrialPublication.nct_id == "NCT10000001")
            .all()
        )
        self.assertTrue(any((p.pmid or "") == "12345678" for p in pubs))

        updated = refresh_trial_publication_summary(session)
        self.assertEqual(updated, 1)
        trial = session.get(ClinicalTrial, "NCT10000001")
        self.assertEqual(trial.has_results, "yes")
        self.assertEqual(trial.publication_date, "2024-01-03")
        self.assertIn("12345678", trial.pubmed_links)
        session.close()

    def test_build_title_query_includes_year_and_keywords(self):
        query = _build_title_query(
            "PDAC Trial Title",
            "Example Sponsor, Inc.",
            "2020-01-01",
            "2021-06-01",
            keywords=["KRAS", "Gemcitabine"],
            year_lookback=1,
            year_lookahead=5,
        )
        self.assertIn("(PDAC Trial Title[Title])", query)
        self.assertIn("2020[Date - Publication]", query)
        self.assertIn("2026[Date - Publication]", query)
        self.assertIn("Example Sponsor[Affiliation]", query)
        self.assertIn("KRAS", query)
        self.assertIn("Gemcitabine", query)
        self.assertIn("[Title/Abstract]", query)

    @patch("scripts.ingest_clinicaltrials._fetch_pubmed_summary")
    def test_rebuild_updates_existing_row_with_doi(self, mock_summary):
        mock_summary.return_value = {
            "12345678": {
                "publication_date_raw": "2024 Jan 03",
                "publication_title": "Pancreatic trial publication",
                "journal": "J Clin Oncol",
                "doi": "10.1000/j.jco.2024.01.003",
            }
        }
        session = self.Session()
        session.add(
            ClinicalTrial(
                nct_id="NCT10000002",
                title="PDAC Trial",
                pubmed_links="https://pubmed.ncbi.nlm.nih.gov/12345678/",
                has_results="no",
            )
        )
        session.add(
            ClinicalTrialPublication(
                nct_id="NCT10000002",
                pmid="12345678",
                doi="",
                publication_date="NA",
                publication_title="NA",
                journal="NA",
                match_method="pubmed_link",
                confidence=98,
                is_full_match="yes",
            )
        )
        session.commit()

        rebuild_trial_publications(
            session,
            max_nct_lookups=0,
            max_title_lookups=0,
            max_doi_lookups=0,
            max_links_per_trial=5,
            incremental_mode=False,
        )
        pubs = (
            session.query(ClinicalTrialPublication)
            .filter(ClinicalTrialPublication.nct_id == "NCT10000002")
            .all()
        )
        self.assertEqual(len(pubs), 1)
        self.assertEqual(pubs[0].doi, "10.1000/j.jco.2024.01.003")
        session.close()

    @patch("scripts.ingest_clinicaltrials._search_pubmed_pmids")
    @patch("scripts.ingest_clinicaltrials._fetch_pubmed_summary")
    def test_rebuild_title_fallback_when_no_ids(self, mock_summary, mock_search):
        mock_search.return_value = ["87654321"]
        mock_summary.return_value = {
            "87654321": {
                "publication_date_raw": "2023 Dec 15",
                "publication_title": "A Study of KRAS inhibition in pancreatic cancer",
                "journal": "Cancer Res",
                "doi": "",
            }
        }
        session = self.Session()
        session.add(
            ClinicalTrial(
                nct_id="CTIS-ONLY-001",
                title="A Study of KRAS inhibition in pancreatic cancer",
                sponsor="Example Sponsor",
                admission_date="2023-01-01",
                pubmed_links="NA",
                has_results="no",
            )
        )
        session.commit()

        stats = rebuild_trial_publications(
            session,
            max_nct_lookups=0,
            max_title_lookups=5,
            max_doi_lookups=0,
            max_links_per_trial=5,
        )
        self.assertEqual(stats["title_lookups_used"], 1)

        pubs = (
            session.query(ClinicalTrialPublication)
            .filter(ClinicalTrialPublication.nct_id == "CTIS-ONLY-001")
            .all()
        )
        self.assertEqual(len(pubs), 1)
        self.assertEqual(pubs[0].match_method, "title_fuzzy")

        refresh_trial_publication_summary(session)
        trial = session.get(ClinicalTrial, "CTIS-ONLY-001")
        self.assertEqual(trial.publication_date, "2023-12-15")
        self.assertIn("87654321", trial.pubmed_links)
        session.close()

    @patch("scripts.ingest_clinicaltrials._search_pubmed_pmids")
    @patch("scripts.ingest_clinicaltrials._fetch_pubmed_summary")
    def test_rebuild_title_fallback_below_threshold_is_not_full_match(self, mock_summary, mock_search):
        mock_search.return_value = ["11223344"]
        mock_summary.return_value = {
            "11223344": {
                "publication_date_raw": "2023 Jan 10",
                "publication_title": "Pancreatic biomarker outcomes in advanced disease",
                "journal": "Oncology",
                "doi": "",
            }
        }
        session = self.Session()
        session.add(
            ClinicalTrial(
                nct_id="CTIS-ONLY-002",
                title="Pancreatic KRAS pathway exploratory safety trial in metastatic disease",
                sponsor="Example Sponsor",
                admission_date="2023-01-01",
                pubmed_links="NA",
                has_results="no",
            )
        )
        session.commit()

        rebuild_trial_publications(
            session,
            max_nct_lookups=0,
            max_title_lookups=5,
            max_doi_lookups=0,
            max_links_per_trial=5,
            full_match_min_confidence=80,
        )
        pub = (
            session.query(ClinicalTrialPublication)
            .filter(ClinicalTrialPublication.nct_id == "CTIS-ONLY-002")
            .one()
        )
        self.assertEqual(pub.match_method, "title_fuzzy")
        self.assertEqual(pub.is_full_match, "no")
        self.assertLess(pub.confidence, 80)

        refresh_trial_publication_summary(session)
        trial = session.get(ClinicalTrial, "CTIS-ONLY-002")
        self.assertEqual(trial.has_results, "no")
        self.assertEqual(trial.pubmed_links, "NA")
        session.close()

    @patch("scripts.ingest_clinicaltrials.requests.get")
    def test_search_pubmed_pmids_parses_esearch_idlist(self, mock_get):
        mock_get.return_value.json.return_value = {
            "esearchresult": {"idlist": ["12345", "not-a-pmid", "67890"]}
        }
        mock_get.return_value.raise_for_status.return_value = None

        pmids = _search_pubmed_pmids("NCT12345678[si]", max_links=3)
        self.assertEqual(pmids, ["12345", "67890"])

    @patch("scripts.ingest_clinicaltrials._search_pubmed_pmids")
    def test_incremental_mode_skips_trials_with_existing_full_match(self, mock_search):
        session = self.Session()
        session.add(
            ClinicalTrial(
                nct_id="NCT10000009",
                title="Historical PDAC trial",
                last_update_date="2019-01-01",
                pubmed_links="https://pubmed.ncbi.nlm.nih.gov/33445566/",
                has_results="yes",
            )
        )
        session.add(
            ClinicalTrialPublication(
                nct_id="NCT10000009",
                pmid="33445566",
                doi="",
                publication_date="2020-01-01",
                publication_title="Historical publication",
                journal="Journal",
                match_method="nct_exact",
                confidence=92,
                is_full_match="yes",
            )
        )
        session.commit()

        stats = rebuild_trial_publications(
            session,
            max_nct_lookups=5,
            max_title_lookups=5,
            max_doi_lookups=5,
            max_links_per_trial=5,
            incremental_mode=True,
            refresh_days=120,
        )

        self.assertEqual(stats["scanned_trials"], 0)
        self.assertEqual(stats["skipped_trials"], 1)
        mock_search.assert_not_called()
        pubs = (
            session.query(ClinicalTrialPublication)
            .filter(ClinicalTrialPublication.nct_id == "NCT10000009")
            .all()
        )
        self.assertEqual(len(pubs), 1)
        self.assertEqual((pubs[0].is_full_match or "").lower(), "yes")
        session.close()

    @patch("scripts.ingest_clinicaltrials._search_pubmed_pmids")
    def test_incremental_mode_uses_retry_window_for_no_match_trials(self, mock_search):
        session = self.Session()
        session.add(
            ClinicalTrial(
                nct_id="NCT10000010",
                title="No match trial",
                last_update_date="2019-01-01",
                publication_scan_date="2026-02-10",
                pubmed_links="NA",
                has_results="no",
            )
        )
        session.commit()

        stats = rebuild_trial_publications(
            session,
            max_nct_lookups=5,
            max_title_lookups=5,
            max_doi_lookups=5,
            max_links_per_trial=5,
            incremental_mode=True,
            refresh_days=120,
            retry_days_no_match=30,
        )

        self.assertEqual(stats["scanned_trials"], 0)
        self.assertEqual(stats["skipped_trials"], 1)
        mock_search.assert_not_called()
        session.close()


if __name__ == "__main__":
    unittest.main()
