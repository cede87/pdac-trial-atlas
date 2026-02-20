import unittest
from datetime import date, timedelta
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from db.models import ClinicalTrial
from db.session import Base
from scripts.ingest_clinicaltrials import compute_signal_fields


class SignalHeuristicsTests(unittest.TestCase):
    def setUp(self):
        self.db_path = Path("tests/tmp_signal_test.db")
        if self.db_path.exists():
            self.db_path.unlink()
        engine = create_engine(f"sqlite:///{self.db_path}", connect_args={"check_same_thread": False})
        Base.metadata.create_all(bind=engine)
        self.Session = sessionmaker(bind=engine, autocommit=False, autoflush=False)

    def tearDown(self):
        if self.db_path.exists():
            self.db_path.unlink()

    def test_evidence_and_dead_end_rules(self):
        session = self.Session()
        old_date = (date.today() - timedelta(days=365 * 6)).isoformat()

        rows = [
            ClinicalTrial(
                nct_id="NCT_HIGH",
                phase="PHASE3",
                status="RECRUITING",
                pubmed_links="https://pubmed.ncbi.nlm.nih.gov/12345678/",
                primary_completion_date="2020-01-01",
                publication_date="2021-01-01",
            ),
            ClinicalTrial(
                nct_id="NCT_MED",
                phase="PHASE2",
                status="ACTIVE_NOT_RECRUITING",
                pubmed_links="https://pubmed.ncbi.nlm.nih.gov/23456789/",
                primary_completion_date="2020-01-01",
                publication_date="2021-06-01",
            ),
            ClinicalTrial(
                nct_id="NCT_LOW",
                phase="PHASE1",
                status="RECRUITING",
                pubmed_links="NA",
                primary_completion_date="2020-01-01",
                publication_date="NA",
            ),
            ClinicalTrial(
                nct_id="NCT_VERY_LOW_DEAD_END",
                phase="PHASE2",
                status="COMPLETED",
                pubmed_links="NA",
                primary_completion_date=old_date,
                publication_date="NA",
            ),
        ]
        session.add_all(rows)
        session.commit()

        updated = compute_signal_fields(session)
        self.assertEqual(updated, 4)

        high = session.get(ClinicalTrial, "NCT_HIGH")
        med = session.get(ClinicalTrial, "NCT_MED")
        low = session.get(ClinicalTrial, "NCT_LOW")
        vlow = session.get(ClinicalTrial, "NCT_VERY_LOW_DEAD_END")

        self.assertEqual(high.evidence_strength, "high")
        self.assertEqual(med.evidence_strength, "medium")
        self.assertEqual(low.evidence_strength, "low")
        self.assertEqual(vlow.evidence_strength, "very_low")
        self.assertEqual(vlow.dead_end, "yes")

        self.assertIsNotNone(high.publication_lag_days)
        self.assertGreater(high.publication_lag_days, 0)

        session.close()


if __name__ == "__main__":
    unittest.main()
