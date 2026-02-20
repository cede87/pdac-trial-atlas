import unittest

from ingest.euctr import parse_summary_text, _normalize_status, _is_pdac_candidate


class EuctrParsingTests(unittest.TestCase):
    def test_parse_summary_text_basic_fields(self):
        payload = """
EudraCT Number:          2010-018900-10
Sponsor Protocol Number: FLIP110
Sponsor Name:            Laboratoires Mayoly Spindler
Full Title:              Safety and preliminary clinical activity in pancreatic cancer
Start Date:              2010-05-11
Medical condition:       Pancreatic cancer
Population Age:          Adults
Gender:                  Male, Female
Trial protocol:          FR(Ongoing)
Link:                    https://www.clinicaltrialsregister.eu/ctr-search/search?query=eudract_number:2010-018900-10
"""
        rows = parse_summary_text(payload)
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row.eudract_number, "2010-018900-10")
        self.assertEqual(row.sponsor_name, "Laboratoires Mayoly Spindler")
        self.assertIn("Pancreatic cancer", row.medical_conditions)
        self.assertIn("FR(Ongoing)", row.trial_protocols)
        self.assertTrue(row.link.startswith("https://www.clinicaltrialsregister.eu/ctr-search/search"))

    def test_status_normalization(self):
        status = _normalize_status(["FR(Ongoing)", "DE(Completed)"])
        self.assertIn("ONGOING", status)
        self.assertIn("COMPLETED", status)

    def test_pdac_candidate_logic(self):
        self.assertTrue(_is_pdac_candidate("Pancreatic ductal adenocarcinoma trial"))
        self.assertFalse(_is_pdac_candidate("Acute pancreatitis observational study"))


if __name__ == "__main__":
    unittest.main()
