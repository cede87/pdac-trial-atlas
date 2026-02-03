import unittest

import pandas as pd

from frontend.dashboard import _build_query_mask, split_csv_values


class DashboardQueryTests(unittest.TestCase):
    def setUp(self):
        self.df = pd.DataFrame(
            {
                "nct_id": ["NCT1", "NCT2", "NCT3", "NCT4"],
                "title": [
                    "KRAS inhibitor in metastatic PDAC",
                    "Phase 3 radiotherapy trial",
                    "After progression chemotherapy strategy",
                    "Observational biomarker registry",
                ],
                "notes": ["alpha", "beta", "gamma", "delta"],
            }
        )

    def test_empty_query_matches_all_rows(self):
        mask = _build_query_mask(self.df, "")
        self.assertEqual(mask.sum(), len(self.df))

    def test_and_restricts_query(self):
        mask = _build_query_mask(self.df, "kras AND metastatic")
        self.assertEqual(mask.tolist(), [True, False, False, False])

    def test_or_broadens_query(self):
        mask = _build_query_mask(self.df, "radiotherapy OR biomarker")
        self.assertEqual(mask.tolist(), [False, True, False, True])

    def test_quoted_phrase_is_supported(self):
        mask = _build_query_mask(self.df, '"after progression"')
        self.assertEqual(mask.tolist(), [False, False, True, False])

    def test_comma_is_treated_like_or(self):
        mask = _build_query_mask(self.df, "kras, registry")
        self.assertEqual(mask.tolist(), [True, False, False, True])

    def test_split_csv_values_drops_na_and_whitespace(self):
        self.assertEqual(split_csv_values("DRUG, PROCEDURE, NA, "), ["DRUG", "PROCEDURE"])


if __name__ == "__main__":
    unittest.main()
