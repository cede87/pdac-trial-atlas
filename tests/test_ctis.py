import unittest

from ingest.ctis import (
    DEFAULT_CTIS_PDAC_QUERY_TERMS,
    _extract_interventions,
    _extract_additional_focus_tags,
    _is_pdac_candidate,
    _map_ctis_study_type,
    _resolve_query_terms,
    normalize_ctis_date,
    normalize_ctis_phase,
)


class CtisNormalizationTests(unittest.TestCase):
    def test_normalize_ctis_date_supports_ddmmyyyy(self):
        self.assertEqual(normalize_ctis_date("25/10/2024"), "2024-10-25")

    def test_normalize_ctis_date_supports_iso_datetime(self):
        self.assertEqual(
            normalize_ctis_date("2024-10-25T18:25:35.308"),
            "2024-10-25",
        )

    def test_normalize_ctis_phase_handles_integrated_phases(self):
        self.assertEqual(
            normalize_ctis_phase("Phase II and Phase III (Integrated)"),
            "PHASE2/PHASE3",
        )

    def test_study_type_mapping_prefers_interventional_when_phase_present(self):
        self.assertEqual(
            _map_ctis_study_type("Phase II", ""),
            "INTERVENTIONAL",
        )

    def test_resolve_query_terms_uses_default_expanded_list(self):
        resolved = _resolve_query_terms()
        self.assertEqual(resolved, DEFAULT_CTIS_PDAC_QUERY_TERMS)

    def test_resolve_query_terms_prioritizes_explicit_query_terms(self):
        resolved = _resolve_query_terms(query_terms=["pancreatic", "pdac", "pancreatic"])
        self.assertEqual(resolved, ["pancreatic", "pdac"])

    def test_pdac_candidate_requires_pancreatic_oncology_signal(self):
        self.assertTrue(_is_pdac_candidate("Metastatic pancreatic ductal adenocarcinoma"))
        self.assertFalse(_is_pdac_candidate("Acute pancreatitis quality of life study"))

    def test_extract_interventions_collects_types(self):
        overview = {"product": "Gemcitabine"}
        details = {
            "authorizedApplication": {
                "authorizedPartI": {
                    "products": [
                        {
                            "productName": "Nivolumab",
                            "productDictionaryInfo": {
                                "prodName": "Nivolumab",
                                "productSubstances": [
                                    {"actSubstOrigin": "Biological"}
                                ],
                            },
                            "devices": [],
                        },
                        {
                            "productName": "",
                            "productDictionaryInfo": {},
                            "devices": [{"name": "Implantable pump"}],
                        },
                    ]
                }
            }
        }
        interventions, intervention_types = _extract_interventions(overview, details)
        self.assertIn("BIOLOGICAL: Nivolumab", interventions)
        self.assertIn("DEVICE: Implantable pump", interventions)
        self.assertIn("DRUG: Gemcitabine", interventions)
        self.assertIn("BIOLOGICAL", intervention_types)
        self.assertIn("DEVICE", intervention_types)
        self.assertIn("DRUG", intervention_types)

    def test_extract_additional_focus_tags_detects_mixed_and_neuroendocrine(self):
        tags = _extract_additional_focus_tags(
            "Basket trial in advanced solid tumors including gastro-entero-pancreatic neuroendocrine tumors"
        )
        self.assertIn("mixed_solid_tumor", tags)
        self.assertIn("neuroendocrine_signal", tags)


if __name__ == "__main__":
    unittest.main()
