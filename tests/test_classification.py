import unittest

from ingest.clinicaltrials import (
    _extract_interventions,
    _extract_outcomes,
    build_classification_text,
    classify_study,
    pdac_match_reason,
)


class ClassificationTests(unittest.TestCase):
    def test_focus_without_therapy_is_not_unknown(self):
        c = classify_study("INTERVENTIONAL", "metastatic pancreatic cancer trial")
        self.assertEqual(c["therapeutic_class"], "context_classified")
        self.assertIn("advanced_disease", c["focus"])

    def test_biomarker_detection_maps_to_biomarker_class(self):
        c = classify_study(
            "OBSERVATIONAL",
            "Early detection imaging MRI and CA19-9 for pancreatic cancer",
        )
        self.assertEqual(c["therapeutic_class"], "biomarker_diagnostics")
        self.assertIn("biomarker", c["focus"])

    def test_supportive_care_is_detected(self):
        c = classify_study(
            "INTERVENTIONAL",
            "Acupuncture and nutritional supportive care for pancreatic cancer pain",
        )
        self.assertEqual(c["therapeutic_class"], "supportive_care")

    def test_strength_training_maps_to_supportive_care(self):
        c = classify_study("INTERVENTIONAL", "Strength training for pancreatic cancer")
        self.assertEqual(c["therapeutic_class"], "supportive_care")

    def test_build_text_uses_interventions_and_conditions(self):
        protocol = {
            "identificationModule": {"briefTitle": "PDAC pilot"},
            "conditionsModule": {"conditions": ["Pancreatic Adenocarcinoma"]},
            "armsInterventionsModule": {
                "interventions": [{"name": "Olaparib", "description": "PARP inhibitor"}]
            },
        }
        text = build_classification_text(protocol)
        c = classify_study("INTERVENTIONAL", text)
        self.assertEqual(c["therapeutic_class"], "targeted_therapy")

    def test_registry_program_is_detected(self):
        c = classify_study(
            "OBSERVATIONAL",
            "Pancreatic Cancer Registry for any person with family history",
        )
        self.assertEqual(c["therapeutic_class"], "registry_program")

    def test_translational_research_is_detected(self):
        c = classify_study(
            "INTERVENTIONAL",
            "Growth rate analysis of pancreatic cancer patient-derived organoids",
        )
        self.assertEqual(c["therapeutic_class"], "translational_research")

    def test_observational_without_signal_defaults_non_therapeutic(self):
        c = classify_study(
            "OBSERVATIONAL",
            "Prospective follow-up of pancreatic cancer outcomes",
        )
        self.assertEqual(c["therapeutic_class"], "observational_non_therapeutic")

    def test_interventional_without_signal_defaults_context(self):
        c = classify_study(
            "INTERVENTIONAL",
            "Investigator initiated phase II study in pancreatic cancer",
        )
        self.assertEqual(c["therapeutic_class"], "context_classified")

    def test_locoregional_therapy_is_detected(self):
        c = classify_study(
            "INTERVENTIONAL",
            "Electroporation therapy with bleomycin in pancreatic cancer",
        )
        self.assertEqual(c["therapeutic_class"], "locoregional_therapy")

    def test_focus_tags_are_more_specific(self):
        c = classify_study(
            "OBSERVATIONAL",
            "High-risk surveillance with MRI and ctDNA liquid biopsy for hereditary pancreatic cancer",
        )
        self.assertIn("early_detection", c["focus"])
        self.assertIn("imaging_diagnostics", c["focus"])
        self.assertIn("liquid_biopsy", c["focus"])
        self.assertIn("hereditary_risk", c["focus"])

    def test_extract_interventions_returns_names_and_types(self):
        arms_mod = {
            "interventions": [
                {"type": "DRUG", "name": "Gemcitabine"},
                {"type": "PROCEDURE", "name": "Whipple"},
                {"type": "DRUG", "name": "Nab-paclitaxel"},
            ]
        }
        interventions, intervention_types = _extract_interventions(arms_mod)
        self.assertIn("DRUG: Gemcitabine", interventions)
        self.assertIn("PROCEDURE: Whipple", interventions)
        self.assertEqual(intervention_types, "DRUG, PROCEDURE")

    def test_extract_outcomes_formats_measure_timeframe_and_description(self):
        outcomes_mod = {
            "primaryOutcomes": [
                {
                    "measure": "Overall Survival",
                    "timeFrame": "24 months",
                    "description": "OS from randomization",
                }
            ]
        }
        primary = _extract_outcomes(outcomes_mod, "primaryOutcomes")
        self.assertIn("Overall Survival", primary)
        self.assertIn("timeframe=24 months", primary)
        self.assertIn("OS from randomization", primary)

    def test_pdac_match_reason_handles_ductal_pancreas_phrase(self):
        reason = pdac_match_reason("Ductal adenocarcinoma of the pancreas pilot trial")
        self.assertEqual(reason, "explicit_pdac")


if __name__ == "__main__":
    unittest.main()
