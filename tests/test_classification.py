import unittest

from ingest.clinicaltrials import build_classification_text, classify_study


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


if __name__ == "__main__":
    unittest.main()
