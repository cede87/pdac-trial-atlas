"""
ClinicalTrials.gov API client.

Fetches pancreas-related trials using a broad query ("pancreas")
and applies a strict PDAC-focused classification layer to reduce noise.

Keeps:
- Pancreatic ductal adenocarcinoma (PDAC)
- Pancreas adenocarcinoma
- Pancreatic cancer (pancreas-specific)

Excludes:
- Unknown primary
- Generic solid tumors
- Non-oncological pancreas studies

OBSERVATIONAL studies are kept but explicitly tagged.
"""

import requests
from typing import List, Dict


BASE_URL = "https://clinicaltrials.gov/api/v2/studies"


# -------------------------------------------------------------------
# PDAC CORE FILTER
# -------------------------------------------------------------------

def is_pdac_core(title: str) -> bool:
    if not title:
        return False

    t = title.lower()

    pdac_terms = [
        "pancreatic ductal adenocarcinoma",
        "ductal adenocarcinoma of the pancreas",
        "pancreas adenocarcinoma",
        "pancreatic adenocarcinoma",
        "pdac",
        "pancreatic cancer",
    ]

    negative_terms = [
        "unknown primary",
        "solid tumor",
        "solid tumours",
        "multiple cancers",
        "various cancers",
        "different cancers",
        "non-pancreatic",
    ]

    if any(term in t for term in negative_terms):
        return False

    return any(term in t for term in pdac_terms)


# -------------------------------------------------------------------
# MATCH REASON (TRANSPARENCY)
# -------------------------------------------------------------------

def pdac_match_reason(title: str) -> str:
    t = title.lower()

    if "pancreatic ductal adenocarcinoma" in t:
        return "explicit_pdac"
    if "pdac" in t:
        return "pdac_acronym"
    if "pancreas adenocarcinoma" in t or "pancreatic adenocarcinoma" in t:
        return "adenocarcinoma_pancreas"
    if "pancreatic cancer" in t:
        return "generic_pancreatic_cancer"

    return "unknown_match"


# -------------------------------------------------------------------
# NOISE FLAGS (SOFT WARNINGS)
# -------------------------------------------------------------------

def noise_flags(title: str) -> List[str]:
    t = title.lower()
    flags = []

    if "solid tumor" in t or "solid tumours" in t:
        flags.append("generic_solid_tumor")

    if "unknown primary" in t:
        flags.append("unknown_primary")

    if "screening" in t or "early detection" in t:
        flags.append("early_detection")

    if "vaccine" in t and "treat" not in t:
        flags.append("non_therapeutic_vaccine")

    return flags


# -------------------------------------------------------------------
# STUDY CLASSIFICATION
# -------------------------------------------------------------------

def classify_study(study_type: str, text: str) -> Dict[str, str]:
    t = (text or "").lower()

    classification = {
        "study_design": "unknown",
        "therapeutic_class": "unknown",
        "focus": [],
    }

    # Study design
    if study_type == "INTERVENTIONAL":
        classification["study_design"] = "interventional"
    elif study_type == "OBSERVATIONAL":
        classification["study_design"] = "observational"
    elif study_type == "EXPANDED_ACCESS":
        classification["study_design"] = "expanded_access"

    # Therapeutic class (count keyword hits to avoid accidental overwrite)
    therapeutic_signals = {
        "chemotherapy": [
            "gemcitabine", "folfirinox", "irinotecan", "oxaliplatin",
            "cisplatin", "nab-paclitaxel", "paclitaxel", "5-fu", "fluorouracil",
            "asparaginase",
        ],
        "immunotherapy": [
            "immunotherapy", "pd-1", "pd-l1", "ctla-4", "vaccine",
            "car-t", "t cell", "nivolumab", "pembrolizumab", "atezolizumab",
            "antibody", "nk cell", "nk cells",
        ],
        "targeted_therapy": [
            "kras", "egfr", "parp", "targeted", "inhibitor", "inhibition",
            "olaparib", "trametinib", "erlotinib", "selumetinib",
            "vorinostat", "binimetinib", "belzutifan", "braf", "ccx872",
        ],
        "radiotherapy": ["radiation", "radiotherapy", "sbrt", "imrt"],
        "surgical": ["surgery", "resection", "pancreatectomy", "whipple"],
        "registry_program": [
            "registry", "database", "data management center", "survey", "case-vignette",
        ],
        "translational_research": [
            "organoid", "exosome", "serum-bank", "bioprinting", "microbiota",
            "perineural invasion",
        ],
        "supportive_care": [
            "pain", "acupuncture", "acupressure", "nutrition", "diet",
            "exercise", "fatigue", "psychosocial", "quality of life",
            "palliative", "supportive care", "prehabilitation", "training",
            "walking", "depression", "cachexia", "anorexia", "appetite",
            "sarcopenia", "pregabalin", "escitalopram", "engagement app",
            "app-based", "with app",
        ],
    }

    class_scores = {
        cls: sum(1 for term in terms if term in t)
        for cls, terms in therapeutic_signals.items()
    }
    best_class = max(class_scores, key=class_scores.get)
    if class_scores[best_class] > 0:
        classification["therapeutic_class"] = best_class

    # Focus tags
    if any(
        x in t
        for x in [
            "biomarker", "ctdna", "circulating tumor", "genomic", "mutation",
            "screening", "early detection", "detection", "diagnosis", "diagnostic",
            "imaging", "pet", "mri", "ca19-9", "liquid biopsy", "genetic testing",
            "methylation", "microrna", "micro-rna", "mirna", "risk model",
        ]
    ):
        classification["focus"].append("biomarker")

    if any(x in t for x in ["metastatic", "advanced", "unresectable"]):
        classification["focus"].append("advanced_disease")

    if any(x in t for x in ["resectable", "neoadjuvant", "adjuvant"]):
        classification["focus"].append("resectable_disease")

    if any(x in t for x in ["first-line", "second-line", "refractory"]):
        classification["focus"].append("line_of_therapy")

    # If focus tags exist but no explicit therapy signal, avoid "unknown"
    if classification["therapeutic_class"] == "unknown" and classification["focus"]:
        if "biomarker" in classification["focus"]:
            classification["therapeutic_class"] = "biomarker_diagnostics"
        elif classification["study_design"] == "observational":
            classification["therapeutic_class"] = "observational_non_therapeutic"
        else:
            classification["therapeutic_class"] = "context_classified"
    elif classification["therapeutic_class"] == "unknown":
        # Observational studies are often non-therapeutic registries/prognostic work.
        if classification["study_design"] == "observational":
            classification["therapeutic_class"] = "observational_non_therapeutic"

    return classification


def build_classification_text(protocol: Dict) -> str:
    """
    Build richer free-text context for classification so we don't rely
    only on briefTitle.
    """
    id_mod = protocol.get("identificationModule", {})
    cond_mod = protocol.get("conditionsModule", {})
    arms_mod = protocol.get("armsInterventionsModule", {})

    parts = [
        id_mod.get("briefTitle", ""),
        id_mod.get("officialTitle", ""),
        id_mod.get("acronym", ""),
        " ".join(cond_mod.get("conditions", []) or []),
        " ".join(cond_mod.get("keywords", []) or []),
    ]

    for i in arms_mod.get("interventions", []) or []:
        parts.append(i.get("name", ""))
        parts.append(i.get("description", ""))

    for a in arms_mod.get("armGroups", []) or []:
        parts.append(a.get("label", ""))
        parts.append(a.get("description", ""))

    return " ".join(p for p in parts if p)


# -------------------------------------------------------------------
# MAIN FETCH FUNCTION
# -------------------------------------------------------------------

def fetch_trials_pancreas(max_records: int = 1000) -> List[Dict]:
    params = {
        "query.term": "pancreas",
        "pageSize": 100,
        "format": "json",
    }

    all_studies = []
    next_page_token = None

    while True:
        if next_page_token:
            params["pageToken"] = next_page_token

        resp = requests.get(BASE_URL, params=params, timeout=30)
        resp.raise_for_status()

        data = resp.json()
        studies = data.get("studies", [])

        for s in studies:
            protocol = s.get("protocolSection", {})

            id_mod = protocol.get("identificationModule", {})
            status_mod = protocol.get("statusModule", {})
            design_mod = protocol.get("designModule", {})
            sponsor_mod = protocol.get("sponsorCollaboratorsModule", {})

            nct_id = id_mod.get("nctId")
            title = id_mod.get("briefTitle", "")

            if not nct_id:
                continue

            # PDAC FILTER
            if not is_pdac_core(title):
                continue

            study_type = design_mod.get("studyType", "UNKNOWN")
            classification_text = build_classification_text(protocol)
            classification = classify_study(study_type, classification_text)

            all_studies.append(
                {
                    "nct_id": nct_id,
                    "title": title,
                    "study_type": study_type,
                    "phase": (
                        design_mod.get("phases", ["Unknown"])[0]
                        if design_mod.get("phases")
                        else "Unknown"
                    ),
                    "status": status_mod.get("overallStatus", "Unknown"),
                    "sponsor": sponsor_mod.get("leadSponsor", {}).get("name", "Unknown"),
                    "pdac_match_reason": pdac_match_reason(title),
                    "study_design": classification["study_design"],
                    "therapeutic_class": classification["therapeutic_class"],
                    "focus_tags": ",".join(classification["focus"]) if classification["focus"] else "",
                    "noise_flags": ",".join(noise_flags(title)) if noise_flags(title) else "",
                }
            )

            if len(all_studies) >= max_records:
                return all_studies

        next_page_token = data.get("nextPageToken")
        if not next_page_token:
            break

    return all_studies
