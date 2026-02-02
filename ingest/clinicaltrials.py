"""
ClinicalTrials.gov API client.

Fetches pancreas-related trials using a broad query ("pancreas")
and applies a strict PDAC-focused classification layer to reduce noise.

The goal is to keep:
- Pancreatic ductal adenocarcinoma (PDAC)
- Pancreas adenocarcinoma
- Pancreatic cancer (generic, but pancreas-specific)

And exclude:
- Carcinoma of unknown primary
- Generic solid tumors
- Non-oncological pancreas studies (diabetes, CF, nutrition, etc.)

OBSERVATIONAL studies are kept but explicitly tagged.
"""

import requests
from typing import List, Dict


BASE_URL = "https://clinicaltrials.gov/api/v2/studies"


# -------------------------------------------------------------------
# PDAC CORE FILTER
# -------------------------------------------------------------------

def is_pdac_core(title: str) -> bool:
    """
    Strong textual filter for PDAC / pancreatic cancer.

    This is intentionally conservative to reduce noise.
    """
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
# STUDY CLASSIFICATION
# -------------------------------------------------------------------

def classify_study(study_type: str, title: str) -> Dict[str, str]:
    """
    Adds lightweight semantic classification tags.

    These tags are NOT used to filter yet.
    They are meant for downstream analysis and comparison.
    """

    tags = []

    t = title.lower()

    # Study design
    if study_type == "INTERVENTIONAL":
        tags.append("interventional")
    elif study_type == "OBSERVATIONAL":
        tags.append("observational")
    elif study_type == "EXPANDED_ACCESS":
        tags.append("expanded_access")

    # Therapeutic focus
    if any(x in t for x in ["chemotherapy", "gemcitabine", "folfirinox", "irinotecan"]):
        tags.append("chemotherapy")

    if any(x in t for x in ["immunotherapy", "pd-1", "pd-l1", "ctla-4", "vaccine", "car-t"]):
        tags.append("immunotherapy")

    if any(x in t for x in ["biomarker", "marker", "ctdna", "circulating tumor", "genomic"]):
        tags.append("biomarker")

    if any(x in t for x in ["early detection", "screening", "imaging", "mri", "ct", "pet"]):
        tags.append("detection")

    if any(x in t for x in ["metastatic", "advanced", "unresectable"]):
        tags.append("advanced_disease")

    if any(x in t for x in ["resectable", "neoadjuvant", "adjuvant", "surgery"]):
        tags.append("resectable_disease")

    return {
        "classification": ",".join(tags) if tags else "unclassified"
    }


# -------------------------------------------------------------------
# MAIN FETCH FUNCTION
# -------------------------------------------------------------------

def fetch_trials_pancreas(max_records: int = 1000) -> List[Dict]:
    """
    Fetch pancreas-related clinical trials and apply PDAC filtering.

    Returns a list of normalized and classified dictionaries.
    """

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

            # --------------------------------------------------------
            # PDAC CORE FILTER (this is the key noise reducer)
            # --------------------------------------------------------
            if not is_pdac_core(title):
                continue

            study_type = design_mod.get("studyType", "UNKNOWN")

            classification = classify_study(study_type, title)

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
                    "classification": classification["classification"],
                }
            )

            if len(all_studies) >= max_records:
                return all_studies

        next_page_token = data.get("nextPageToken")
        if not next_page_token:
            break

    return all_studies
