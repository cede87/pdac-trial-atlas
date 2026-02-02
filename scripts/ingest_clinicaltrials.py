"""
Ingest PDAC-related clinical trials from ClinicalTrials.gov,
classify them (therapeutic / biomarker / other),
store them in the database, and print a readable preview table.

Comments intentionally verbose for clarity.
"""

from ingest.clinicaltrials import fetch_trials_pancreas
from db.session import SessionLocal, init_db
from db.models import ClinicalTrial

from tabulate import tabulate


# -------------------------------------------------------------------
# Trial classification logic
# -------------------------------------------------------------------

def classify_trial(title: str, study_type: str) -> tuple[str, str]:
    """
    Classify a clinical trial based on title keywords and study type.

    Returns:
        (classification, reason)
    """

    t = title.lower()

    # -------------------------
    # Therapeutic keywords
    # -------------------------
    therapeutic_keywords = [
        # generic therapy terms
        "treatment", "therapy", "therapeutic", "intervention",
        "chemotherapy", "immunotherapy", "radiotherapy", "radiation",
        "drug", "agent", "compound", "monotherapy", "combination",

        # specific treatment strategies
        "folfirinox", "gemcitabine", "nab-paclitaxel", "abraxane",
        "irinotecan", "oxaliplatin", "cisplatin", "capecitabine",
        "durvalumab", "pembrolizumab", "nivolumab", "avelumab",
        "trastuzumab", "olaparib", "talazoparib",

        # advanced therapies
        "car-t", "cart", "tcr", "vaccine", "neoantigen",
        "oncolytic", "viral", "cell therapy",

        # procedures
        "surgery", "resection", "sbtr", "sbrt",
        "ablation", "radiofrequency", "hifu", "hipec",

        # trial wording
        "dose escalation", "dose expansion",
        "phase i", "phase ii", "phase iii"
    ]

    # -------------------------
    # Biomarker / molecular keywords
    # -------------------------
    biomarker_keywords = [
        "biomarker", "marker", "molecular", "signature",
        "expression", "profiling", "mutation", "mutational",
        "genomic", "genetic", "germline", "somatic",

        # liquid biopsy
        "ctdna", "circulating tumor dna", "circulating tumor cells",
        "ctc", "blood biomarker", "plasma",

        # omics
        "transcriptomic", "proteomic", "metabolomic", "multiomics",
        "epigenetic", "methylation",

        # imaging biomarkers
        "pet", "mri", "ct", "imaging biomarker", "radiomic",

        # prognosis / prediction
        "predictive", "prognostic", "risk model",
        "stratification", "response prediction"
    ]

    # -------------------------
    # Early detection / screening (kept but tagged)
    # -------------------------
    early_detection_keywords = [
        "screening", "early detection", "early diagnosis",
        "high-risk", "surveillance", "follow-up",
        "precancerous", "ipan", "ipmn"
    ]

    # -------------------------
    # Rule-based classification
    # -------------------------

    if study_type == "INTERVENTIONAL":
        return "therapeutic", "Interventional study"

    if any(k in t for k in biomarker_keywords):
        return "biomarker", "Biomarker or molecular study"

    if any(k in t for k in early_detection_keywords):
        return "other", "Early detection / screening study"

    return "other", "Does not match therapeutic or biomarker criteria"


# -------------------------------------------------------------------
# Main ingestion routine
# -------------------------------------------------------------------

def run():
    """
    Main ingestion flow:
    - fetch pancreas-related trials
    - classify them
    - upsert into DB
    - print a preview table
    """

    init_db()
    session = SessionLocal()

    print("Fetching pancreas-related trials from ClinicalTrials.gov ...")

    studies = fetch_trials_pancreas(max_records=1000)

    inserted = 0

    for s in studies:
        nct_id = s["nct_id"]

        title = s.get("title", "")
        study_type = s.get("study_type", "UNKNOWN")
        phase = s.get("phase", "Unknown")
        status = s.get("status", "Unknown")
        sponsor = s.get("sponsor", "Unknown")

        classification, reason = classify_trial(title, study_type)

        # Upsert logic (idempotent ingestion)
        trial = session.get(ClinicalTrial, nct_id)
        if not trial:
            trial = ClinicalTrial(nct_id=nct_id)
            session.add(trial)
            inserted += 1

        trial.title = title
        trial.study_type = study_type
        trial.phase = phase
        trial.status = status
        trial.sponsor = sponsor
        trial.classification = classification
        trial.classification_reason = reason

    session.commit()

    print(f"\nTrials processed: {len(studies)}")
    print(f"New trials inserted: {inserted}")

    # ----------------------------------------------------------------
    # Console preview table (WHAT YOU WANTED)
    # ----------------------------------------------------------------

    rows = (
        session.query(
            ClinicalTrial.nct_id,
            ClinicalTrial.title,
            ClinicalTrial.study_type,
            ClinicalTrial.phase,
            ClinicalTrial.status,
            ClinicalTrial.classification,
        )
        .order_by(ClinicalTrial.classification, ClinicalTrial.phase)
        .limit(50)
        .all()
    )

    print("\nPDAC trials (classified preview):\n")
    print(
        tabulate(
            rows,
            headers=[
                "NCT",
                "Title",
                "StudyType",
                "Phase",
                "Status",
                "Class",
            ],
            tablefmt="github",
            maxcolwidths=[12, 55, 15, 10, 20, 15],
        )
    )

    session.close()


# -------------------------------------------------------------------
# Entry point
# -------------------------------------------------------------------

if __name__ == "__main__":
    run()
