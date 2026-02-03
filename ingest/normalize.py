from datetime import datetime


def parse_date(date_struct):
    if not date_struct:
        return None
    try:
        return datetime.strptime(date_struct.get("date"), "%Y-%m-%d").date()
    except Exception:
        return None


def normalize_trial(study: dict) -> dict:
    """
    Normalize a single clinical trial record coming from
    clinicaltrials.py output.

    This function does NOT reclassify.
    It trusts upstream classification and just standardizes fields.
    """

    return {
        # Core identifiers
        "nct_id": study.get("nct_id"),
        "title": study.get("title"),

        # Trial metadata
        "study_type": study.get("study_type"),
        "study_design": study.get("study_design"),
        "phase": study.get("phase"),
        "status": study.get("status"),
        "sponsor": study.get("sponsor"),

        # PDAC relevance
        "pdac_match_reason": study.get("pdac_match_reason"),

        # Classification (already inferred upstream)
        "therapeutic_class": study.get("therapeutic_class"),
        "focus_tags": study.get("focus_tags"),

        # Dates (optional, may be null if missing)
        "start_date": parse_date(study.get("start_date"))
        if isinstance(study.get("start_date"), dict)
        else study.get("start_date"),

        "primary_completion_date": parse_date(
            study.get("primary_completion_date")
        )
        if isinstance(study.get("primary_completion_date"), dict)
        else study.get("primary_completion_date"),

        # Derived / future-use fields
        "setting": infer_setting(study),
        "line_of_therapy": infer_line_of_therapy(study),
    }


def infer_setting(study: dict) -> str:
    """
    Lightweight inference of disease setting.
    """
    tags = (study.get("focus_tags") or "").lower()

    if "advanced_disease" in tags:
        return "advanced"
    if "resectable_disease" in tags:
        return "resectable"

    return "unspecified"


def infer_line_of_therapy(study: dict) -> str:
    """
    Very conservative inference.
    """
    title = (study.get("title") or "").lower()

    if "first-line" in title:
        return "first_line"
    if "second-line" in title:
        return "second_line"
    if "refractory" in title:
        return "refractory"

    return "unspecified"
