from datetime import datetime


def parse_date(date_struct):
    if not date_struct:
        return None
    try:
        return datetime.strptime(date_struct.get("date"), "%Y-%m-%d").date()
    except Exception:
        return None

def classify_study(proto):
    design = proto.get("designModule", {})
    study_type = design.get("studyType")

    if study_type == "Interventional":
        return "therapeutic"

    return "non_therapeutic"

def normalize_trial(study):
    proto = study.get("protocolSection", {})
    ident = proto.get("identificationModule", {})
    status = proto.get("statusModule", {})
    sponsor = proto.get("sponsorCollaboratorsModule", {})
    design = proto.get("designModule", {})
    conditions = proto.get("conditionsModule", {})

    study_category = classify_study(proto)

    return {
        "nct_id": ident.get("nctId"),
        "title": ident.get("briefTitle"),
        "phase": (
            design.get("phaseList", {})
            .get("phase", ["Unknown"])[0]
        ),
        "status": status.get("overallStatus"),
        "condition_raw": ", ".join(
            conditions.get("conditions", [])
        ),
        "study_type": design.get("studyType"),
        "study_category": study_category,
        "start_date": parse_date(status.get("startDateStruct")),
        "primary_completion": parse_date(
            status.get("primaryCompletionDateStruct")
        ),
        "sponsor": sponsor.get("leadSponsor", {}).get("name"),
        "setting": "unknown",
        "line_of_therapy": "unknown"
    }

def infer_therapeutic_intent(title):
    if not title:
        return "unclear"

    title_l = title.lower()

    supportive_keywords = [
        "quality of life",
        "symptom",
        "pain",
        "nutrition",
        "pancrelipase",
        "appetite",
        "supportive",
        "palliative"
    ]

    for kw in supportive_keywords:
        if kw in title_l:
            return "supportive"

    return "anticancer"
