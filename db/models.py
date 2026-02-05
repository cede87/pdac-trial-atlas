"""SQLAlchemy models."""

from sqlalchemy import Column, ForeignKey, String, Text
from db.session import Base


class ClinicalTrial(Base):
    __tablename__ = "clinical_trials"

    # Core identifiers
    nct_id = Column(String, primary_key=True, index=True)
    source = Column(String)
    secondary_id = Column(String)
    trial_link = Column(Text)
    title = Column(Text)

    # Trial metadata
    study_type = Column(String)
    study_design = Column(String)
    phase = Column(String)
    status = Column(String)
    sponsor = Column(String)
    admission_date = Column(String)
    last_update_date = Column(String)
    has_results = Column(String)
    results_last_update = Column(String)
    pubmed_links = Column(Text)
    intervention_types = Column(String)

    # Semantic classification
    therapeutic_class = Column(String)
    focus_tags = Column(Text)
    pdac_match_reason = Column(Text)


class ClinicalTrialDetails(Base):
    __tablename__ = "clinical_trial_details"

    nct_id = Column(String, ForeignKey("clinical_trials.nct_id"), primary_key=True, index=True)
    conditions = Column(Text)
    interventions = Column(Text)
    primary_outcomes = Column(Text)
    secondary_outcomes = Column(Text)
    inclusion_criteria = Column(Text)
    exclusion_criteria = Column(Text)
    locations = Column(Text)
    brief_summary = Column(Text)
    detailed_description = Column(Text)
