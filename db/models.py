"""
SQLAlchemy models.
"""

from sqlalchemy import Column, String, Text
from db.session import Base


class ClinicalTrial(Base):
    __tablename__ = "clinical_trials"

    # Core identifiers
    nct_id = Column(String, primary_key=True, index=True)
    title = Column(Text)

    # Trial metadata
    study_type = Column(String)
    study_design = Column(String)
    phase = Column(String)
    status = Column(String)
    sponsor = Column(String)

    # Semantic classification
    therapeutic_class = Column(String)
    focus_tags = Column(Text)
    pdac_match_reason = Column(Text)
    noise_flags = Column(Text)

