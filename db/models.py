"""
SQLAlchemy models.
"""

from sqlalchemy import Column, String, Text
from db.session import Base


class ClinicalTrial(Base):
    __tablename__ = "clinical_trials"

    # Primary key
    nct_id = Column(String, primary_key=True, index=True)

    # Core metadata
    title = Column(Text, nullable=False)
    study_type = Column(String, nullable=True)
    phase = Column(String, nullable=True)
    status = Column(String, nullable=True)
    sponsor = Column(String, nullable=True)

    # Classification
    classification = Column(String, nullable=True)
    classification_reason = Column(Text, nullable=True)

    def __repr__(self):
        return f"<ClinicalTrial {self.nct_id} ({self.classification})>"
