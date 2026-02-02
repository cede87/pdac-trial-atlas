"""
Database session and engine configuration.
Uses SQLite by default.
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

DATABASE_URL = "sqlite:///./pdac_trials.db"

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False},  # needed for SQLite
)

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
)

Base = declarative_base()


def init_db():
    """
    Create all tables if they do not exist.
    """
    from db import models  # noqa: F401
    Base.metadata.create_all(bind=engine)
