from sqlalchemy import (
    Column,
    String,
    Integer,
    Float,
    Boolean,
    Date,
    DateTime,
    Text,
)
from sqlalchemy.ext.declarative import declarative_base
from geoalchemy2 import Geometry
from datetime import datetime

Base = declarative_base()


class Project(Base):
    __tablename__ = "raw_projects"
    __table_args__ = {"schema": "collaboration"}

    # Primary key
    project_id = Column(String, primary_key=True)

    # Project Identification
    programme_id = Column(String, nullable=True)
    source = Column(String, nullable=False)
    swa_code = Column(String, nullable=False)
    contact = Column(String, nullable=True)
    department = Column(String, nullable=True)
    tele = Column(String, nullable=True)
    email = Column(String, nullable=True)

    # Project Details
    title = Column(String, nullable=False)
    scheme = Column(Text, nullable=False)
    simple_theme = Column(String, nullable=False)
    multi_theme = Column(String, nullable=True)
    comments = Column(Text, nullable=True)

    # Location Information - PostGIS geometry
    geo_point = Column(String, nullable=False)
    geometry = Column(Geometry("POINT", srid=4326), nullable=False)
    geo_shape = Column(Geometry("LINESTRING", srid=4326), nullable=True)
    usrn = Column(Integer, nullable=True)
    post_code = Column(String, nullable=True)
    site_area = Column(Float, nullable=True)
    location_meta = Column(String, nullable=True)

    # Asset Information
    asset_type = Column(String, nullable=False)
    pressure = Column(String, nullable=True)
    material = Column(String, nullable=True)
    diameter = Column(Float, nullable=True)
    diam_unit = Column(String, nullable=True)
    carr_mat = Column(String, nullable=True)
    carr_dia = Column(Float, nullable=True)
    carr_di_un = Column(String, nullable=True)
    asset_id = Column(String, nullable=False)
    depth = Column(Float, nullable=True)
    ag_ind = Column(Boolean, default=False)
    inst_date = Column(Date, nullable=True)
    length = Column(Float, nullable=True)
    length_unit = Column(String, nullable=True)

    # Timing and Scheduling
    start_date = Column(Date, nullable=True)
    start_date_yy = Column(Integer, nullable=False)
    start_date_meta = Column(String, nullable=True)
    completion_date = Column(Date, nullable=True)
    completion_date_yy = Column(Integer, nullable=False)
    completion_date_meta = Column(String, nullable=True)
    dates_yy_range = Column(String, nullable=True)
    flexibility = Column(Integer, nullable=True)

    # Financial Information
    programme_value = Column(Float, nullable=True)
    programme_range = Column(String, nullable=True)
    programme_value_meta = Column(String, nullable=True)
    project_value = Column(Float, nullable=True)
    project_range = Column(String, nullable=True)
    project_value_meta = Column(String, nullable=True)

    # Status and Governance
    funding_status = Column(String, nullable=False)
    planning_status = Column(String, nullable=True)
    collaboration = Column(Boolean, default=True)
    restrictions = Column(Text, nullable=True)

    # Additional Information
    more_url = Column(String, nullable=True)
    provider_db_date = Column(Date, nullable=False)

    # System Metadata
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, nullable=True)
    processing_status = Column(String, default="pending")
