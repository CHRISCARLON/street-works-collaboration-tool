from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from .models import Base, Project
from geoalchemy2 import WKTElement
from datetime import date
import os


# Database connection
DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://postgres:password@localhost:5432/collaboration_tool"
)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def create_database_and_extensions():
    """Create database, schema and enable PostGIS extension"""

    # Connect to postgres to create database
    admin_engine = create_engine(
        DATABASE_URL.replace("/collaboration_tool", "/postgres")
    )

    with admin_engine.connect() as conn:
        conn.execute(text("COMMIT"))

        # Check if database exists
        result = conn.execute(
            text("SELECT 1 FROM pg_database WHERE datname = 'collaboration_tool'")
        )
        if not result.fetchone():
            conn.execute(text("CREATE DATABASE collaboration_tool"))
            print("Database 'collaboration_tool' created!")
        else:
            print("Database 'collaboration_tool' already exists")

    admin_engine.dispose()

    with engine.connect() as conn:
        conn.execute(text("COMMIT"))

        # Create collaboration schema
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS collaboration"))

        # Enable PostGIS extension
        try:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis_topology"))
            print("PostGIS extensions enabled!")
        except Exception as e:
            print(f"PostGIS extension setup: {e}")

        conn.commit()


def create_tables():
    """Create all tables"""
    Base.metadata.create_all(bind=engine)
    print("Database tables created successfully!")


def insert_sample_project():
    """Insert sample project based on cadent asset data"""

    session = SessionLocal()

    try:
        lat, lon = 51.53689330852058, -0.10102563622383007
        point_wkt = f"POINT({lon} {lat})"


        polygon_coords = [
            [-0.101807177355459, 51.53639593313029],
            [-0.10178773787031, 51.53638662605521],
            [-0.101763667770392, 51.53640197347534],
            [-0.101739597654272, 51.5364173208903],
            [-0.101565515762334, 51.536529],
            [-0.101807177355459, 51.53639593313029],
        ]

        polygon_wkt = (
            "POLYGON(("
            + ",".join([f"{coord[0]} {coord[1]}" for coord in polygon_coords])
            + "))"
        )

        # Calculate approximate easting/northing for British National Grid
        # These are approximate values for the London area
        easting = 529500.0
        northing = 181500.0

        sample_project = Project(
            project_id="PROJ_CDT1021657513",
            source="Cadent Gas",
            swa_code="12345",
            title="Main Pipe Asset Maintenance - London",
            scheme="Maintenance work on main gas pipe asset CDT1021657513",
            simple_theme="Gas",
            geo_point=f"{lat}, {lon}",
            geometry=WKTElement(point_wkt, srid=4326),
            geo_shape=WKTElement(polygon_wkt, srid=4326),
            easting=easting,
            northing=northing,
            asset_type="Main Pipe",
            pressure="LP",
            material="PE",
            diameter=125.0,
            diam_unit="MM",
            asset_id="CDT1021657513",
            ag_ind=False,
            inst_date=date(1982, 1, 7),
            length=100.0,
            length_unit="M",
            start_date_yy=2026,
            completion_date_yy=2026,
            start_date=date(2026, 4, 1),
            completion_date=date(2026, 4, 30),
            funding_status="Confirmed",
            planning_status="Confirmed",
            collaboration=True,
            restrictions="None",
            more_url="https://www.cadent.co.uk",
            provider_db_date=date.today(),
            processing_status="pending",
            usrn="21604817",  # Example USRN for London area
        )

        # Check if project exists
        existing_project = (
            session.query(Project).filter_by(project_id="PROJ_CDT1021657513").first()
        )
        if not existing_project:
            session.add(sample_project)
            print(f"Sample project PROJ_CDT1021657513 inserted successfully!")
        else:
            print(f"Project PROJ_CDT1021657513 already exists, skipping...")

        session.commit()

    except Exception as e:
        session.rollback()
        print(f"Error inserting sample projects: {e}")
        raise
    finally:
        session.close()


if __name__ == "__main__":
    print("Setting up PostGIS database...")
    create_database_and_extensions()
    create_tables()
    insert_sample_project()
    print("Database setup complete!")
