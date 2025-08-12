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
        lat, lon = 51.53788595115742, -0.09974588865375973
        point_wkt = f"POINT({lon} {lat})"


        # Linestring coordinates representing the streetwork area
        linestring_coords = [
            [-0.100267922164017, 51.53743438161744],
            [-0.100252857275593, 51.53744987456288],
            [-0.100274004594968, 51.53746145767693],
            [-0.099951908088231, 51.53770803172655],
            [-0.099617503110198, 51.53799037559009],
            [-0.099497916349845, 51.53809185224498],
            [-0.099210176066717, 51.538335608676924],
            [-0.09919506371428, 51.538352224784134],
        ]

        linestring_wkt = (
            "LINESTRING("
            + ",".join([f"{coord[0]} {coord[1]}" for coord in linestring_coords])
            + ")"
        )


        sample_project = Project(
            project_id="PROJ_CDT1021657513",
            source="Cadent Gas",
            swa_code="12345",
            title="Main Pipe Asset Maintenance - London",
            scheme="Maintenance work on main gas pipe asset CDT1021657513",
            simple_theme="Gas",
            geo_point=f"{lat}, {lon}",
            geometry=WKTElement(point_wkt, srid=4326),
            geo_shape=WKTElement(linestring_wkt, srid=4326),
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
            usrn="21604817",
        )

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
