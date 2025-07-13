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
    """Create database and enable PostGIS extension"""

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
    """Insert the sample project based on your asset data"""

    session = SessionLocal()

    try:
        # Convert coordinates to WKT format for PostGIS
        lat, lon = 53.496572840571446, -2.2205016817669714
        point_wkt = f"POINT({lon} {lat})"

        # Create polygon from geo_shape coordinates
        polygon_coords = [
            [-2.220892857181577, 53.496063545601565],
            [-2.220922122733954, 53.49608553288395],
            [-2.220931374588726, 53.49613643073084],
            [-2.220863893947934, 53.4962039688097],
            [-2.220811469243992, 53.49620000000000],
            [-2.220892857181577, 53.496063545601565],
        ]

        # Convert to WKT polygon
        polygon_wkt = (
            "POLYGON(("
            + ",".join([f"{coord[0]} {coord[1]}" for coord in polygon_coords])
            + "))"
        )

        # Create sample project
        sample_project = Project(
            project_id="PROJ_CDT440003968937",
            source="Cadent Gas",
            swa_code="12345",
            title="Main Pipe Asset Maintenance",
            scheme="Maintenance work on main gas pipe asset CDT440003968937",
            simple_theme="Gas",
            geo_point=f"{lat}, {lon}",
            geometry=WKTElement(point_wkt, srid=4326),
            geo_shape=WKTElement(polygon_wkt, srid=4326),
            easting=384500.0,
            northing=401200.0,
            asset_type="Main Pipe",
            pressure="LP",
            material="PE",
            diameter=125.0,
            diam_unit="MM",
            asset_id="CDT440003968937",
            ag_ind=False,
            inst_date=date(1983, 1, 1),
            length=100.0,
            length_unit="M",
            start_date_yy=2024,
            completion_date_yy=2024,
            start_date=date(2026, 4, 1),
            completion_date=date(2026, 4, 30),
            funding_status="Confirmed",
            planning_status="Confirmed",
            collaboration=True,
            restrictions="None",
            more_url="https://www.cadent.co.uk",
            provider_db_date=date.today(),
            processing_status="pending",
        )

        session.add(sample_project)
        session.commit()

        print(f"Sample project {sample_project.project_id} inserted successfully!")

    except Exception as e:
        session.rollback()
        print(f"Error inserting sample project: {e}")
        raise
    finally:
        session.close()


if __name__ == "__main__":
    print("Setting up PostGIS database...")
    create_database_and_extensions()
    create_tables()
    insert_sample_project()
    print("Database setup complete!")
