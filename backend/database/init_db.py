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
    """Insert sample projects based on cadent asset data"""

    session = SessionLocal()

    try:
        lat1, lon1 = 53.496572840571446, -2.2205016817669714
        point_wkt1 = f"POINT({lon1} {lat1})"

        polygon_coords1 = [
            [-2.220892857181577, 53.496063545601565],
            [-2.220922122733954, 53.49608553288395],
            [-2.220931374588726, 53.49613643073084],
            [-2.220863893947934, 53.4962039688097],
            [-2.220811469243992, 53.49620000000000],
            [-2.220892857181577, 53.496063545601565],
        ]

        polygon_wkt1 = (
            "POLYGON(("
            + ",".join([f"{coord[0]} {coord[1]}" for coord in polygon_coords1])
            + "))"
        )

        sample_project1 = Project(
            project_id="PROJ_CDT440003968937",
            source="Cadent Gas",
            swa_code="12345",
            title="Main Pipe Asset Maintenance",
            scheme="Maintenance work on main gas pipe asset CDT440003968937",
            simple_theme="Gas",
            geo_point=f"{lat1}, {lon1}",
            geometry=WKTElement(point_wkt1, srid=4326),
            geo_shape=WKTElement(polygon_wkt1, srid=4326),
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
        )

        lat2, lon2 = 53.536976798965554, -2.2350125354635813
        point_wkt2 = f"POINT({lon2} {lat2})"

        polygon_coords2 = [
            [-2.23506464770909, 53.5372775565861],
            [-2.235058691190096, 53.53722374594018],
            [-2.235025480135593, 53.53706833942631],
            [-2.23499319345754, 53.53687278002434],
            [-2.234964532231858, 53.5366752],
            [-2.23506464770909, 53.5372775565861],
        ]

        polygon_wkt2 = (
            "POLYGON(("
            + ",".join([f"{coord[0]} {coord[1]}" for coord in polygon_coords2])
            + "))"
        )

        sample_project2 = Project(
            project_id="PROJ_CDT1265841717",
            source="Cadent Gas",
            swa_code="12345",
            title="Main Pipe Asset Maintenance - Site 2",
            scheme="Maintenance work on main gas pipe asset CDT1265841717",
            simple_theme="Gas",
            geo_point=f"{lat2}, {lon2}",
            geometry=WKTElement(point_wkt2, srid=4326),
            geo_shape=WKTElement(polygon_wkt2, srid=4326),
            easting=384000.0,
            northing=405500.0,
            asset_type="Main Pipe",
            pressure="LP",
            material="PE",
            diameter=63.0,
            diam_unit="MM",
            carr_mat="SI",
            carr_dia=3.0,
            carr_di_un="I",
            asset_id="CDT1265841717",
            ag_ind=False,
            inst_date=date(2007, 8, 17),
            length=150.0,
            length_unit="M",
            start_date_yy=2026,
            completion_date_yy=2026,
            start_date=date(2026, 5, 1),
            completion_date=date(2026, 5, 15),
            funding_status="Confirmed",
            planning_status="Confirmed",
            collaboration=True,
            restrictions="None",
            more_url="https://www.cadent.co.uk",
            provider_db_date=date.today(),
            processing_status="pending",
        )

        lat3, lon3 = 53.49530192933666, -2.243902541021569
        point_wkt3 = f"POINT({lon3} {lat3})"

        # Convert MultiLineString to LineString first
        line_coords3 = [
            [-2.244644164707686, 53.49545539181379],
            [-2.243891483370418, 53.49530075746595],
            [-2.243633036965647, 53.495246230895184],
            [-2.243161417386422, 53.495146072327664],
        ]

        buffer_distance = 0.0001
        
        polygon_coords3 = []
        for coord in line_coords3:
            polygon_coords3.extend([
                [coord[0] - buffer_distance, coord[1] - buffer_distance],
                [coord[0] + buffer_distance, coord[1] - buffer_distance],
                [coord[0] + buffer_distance, coord[1] + buffer_distance],
                [coord[0] - buffer_distance, coord[1] + buffer_distance],
            ])
        
        if polygon_coords3:
            polygon_coords3.append(polygon_coords3[0])

        # Create polygon WKT
        polygon_wkt3 = (
            "POLYGON(("
            + ",".join([f"{coord[0]} {coord[1]}" for coord in polygon_coords3])
            + "))"
        )

        sample_project3 = Project(
            project_id="PROJ_CDT626864553",
            source="Cadent Gas",
            swa_code="12345",
            title="Main Pipe Asset Maintenance - Site 3",
            scheme="Maintenance work on main gas pipe asset CDT626864553",
            simple_theme="Gas",
            geo_point=f"{lat3}, {lon3}",
            geometry=WKTElement(point_wkt3, srid=4326),
            geo_shape=WKTElement(polygon_wkt3, srid=4326),
            easting=383800.0,
            northing=401000.0,
            asset_type="Main Pipe",
            pressure="LP",
            material="PE",
            diameter=125.0,
            diam_unit="MM",
            asset_id="CDT626864553",
            ag_ind=False,
            inst_date=date(1980, 1, 1),
            length=200.0,
            length_unit="M",
            start_date_yy=2026,
            completion_date_yy=2026,
            start_date=date(2026, 6, 1),
            completion_date=date(2026, 6, 20),
            funding_status="Confirmed",
            planning_status="Confirmed",
            collaboration=True,
            restrictions="None",
            more_url="https://www.cadent.co.uk",
            provider_db_date=date.today(),
            processing_status="pending",
        )

        projects_to_add = [
            ("PROJ_CDT440003968937", sample_project1),
            ("PROJ_CDT1265841717", sample_project2),
            ("PROJ_CDT626864553", sample_project3),
        ]

        for project_id, project_obj in projects_to_add:
            existing_project = session.query(Project).filter_by(project_id=project_id).first()
            if not existing_project:
                session.add(project_obj)
                print(f"Sample project {project_id} inserted successfully!")
            else:
                print(f"Project {project_id} already exists, skipping...")

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
