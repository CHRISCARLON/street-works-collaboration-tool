from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from .models import Base
import os


DATABASE_URL = os.getenv(
    "DATABASE_URL", "postgresql://postgres:password@localhost:5432/collaboration_tool"
)

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def create_database_and_extensions():
    """Create database, schema and enable PostGIS extension"""

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
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS collaboration"))
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


if __name__ == "__main__":
    print("Setting up PostGIS database...")
    create_database_and_extensions()
    create_tables()
    print("Database setup complete!")
