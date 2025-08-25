from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from .models import Base
import os

host = os.getenv("POSTGRES_HOST", "localhost")
port = os.getenv("POSTGRES_PORT", "5432")
db = os.getenv("POSTGRES_DB", "collaboration_tool")
user = os.getenv("POSTGRES_USER", "postgres")
password = os.getenv("POSTGRES_PASSWORD", "password")

DATABASE_URL = f"postgresql://{user}:{password}@{host}:{port}/{db}"

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def create_database_and_extensions():
    """Create database, schema and enable PostGIS extension"""
    admin_engine = create_engine(DATABASE_URL.replace(f"/{db}", "/postgres"))

    with admin_engine.connect() as conn:
        conn.execute(text("COMMIT"))

        result = conn.execute(text(f"SELECT 1 FROM pg_database WHERE datname = '{db}'"))
        if result.fetchone():
            print(f"Database '{db}' already exists, skipping creation")
            admin_engine.dispose()
            return

        conn.execute(text(f"CREATE DATABASE {db}"))
        print(f"Database '{db}' created!")

    admin_engine.dispose()

    try:
        with engine.connect() as conn:
            conn.execute(text("COMMIT"))
            conn.execute(text("CREATE SCHEMA IF NOT EXISTS collaboration"))
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS postgis_topology"))
            conn.commit()
            print("Schema and extensions created successfully")
    except Exception as e:
        print(f"Database setup failed: {e}")
        raise


def create_tables():
    """Create all tables (only if they don't exist)"""
    try:
        with engine.connect() as conn:
            result = conn.execute(
                text(
                    "SELECT table_name FROM information_schema.tables WHERE table_schema = 'collaboration'"
                )
            )
            existing_tables = [row[0] for row in result.fetchall()]

            if existing_tables:
                print(f"Tables already exist: {', '.join(existing_tables)}")
                return

            print("No existing tables found, creating new tables...")
            Base.metadata.create_all(bind=engine)
            print("Tables created successfully")
    except Exception as e:
        print(f"Table creation failed: {e}")
        raise
