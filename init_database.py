import os
import sys
from pathlib import Path
from backend.database.init_db import (
    create_database_and_extensions,
    create_tables,
    insert_sample_project,
)

project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))


def main():
    print("🚀 Initialising PostGIS database for Collaboration Tool...")

    # Check if PostgreSQL is running
    db_url = os.getenv(
        "DATABASE_URL",
        "postgresql://postgres:password@localhost:5432/collaboration_tool",
    )
    print(f"📊 Database URL: {db_url}")

    try:
        print("1️⃣ Creating database and enabling PostGIS extensions...")
        create_database_and_extensions()

        print("2️⃣ Creating tables...")
        create_tables()

        print("3️⃣ Inserting sample project...")
        insert_sample_project()

        print("✅ Database initialisation complete!")
        print("\n📋 Next steps:")
        print("   - Run: fastapi dev main.py")
        print("   - Visit: http://localhost:8000/docs")

    except Exception as e:
        print(f"❌ Error during initialisation: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
