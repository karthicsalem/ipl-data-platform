import sqlite3
import shutil
import os
from datetime import datetime

DB_PATH = "data/local_db/ipl.db"
BRONZE_PATH = "data/bronze/"
SILVER_PATH = "data/silver/"

def backup_registry(conn):
    """Move existing registry records to history table before cleanup"""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS registry_history (
            match_id      TEXT,
            revision      INTEGER,
            created       TEXT,
            data_version  TEXT,
            ingested_at   TEXT,
            archived_at   TEXT
        )
    """)

    conn.execute("""
        INSERT INTO registry_history
        SELECT 
            match_id,
            revision,
            created,
            data_version,
            ingested_at,
            ? AS archived_at
        FROM registry
    """, (datetime.now().isoformat(),))

    count = conn.execute("SELECT COUNT(*) FROM registry_history").fetchone()[0]
    conn.commit()
    print(f"Backed up {count} records to registry_history")

def clear_registry(conn):
    """Clear registry table after backup"""
    conn.execute("DELETE FROM registry")
    conn.commit()
    print("Registry cleared")

def clear_parquet(path, name):
    """Delete parquet directory"""
    if os.path.exists(path):
        shutil.rmtree(path)
        print(f"{name} parquet cleared")
    else:
        print(f"{name} path not found, skipping")

def run_cleanup(clear_bronze=True, clear_silver=True):
    """
    Main cleanup orchestrator.
    Backs up registry before clearing anything.
    """
    print(f"\n--- Cleanup started at {datetime.now().isoformat()} ---\n")

    conn = sqlite3.connect(DB_PATH)

    # Always backup first
    backup_registry(conn)
    clear_registry(conn)
    conn.close()

    if clear_bronze:
        clear_parquet(BRONZE_PATH, "Bronze")

    if clear_silver:
        clear_parquet(SILVER_PATH, "Silver")

    print(f"\n--- Cleanup complete ---\n")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Cleanup IPL data platform local data")
    parser.add_argument("--bronze-only", action="store_true", help="Clear bronze only")
    parser.add_argument("--silver-only", action="store_true", help="Clear silver only")
    args = parser.parse_args()

    if args.bronze_only:
        run_cleanup(clear_bronze=True, clear_silver=False)
    elif args.silver_only:
        run_cleanup(clear_bronze=False, clear_silver=True)
    else:
        run_cleanup()