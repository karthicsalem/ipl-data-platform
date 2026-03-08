import sqlite3
import os
import json

DB_PATH = "data/local_db/ipl.db"
RAW_DIR = "data/raw/ipl_json"

# get_unprocessed_files opens every file to read meta
# This is fine for now but a known bottleneck at scale - becomes the manifest pattern
def get_unprocessed_files(conn):
    """
    Returns list of files that are:
    - New (match_id not in registry)
    - Updated (revision in file > revision in registry)
    """
    registry = get_registry_state(conn)
    to_process = []

    for f in os.listdir(RAW_DIR):
        if not f.endswith(".json"):
            continue

        match_id = f.replace(".json", "")
        filepath = os.path.join(RAW_DIR, f)

        with open(filepath) as file:
            data = json.load(file)
            meta = data["meta"]
            revision = meta["revision"]

        if match_id not in registry:
            to_process.append(filepath)
        elif revision > registry[match_id]:
            to_process.append(filepath)

    print(f"{len(to_process)} files to process")
    return to_process

def db_setup():
    os.makedirs("data/local_db", exist_ok=True)
    conn=sqlite3.connect(DB_PATH)
    version= conn.execute('SELECT SQLITE_VERSION()')
    data = version.fetchone()
    print('SQLite version:', data,'')
    conn.execute("""
        CREATE TABLE IF NOT EXISTS registry (
        match_id TEXT primary key,
        revision     INTEGER,
        created      TEXT,
        data_version TEXT,
        ingested_at  TEXT )
                 """)
    conn.commit()
    print('DB and table ready\n')
    return conn

def get_registry_state(conn):
    """Returns dict of {match_id: revision} for quick lookup"""
    rows = conn.execute("SELECT match_id, revision FROM registry").fetchall()
    return {row[0]: row[1] for row in rows}

def update_registry(conn, files):
    """
    Upsert registry entries for all processed files.
    Called AFTER successful bronze write.
    """
    from datetime import datetime

    for filepath in files:
        with open(filepath) as f:
            data = json.load(f)
            meta = data["meta"]

        match_id = os.path.basename(filepath).replace(".json", "")

        conn.execute("""
            INSERT INTO registry (match_id, revision, created, data_version, ingested_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(match_id) DO UPDATE SET
                revision     = excluded.revision,
                ingested_at  = excluded.ingested_at
        """, (
            match_id,
            meta["revision"],
            meta["created"],
            meta["data_version"],
            datetime.now().isoformat()
        ))

    conn.commit()
    print(f"Registry updated for {len(files)} files")
    
def close_connection(conn):
    conn.close()
    print('Connection closed..')
    return None

