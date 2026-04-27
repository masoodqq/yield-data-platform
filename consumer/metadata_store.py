import sqlite3
import os

DB_PATH = "./metadata.db"

def init_db():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS lineage (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id      TEXT NOT NULL,
            run_id        TEXT NOT NULL,
            git_sha       TEXT NOT NULL,
            row_count     INTEGER NOT NULL,
            delta_version INTEGER NOT NULL,
            processed_at  TEXT NOT NULL
        )
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_run_id
        ON lineage(run_id)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_delta_version
        ON lineage(delta_version)
    """)
    conn.commit()
    conn.close()
    print(f"Metadata DB initialized at {DB_PATH}")

def insert_lineage(batch_id, run_id, git_sha, row_count, delta_version, processed_at):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO lineage
            (batch_id, run_id, git_sha, row_count, delta_version, processed_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (batch_id, run_id, git_sha, row_count, delta_version, processed_at))
    conn.commit()
    conn.close()

def query_by_run_id(run_id):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT run_id, git_sha, delta_version, row_count, processed_at
        FROM lineage
        WHERE run_id = ?
    """, (run_id,))
    rows = cursor.fetchall()
    conn.close()
    return rows

def query_by_delta_version(delta_version):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT run_id, git_sha, delta_version, row_count, processed_at
        FROM lineage
        WHERE delta_version = ?
    """, (delta_version,))
    rows = cursor.fetchall()
    conn.close()
    return rows