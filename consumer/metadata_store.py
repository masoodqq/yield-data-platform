import sqlite3
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from config import DB_PATH
except ImportError:
    DB_PATH = "./metadata.db"


def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
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
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_run_id ON lineage(run_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_delta_version ON lineage(delta_version)"
        )
        conn.commit()
    print(f"Metadata DB initialized at {DB_PATH}")


def insert_lineage(batch_id, run_id, git_sha, row_count, delta_version, processed_at):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            INSERT INTO lineage
                (batch_id, run_id, git_sha, row_count, delta_version, processed_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (batch_id, run_id, git_sha, row_count, delta_version, processed_at))
        conn.commit()


def query_by_run_id(run_id):
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT run_id, git_sha, delta_version, row_count, processed_at
            FROM lineage WHERE run_id = ?
        """, (run_id,))
        return cursor.fetchall()


def query_by_delta_version(delta_version):
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT run_id, git_sha, delta_version, row_count, processed_at
            FROM lineage WHERE delta_version = ?
        """, (delta_version,))
        return cursor.fetchall()


def query_all(limit=20):
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT run_id, git_sha, delta_version, row_count, processed_at
            FROM lineage
            ORDER BY id DESC
            LIMIT ?
        """, (limit,))
        return cursor.fetchall()