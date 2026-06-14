# ingestion/file_tracker.py
import os
import uuid
import hashlib
import duckdb
from datetime import datetime

DB_PATH = "warehouse/fastfeast.duckdb"
PIPELINE_VERSION = "0.1.0"  # bump manually or wire to git commit hash


def _ensure_db_dir():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)


def get_connection():
    """Return a DuckDB connection to the warehouse database."""
    _ensure_db_dir()
    return duckdb.connect(DB_PATH)


def init_tracker_table(con=None):
    """Create the file-tracking table if it doesn't exist."""
    close_after = con is None
    con = con or get_connection()

    con.execute("""
        CREATE TABLE IF NOT EXISTS etl_file_tracker (
            file_id          VARCHAR PRIMARY KEY,
            run_id           VARCHAR,
            filepath         VARCHAR,
            file_hash        VARCHAR,
            file_size_bytes  BIGINT,
            layer            VARCHAR,
            source_table     VARCHAR,
            run_date         DATE,
            run_hour         INTEGER,
            status           VARCHAR,
            rows_read        BIGINT,
            rows_inserted    BIGINT,
            rows_rejected    BIGINT,
            error_message    VARCHAR,
            retry_count      INTEGER DEFAULT 0,
            pipeline_version VARCHAR,
            started_at       TIMESTAMP,
            processed_at      TIMESTAMP,
            duration_ms      BIGINT
        )
    """)

    if close_after:
        con.close()


def new_run_id():
    """Generate a new unique run id for a pipeline execution."""
    return str(uuid.uuid4())


def file_hash(filepath, chunk_size=8192):
    """Compute an MD5 hash of a file's contents."""
    h = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def make_file_id(filepath, run_date, run_hour=None):
    """Build a stable file_id from path + business date (+ hour)."""
    key = f"{filepath}|{run_date}|{run_hour if run_hour is not None else ''}"
    return hashlib.md5(key.encode()).hexdigest()


def is_processed(filepath, run_date, run_hour=None, con=None):
    """Check if a file was already successfully processed (by id + content hash)."""
    close_after = con is None
    con = con or get_connection()

    file_id = make_file_id(filepath, run_date, run_hour)

    result = con.execute(
        "SELECT file_hash, status FROM etl_file_tracker WHERE file_id = ?",
        [file_id]
    ).fetchone()

    if close_after:
        con.close()

    if result is None:
        return False

    stored_hash, status = result
    if status != "success":
        return False

    return stored_hash == file_hash(filepath)


def get_unprocessed_files(filepaths, run_date, run_hour=None, con=None):
    """Return only files that haven't been successfully processed yet (or changed since)."""
    close_after = con is None
    con = con or get_connection()

    result = [
        f for f in filepaths
        if not is_processed(f, run_date, run_hour, con=con)
    ]

    if close_after:
        con.close()

    return result


def start_file_tracking(filepath, run_id, layer, source_table, run_date, run_hour=None, con=None):
    """Insert/update a 'pending' record at the start of processing a file."""
    close_after = con is None
    con = con or get_connection()

    file_id = make_file_id(filepath, run_date, run_hour)

    con.execute("""
        INSERT INTO etl_file_tracker
            (file_id, run_id, filepath, file_hash, file_size_bytes, layer,
             source_table, run_date, run_hour, status, pipeline_version, started_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)
        ON CONFLICT (file_id) DO UPDATE SET
            run_id = excluded.run_id,
            file_hash = excluded.file_hash,
            file_size_bytes = excluded.file_size_bytes,
            status = 'pending',
            pipeline_version = excluded.pipeline_version,
            started_at = excluded.started_at,
            retry_count = etl_file_tracker.retry_count + 1
    """, [
        file_id, run_id, filepath, file_hash(filepath), os.path.getsize(filepath),
        layer, source_table, run_date, run_hour, PIPELINE_VERSION, datetime.now()
    ])

    if close_after:
        con.close()

    return file_id


def mark_processed(file_id, status="success", rows_read=0, rows_inserted=0,
                    rows_rejected=0, error_message=None, con=None):
    """Update the tracking record once a file has finished processing."""
    close_after = con is None
    con = con or get_connection()

    row = con.execute(
        "SELECT started_at FROM etl_file_tracker WHERE file_id = ?", [file_id]
    ).fetchone()

    now = datetime.now()
    duration_ms = None
    if row and row[0] is not None:
        duration_ms = int((now - row[0]).total_seconds() * 1000)

    con.execute("""
        UPDATE etl_file_tracker
        SET status = ?,
            rows_read = ?,
            rows_inserted = ?,
            rows_rejected = ?,
            error_message = ?,
            processed_at = ?,
            duration_ms = ?
        WHERE file_id = ?
    """, [
        status, rows_read, rows_inserted, rows_rejected,
        error_message, now, duration_ms, file_id
    ])

    if close_after:
        con.close()


def get_processing_history(layer=None, run_id=None, con=None):
    """Return tracker rows, optionally filtered by layer and/or run_id."""
    close_after = con is None
    con = con or get_connection()

    query = "SELECT * FROM etl_file_tracker WHERE 1=1"
    params = []

    if layer:
        query += " AND layer = ?"
        params.append(layer)

    if run_id:
        query += " AND run_id = ?"
        params.append(run_id)

    query += " ORDER BY processed_at DESC"

    df = con.execute(query, params).fetchdf()

    if close_after:
        con.close()

    return df