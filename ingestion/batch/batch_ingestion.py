# ingestion/batch/batch_ingestion.py
import os
from datetime import datetime
from ingestion.file_tracker import (
    get_connection, init_tracker_table, new_run_id,
    get_unprocessed_files, start_file_tracking, mark_processed
)

SUPPORTED_EXTENSIONS = {".csv", ".json"}


def get_batch_files(batch_path: str) -> list:
    if not os.path.exists(batch_path):
        print(f"[BATCH] Folder not found: {batch_path}")
        return []
    return [
        os.path.join(batch_path, f) for f in os.listdir(batch_path)
        if os.path.splitext(f)[1] in SUPPORTED_EXTENSIONS
    ]


def infer_source_table(filepath: str) -> str:
    """Derive table name from filename, e.g. 'customers.csv' -> 'customers'."""
    return os.path.splitext(os.path.basename(filepath))[0]


def process_file(filepath, run_id, run_date, con):
    """
    Process a single batch file. Returns (status, rows_read, rows_inserted, rows_rejected, error_message).
    Replace the body with real load/transform logic.
    """
    source_table = infer_source_table(filepath)

    file_id = start_file_tracking(
        filepath=filepath,
        run_id=run_id,
        layer="batch",
        source_table=source_table,
        run_date=run_date,
        con=con
    )

    try:
        # ------------------------------------------------------------
        # TODO: real logic here
        #   - read file (pandas/duckdb)
        #   - validate / clean
        #   - load into warehouse table
        # ------------------------------------------------------------
        rows_read = 0
        rows_inserted = 0
        rows_rejected = 0

        mark_processed(
            file_id, status="success",
            rows_read=rows_read, rows_inserted=rows_inserted, rows_rejected=rows_rejected,
            con=con
        )
        return "success", rows_read, rows_inserted, rows_rejected, None

    except Exception as e:
        mark_processed(file_id, status="failed", error_message=str(e), con=con)
        return "failed", 0, 0, 0, str(e)


def run_batch_pipeline():
    batch_date = datetime.today().strftime("%Y-%m-%d")
    batch_path = f"data/input/batch/{batch_date}/"

    run_id = new_run_id()
    print(f"[BATCH] Run {run_id} — pipeline for {batch_date}")

    con = get_connection()
    init_tracker_table(con)

    files = get_batch_files(batch_path)
    new_files = get_unprocessed_files(files, run_date=batch_date, con=con)

    if not new_files:
        print("[BATCH] No new or changed files to process.")
        con.close()
        return

    succeeded, failed = 0, 0

    for f in new_files:
        print(f"[BATCH] Processing → {f}")
        status, rows_read, rows_inserted, rows_rejected, err = process_file(
            f, run_id=run_id, run_date=batch_date, con=con
        )

        if status == "success":
            succeeded += 1
            print(f"[BATCH]   OK  rows_read={rows_read} inserted={rows_inserted} rejected={rows_rejected}")
        else:
            failed += 1
            print(f"[BATCH]   FAILED → {err}")

    con.close()

    print(f"[BATCH] Run {run_id} done. {succeeded} succeeded, {failed} failed (of {len(new_files)}).")


if __name__ == "__main__":
    run_batch_pipeline()