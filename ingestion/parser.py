import polars as pl
import sys
import json
import io
from pathlib import Path

root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root))
from ingestion.file_tracker import get_connection

file_not_found = "FileNotFoundError"
corrupted_file = "ComputeError"
invalid_format = "InvalidFormat"


def handle_error(run_id: str, source_table: str, error_msg):
    # Opening connection
    conn = get_connection()

    # updating the file [status: "failed", error_message, retry_count++]
    conn.execute("""
        UPDATE etl_file_tracker
        SET status = 'failed', retry_count = retry_count + 1, error_message = ?
        WHERE run_id = ? AND source_table = ?;
    """, [error_msg, run_id, source_table])

    # Closing connection 
    conn.close()


def read_file(file_path: str, run_id: str, source_table: str) -> pl.LazyFrame | None:
    path = Path(file_path)

    try:
        match path.suffix.lower():
            case ".csv":
                # infer_schema=False reads every column as a string. Dirty
                # values (e.g. a non-numeric phone number, a malformed date)
                # then flow through as strings instead of crashing the scan,
                # so the validators get a chance to flag them as rejected
                # rows instead of the whole file failing to parse.
                return pl.scan_csv(file_path, infer_schema=False).with_row_index("row_number")
            
            case ".json":
                with open(file_path) as f: # This is to handle NaN problem.
                    clean_json_str = f.read().replace("NaN", "null")
                df = pl.read_json(io.BytesIO(clean_json_str.encode()))
                # Same reasoning as CSV: cast every column to string so
                # malformed values are caught by the validators, not by a
                # failed parse.
                df = df.with_columns(pl.all().cast(pl.String))
                return df.lazy().with_row_index("row_number")
            
            case _:
                # The file type isn't supported [InvalidFormat]
                handle_error(run_id, source_table, invalid_format)
                return None
            
    except Exception as e:
        # The file is corrupted or doesn't exist [ComputeError | FileNotFoundError]
        error_msg = ""
        if (type(e).__name__ == corrupted_file):
            error_msg = "This file is corrupted."
        elif (type(e).__name__ == file_not_found):
            error_msg = "This file does not exist in this path."
        else:
            error_msg = type(e).__name__

        handle_error(run_id, source_table, error_msg)
        print(type(e).__name__)
        return None


# lf = read_file("F:\\ITI\\17-Python\\New Project\\FastFeast\\fastfeast-data-pipeline\\data\\input\\stream\\2026-06-18\\13\\orders.json", "3d56aafc-36f4-4d09-95d5-d871229ac81f", "orders")
# print(lf.collect())
# print(lf)
# handle_error("3d56aafc-36f4-4d09-95d5-d871229ac81f", "orders", "ERROR!")