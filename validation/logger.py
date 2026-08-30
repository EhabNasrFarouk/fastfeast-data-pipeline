import sys
import polars as pl
from pathlib import Path
from datetime import datetime

# -------------------------- Handling Paths --------------------------
root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root))
from ingestion.file_tracker import get_connection

def logging_errors(run_id: str, file_path: str, file_type: str, source_table: str, errors_lf: pl.LazyFrame):
    conn = get_connection()

    df = errors_lf.collect()
    conn.execute("""
        INSERT INTO validation_log (
            run_id, filepath, file_type, source_table, severity, 
            record_idx, error_category, column_name, invalid_value, logged_at
        )
        SELECT 
            ? as run_id, 
            ? as filepath, 
            ? as file_type, 
            ? as source_table, 
            severity, 
            row_number as record_idx, -- Map polars column to db column name
            error_type as error_category, -- Map polars column to db column name
            column_name, 
            invalid_value, 
            ? as logged_at
        FROM df
    """, [run_id, file_path, file_type, source_table, datetime.now()])

    conn.close()