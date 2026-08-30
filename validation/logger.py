import sys
import json 
import polars as pl
from pathlib import Path
from datetime import datetime

# -------------------------- Handling Paths --------------------------
root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root))
from ingestion.file_tracker import get_connection


# -------------------------- Validation Logs --------------------------
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


# -------------------------- Quarntine --------------------------
def build_reasons(quarantine_lf: pl.LazyFrame) -> pl.LazyFrame:
    # 1. Identity the metadata columns we want to exclude from the merged column
    exclude_cols = ["row_number", "column_name", "error_type", "invalid_value"]
    
    # 2. Build the reason text and pack all other columns into a JSON string
    processed_lf = quarantine_lf.with_columns([
        (
            pl.col("column_name") 
            + ": "
            + pl.col("error_type")
            + " ("
            + pl.col("invalid_value").cast(pl.String).fill_null("NULL")
            + ")"
        ).alias("reason"),
        
        # Select all columns EXCEPT the ones in exclude_cols, pack to struct, convert to JSON
        pl.struct(pl.all().exclude(exclude_cols)).struct.json_encode().alias("extra_data")
    ])
    
    # 3. Group by row_number, join the reasons, and grab the first instance of extra_data
    errors = (
        processed_lf.group_by("row_number")
        .agg([
            pl.col("reason").str.join("; ").alias("reasons"),
            pl.col("extra_data").first()  # Since extra_data is identical for the same row_number
        ])
    )

    return errors


def store_quarantine(quarantine_lf: pl.LazyFrame, file_path: str, run_id: str, file_type: str, source: str) -> int:
    conn = get_connection()
   
    df = build_reasons(quarantine_lf).collect()
    conn.execute("""
        INSERT INTO quarantine (
            run_id, file_path, file_type, source, record_idx, record_raw, quarantine_reason, quarantined_at
        )
        SELECT 
            ? as run_id, 
            ? as file_path, 
            ? as file_type, 
            ? as source, 
            row_number,
            extra_data,
            reasons, 
            ? as quarantined_at
        FROM df
    """, [run_id, file_path, file_type, source, datetime.now()])

    conn.close()