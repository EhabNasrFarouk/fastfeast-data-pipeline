import sys
import polars as pl
from pathlib import Path
from dataclasses import dataclass


# -------------------------- Handling Paths --------------------------
root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root))
from validation.validators import *
from config.config_loader import load_metadata
from validation.logger import logging_errors
from validation.pii_masking import pii_mask
from validation.logger import store_quarantine

RULES = {
    "not_null": null_validator,
    "unique": duplicate_validator,
    "data_types": data_type_validator,
    "range": range_validator,
    "regex": regex_validator,
    "allowed_values": allowed_values_validator,
}


@dataclass
class ValidationResult:
    valid_lf: pl.LazyFrame          # rows to load (clean + warning-only rows)
    quarantine_lf: pl.LazyFrame     # rows to quarantine (>=1 CRITICAL error)
    errors_lf: pl.LazyFrame         # full error log, both severities
    total_rows: int
    quarantined_count: int
    warning_count: int


# -------------------------- Main Function --------------------------
def validate_file(run_id: str, file_path: str, lf: pl.LazyFrame, file_type: str, source_table: str) -> ValidationResult:
    # Loading table metadata
    metadata = load_metadata()
    layer_key = file_type.strip().title()
    table_rules = metadata[layer_key][source_table]

    # Applying validations on the table
    error_frames = [empty_errors()]
    date_formats = table_rules["date_formats"] if "date_formats" in table_rules else {}
    for rule_key, validator_fn in RULES.items():
        if rule_key not in table_rules:
            continue

        if rule_key == "data_types":
            error_frames.append(validator_fn(lf, table_rules[rule_key], date_formats))
        else:
            error_frames.append(validator_fn(lf, table_rules[rule_key]))

    errors_lf = pl.concat(error_frames)

    # PII Masking
    lf = pii_mask(lf,layer_key,source_table)

    # Logging the errors
    logging_errors(run_id, file_path, file_type, source_table, errors_lf) 

    # Putting bad records in the quearntine
    critical_rows = (
        errors_lf.filter(pl.col("severity") == "CRITICAL")
        .select(["row_number", "column_name", "error_type", "invalid_value"])
    )
    quarantine_lf = lf.join(
        critical_rows.select(["row_number", "column_name", "error_type", "invalid_value"]),
        on="row_number",
        how="inner"
    )
    store_quarantine(quarantine_lf, file_path, run_id, file_type, source_table)

    # Updating Tracking Table (rows_cnt)

    # Showing Result
    # lf.collect().write_csv(f"data/{source_table}.csv")
    # errors_lf.collect().write_csv(f"data/{source_table}_errors.csv")

    # Returning the valid frame
    # valid_lf = lf.join(critical_rows, on="row_number", how="anti")
    # return valid_lf

    # ********************************************************************************
    # critical_rows = (
    #     errors_lf.filter(pl.col("severity") == "CRITICAL")
    #     .select("row_number").unique()
    # )

    # quarantine_lf = lf.join(critical_rows, on="row_number", how="semi")
    # valid_lf = lf.join(critical_rows, on="row_number", how="anti")

    # total_rows = lf.select(pl.len()).collect().item()
    # quarantined_count = critical_rows.select(pl.len()).collect().item()
    # warning_count = (
    #     errors_lf.filter(pl.col("severity") == "WARNING")
    #     .select("row_number").unique()
    #     .select(pl.len()).collect().item()
    # )

    # print(
    #     f"[{source_table}] {quarantined_count} quarantined (CRITICAL), "
    #     f"{warning_count} loaded-with-warnings, "
    #     f"{total_rows - quarantined_count} total will load."
    # )

    # return ValidationResult(
    #     valid_lf=valid_lf,
    #     quarantine_lf=quarantine_lf,
    #     errors_lf=errors_lf,
    #     total_rows=total_rows,
    #     quarantined_count=quarantined_count,
    #     warning_count=warning_count,
    # )
    
    # bad_row_numbers = errors_lf.select("row_number").unique()

    # valid_lf = lf.join(bad_row_numbers, on="row_number", how="anti")

    # total_rows = lf.select(pl.len()).collect().item()
    # error_row_count = bad_row_numbers.select(pl.len()).collect().item()
    # print(f"[{source_table}] Validation complete: {error_row_count} errors out of {total_rows} rows.")

    # return ValidationResult(
    #         valid_lf=valid_lf,
    #         errors_lf=errors_lf,
    #         total_rows=total_rows,
    #         error_row_count=error_row_count,
    #     )


# -------------------------------------------------------------------------------------------------------------------------------------------------
# file_path = "F:\\ITI\\17-Python\\New Project\\FastFeast\\fastfeast-data-pipeline\\data\\input\\batch\\2026-08-27\\agents.csv"
# file_path_json = "F:\\ITI\\17-Python\\New Project\\FastFeast\\fastfeast-data-pipeline\\data\\input\\stream\\2026-06-18\\17\\orders.json"

# md = load_metadata()
# validation_cols = md["Stream"]["orders"]["data_types"].keys()
# dynamic_overrides = {col_nm: pl.String for col_nm in validation_cols}

# ----------------------------- CSVS -----------------------------
# file_path = "F:\\ITI\\17-Python\\New Project\\FastFeast\\full-logging\\fastfeast-data-pipeline\\data\\input\\batch\\2026-06-14\\drivers.csv"
# validation_cols = md["Batch"]["drivers"]["data_types"].keys()

# excluded = ["float_to_int"]
# dynamic_overrides = {col_nm: pl.String for col_nm in validation_cols if col_nm not in excluded}
# lf = pl.scan_csv(file_path, schema_overrides=dynamic_overrides)

# lf = lf.with_columns([
#     pl.col(col_name).str.strip_chars() for col_name in dynamic_overrides.keys()
# ])

# lf = lf.with_row_index("row_number")

# validate_file("1111", file_path, lf, "Batch", "drivers")
# print(lf.collect())

# ----------------------------- JSON -----------------------------
# file_path_json = "F:\\ITI\\17-Python\\New Project\\FastFeast\\Integration\\fastfeast-data-pipeline\\data\\input\\stream\\2026-06-14\\21\\orders.json"
# validation_cols = md["Stream"]["orders"]["data_types"].keys()
# dynamic_overrides = {col_nm: pl.String for col_nm in validation_cols if col_nm != "date_format"}

# with open(file_path_json) as f: # This is to handle NaN problem.
#     clean_json_str = f.read().replace("NaN", "null")

# lf = (
#     pl.read_json(clean_json_str.encode(), schema_overrides=dynamic_overrides)
#     .lazy()
# )

# lf = lf.with_columns([
#     pl.col(col_name).str.strip_chars() for col_name in dynamic_overrides.keys()
# ])

# lf = lf.with_row_index("row_number")

# validate_file(lf, "Stream", "orders")

# print(lf.filter( (pl.col("row_number") == 85) | (pl.col("row_number") == 131) ).collect())
# print(lf.filter( pl.col("order_created_at").is_null() ).collect())