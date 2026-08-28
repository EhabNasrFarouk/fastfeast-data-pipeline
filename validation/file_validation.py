import sys
import polars as pl
from pathlib import Path
from dataclasses import dataclass
from validation.pii_masking import pii_mask
from validation.quarantine_writer import store_quarantine
# -------------------------- Handling Paths --------------------------
root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root))
from validation.validators import *
from config.config_loader import load_metadata


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
def validate_file(lf: pl.LazyFrame, file_type: str, source_table: str) -> ValidationResult:
    metadata = load_metadata()
    layer_key = file_type.strip().title()
    table_rules = metadata[layer_key][source_table]

    error_frames = [empty_errors()]
    for rule_key, validator_fn in RULES.items():
        if rule_key not in table_rules:
            continue
        error_frames.append(validator_fn(lf, table_rules[rule_key]))

    errors_lf = pl.concat(error_frames)

    critical_rows = (
        errors_lf.filter(pl.col("severity") == "CRITICAL")
        .select("row_number").unique()
    )

    quarantine_lf = lf.join(critical_rows, on="row_number", how="semi")
    valid_lf = lf.join(critical_rows, on="row_number", how="anti")

    total_rows = lf.select(pl.len()).collect().item()
    quarantined_count = critical_rows.select(pl.len()).collect().item()
    warning_count = (
        errors_lf.filter(pl.col("severity") == "WARNING")
        .select("row_number").unique()
        .select(pl.len()).collect().item()
    )

    print(
        f"[{source_table}] {quarantined_count} quarantined (CRITICAL), "
        f"{warning_count} loaded-with-warnings, "
        f"{total_rows - quarantined_count} total will load."
    )

    try:
        lf_with_pii_masked = pii_mask(quarantine_lf,layer_key,source_table)
        print(lf_with_pii_masked.collect())
    except Exception as e:
        print(f"Error during PII masking: {e}")

    store_quarantine()
    return ValidationResult(
        valid_lf=valid_lf,
        quarantine_lf=quarantine_lf,
        errors_lf=errors_lf,
        total_rows=total_rows,
        quarantined_count=quarantined_count,
        warning_count=warning_count,
    )
    
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
# file_path = "F:\\ITI\\17-Python\\New Project\\FastFeast\\fastfeast-data-pipeline\\data\\input\\batch\\2026-06-17\\customers.csv"
# file_path_json = "F:\\ITI\\17-Python\\New Project\\FastFeast\\fastfeast-data-pipeline\\data\\input\\stream\\2026-06-18\\17\\orders.json"

# md = load_metadata()
# validation_cols = md["Stream"]["orders"]["data_types"].keys()
# dynamic_overrides = {col_nm: pl.String for col_nm in validation_cols}

# ----------------------------- CSVS -----------------------------
# file_path = "F:\\ITI\\17-Python\\New Project\\FastFeast\\Integration\\fastfeast-data-pipeline\\data\\input\\stream\\2026-06-14\\21\\tickets.csv"
# validation_cols = md["Stream"]["tickets"]["data_types"].keys()

# excluded = ["date_format", "float_to_int"]
# dynamic_overrides = {col_nm: pl.String for col_nm in validation_cols if col_nm not in excluded}
# lf = pl.scan_csv(file_path, schema_overrides=dynamic_overrides)

# lf = lf.with_columns([
#     pl.col(col_name).str.strip_chars() for col_name in dynamic_overrides.keys()
# ])

# lf = lf.with_row_index("row_number")

# validate_file(lf, "Stream", "tickets")
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

# =====================================================================================================
# import polars as pl
# from pathlib import Path
# from dataclasses import dataclass
# from config.config_loader import load_config_tables
# from validation.validators import (
#     null_validator,
#     duplicate_validator,
#     data_type_validator,
#     range_validator,
#     regex_validator,
#     allowed_values_validator,
#     empty_errors,
# )

# root = Path(__file__).resolve().parent.parent
# sys.path.insert(0, str(root))

# RULES = {
#     "not_null": null_validator,
#     "unique": duplicate_validator,
#     "data_types": data_type_validator,
#     "range": range_validator,
#     "regex": regex_validator,
#     "allowed_values": allowed_values_validator,
# }


# @dataclass
# class ValidationResult:
#     valid_lf: pl.LazyFrame
#     errors_lf: pl.LazyFrame
#     total_rows: int
#     error_row_count: int


# def validate_file(lf: pl.LazyFrame, layer: str, source_table: str) -> ValidationResult:

#     metadata = load_config_tables()
#     layer_key = layer.strip().title()

#     table_rules = metadata[layer_key][source_table]
 
#     error_frames = [empty_errors()]
#     for rule_key, validator_fn in RULES.items():
#         if rule_key not in table_rules:
#             continue
#         error_frames.append(validator_fn(lf, table_rules[rule_key]))

#     errors_lf = pl.concat(error_frames)


#     bad_row_numbers = errors_lf.select("row_number").unique()

#     valid_lf = lf.join(bad_row_numbers, on="row_number", how="anti")

#     total_rows = lf.select(pl.len()).collect().item()
#     error_row_count = bad_row_numbers.select(pl.len()).collect().item()
#     print(f"[{source_table}] Validation complete: {error_row_count} errors out of {total_rows} rows.")
#     return ValidationResult(
#         valid_lf=valid_lf,
#         errors_lf=errors_lf,
#         total_rows=total_rows,
#         error_row_count=error_row_count,
#     )