import sys
import polars as pl
from pathlib import Path
from dataclasses import dataclass


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
    valid_lf: pl.LazyFrame
    errors_lf: pl.LazyFrame
    total_rows: int
    error_row_count: int


# -------------------------- Main Function --------------------------
def validate_file(lf: pl.LazyFrame, file_type: str, source_table: str) -> ValidationResult:
    # print(f"Validating Phase for {source_table} table.", "\n------------------------------------\n")

    metadata = load_metadata()
    layer_key = file_type.strip().title()

    table_rules = metadata[layer_key][source_table]
 
    error_frames = [empty_errors()]
    for rule_key, validator_fn in RULES.items():
        if rule_key not in table_rules:
            continue
        
        error_frames.append(validator_fn(lf, table_rules[rule_key]))

    errors_lf = pl.concat(error_frames)

    errors_lf = pl.concat(error_frames)
    
    bad_row_numbers = errors_lf.select("row_number").unique()

    valid_lf = lf.join(bad_row_numbers, on="row_number", how="anti")

    total_rows = lf.select(pl.len()).collect().item()
    error_row_count = bad_row_numbers.select(pl.len()).collect().item()
    print(f"[{source_table}] Validation complete: {error_row_count} errors out of {total_rows} rows.")

    return ValidationResult(
            valid_lf=valid_lf,
            errors_lf=errors_lf,
            total_rows=total_rows,
            error_row_count=error_row_count,
        )


# -------------------------------------------------------------------------------------------------------------------------------------------------
# file_path = "F:\\ITI\\17-Python\\New Project\\FastFeast\\fastfeast-data-pipeline\\data\\input\\batch\\2026-06-17\\customers.csv"
# file_path_json = "F:\\ITI\\17-Python\\New Project\\FastFeast\\fastfeast-data-pipeline\\data\\input\\stream\\2026-06-18\\20\\orders.json"

# md = load_metadata()
# validation_cols = md["Batch"]["customers"]["data_types"].keys()
# dynamic_overrides = {col_nm: pl.String for col_nm in validation_cols}

# ----------------------------- CSVS -----------------------------
# lf = pl.scan_csv(file_path, schema_overrides=dynamic_overrides).with_row_index("row_number")
# print(lf.collect())


# ----------------------------- JSON -----------------------------
# with open(file_path_json) as f: # This is to handle NaN problem.
#     clean_json_str = f.read().replace("NaN", "null")

# lf = pl.read_json(clean_json_str.encode()).lazy().with_row_index("row_number")
# order_id = "392d6248-2aaf-40b7-9759-f534403a7fc8"
# print( lf.filter(pl.col("order_id") == order_id).collect() )

# ----------------------------------------------------------------------------------------
# validate_file(lf, "Batch", "customers")


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