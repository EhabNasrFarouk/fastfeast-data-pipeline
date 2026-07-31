import sys
import json
import yaml
import polars as pl
from pathlib import Path

# -------------------------- Handling Paths --------------------------
root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root))
from validation.validators import *

metadata_path = root / "config" / "tables_metadata.yaml"


test = {
    "not_null": null_validator 
}

# -------------------------- Main Function --------------------------
def process_data(lf: pl.LazyFrame, type: str, source_table: str):    
    # Reading metadta
    with open(metadata_path, 'r') as f:
        md = yaml.safe_load(f)

    # md[type][source_table]

    null_errors = null_validator(lf, md[type][source_table]["not_null"])
    duplicate_errors = duplicate_validator(lf, md[type][source_table]["unique"])
    data_type_errors = data_type_validator(lf, md[type][source_table]["data_types"])
    regex_errors = regex_validator(lf, md[type][source_table]["regex"])
    allowed_values_errors = allowed_values_validator(lf, md[type][source_table]["allowed_values"])
    # range_errors = range_validator(lf, md[type][source_table]["range"])

    all_errors = pl.concat([null_errors, duplicate_errors, data_type_errors, regex_errors,
                           allowed_values_errors])

    all_errors.collect().write_csv("output.csv")

    # print(md[type][source_table]["not_null"])
    # print("-" * 30)
    # print(yaml.dump(md[type][source_table], indent=4, sort_keys=False))

    # 2- Looping over the table
    # pass


# -------------------------------------------------------------------------------------------------------------------------------------------------
file_path = "F:\\ITI\\17-Python\\New Project\\FastFeast\\fastfeast-data-pipeline\\data\\input\\batch\\2026-06-17\\customers.csv"
file_path_json = "F:\\ITI\\17-Python\\New Project\\FastFeast\\fastfeast-data-pipeline\\data\\input\\stream\\2026-06-18\\20\\orders.json"

with open(metadata_path, 'r') as f:
    md = yaml.safe_load(f)
validation_cols = md["Batch"]["customers"]["data_types"].keys()
dynamic_overrides = {col_nm: pl.String for col_nm in validation_cols}

# ----------------------------- CSVS -----------------------------
lf = pl.scan_csv(file_path, schema_overrides=dynamic_overrides).with_row_index("row_number")
# print(lf.collect())


# ----------------------------- JSON -----------------------------
# with open(file_path_json) as f: # This is to handle NaN problem.
#     clean_json_str = f.read().replace("NaN", "null")

# lf = pl.read_json(clean_json_str.encode()).lazy().with_row_index("row_number")
# order_id = "392d6248-2aaf-40b7-9759-f534403a7fc8"
# print( lf.filter(pl.col("order_id") == order_id).collect() )

# ----------------------------------------------------------------------------------------
process_data(lf, "Batch", "customers")
