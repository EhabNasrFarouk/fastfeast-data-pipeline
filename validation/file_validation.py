import sys
import json
import yaml
import polars as pl
from pathlib import Path

# -------------------------- Handling Paths --------------------------
root = Path(__file__).resolve().parent.parent
metadata_path = root / "config" / "tables_metadata.yaml"


# -------------------------- Validators --------------------------
def null_validator(lf: pl.LazyFrame, columns: list[str]):
    error_frames = []
      
    for col_nm in columns:
        errors = (
            lf.filter(pl.col(col_nm).is_null())
                .select(
                    "row_number",
                    pl.lit(col_nm).alias("column_name"),
                    pl.lit("NULL_VALUE").alias("error_type"),
                    pl.col(col_nm).cast(pl.String).alias("invalid_value")
                )
        )
        error_frames.append(errors)

    rs = pl.concat(error_frames)
    print(rs.collect())

def duplicate_validator(lf: pl.LazyFrame, columns: list[str]):
    error_frames = []
           
    for col_nm in columns:
        errors = (
            lf.filter(
                    pl.col(col_nm).is_duplicated() &
                    ~pl.col(col_nm).is_first_distinct()
                )
                .select(
                    "row_number",
                    pl.lit(col_nm).alias("column_name"),
                    pl.lit("DUPLICATE_KEY").alias("error_type"),
                    pl.col(col_nm).cast(pl.String).alias("invalid_value")
                )
        )
        error_frames.append(errors)

    rs = pl.concat(error_frames)
    print(rs.collect())

def data_type_validator(lf: pl.LazyFrame, columns: dict):
    error_frames = []
    STR_TO_DTYPE = {
        "pl.Int64": pl.Int64,
        "pl.Utf8": pl.String,
        "pl.Boolean": pl.Boolean,
        "pl.Date": pl.Date,
        "pl.Timestamp": pl.Datetime,
        "pl.Decimal": pl.Decimal
    }
                
    for col_nm, data_type in columns.items():
        #  print(col_nm, data_type)
        errors = (
            lf.filter(
                    pl.col(col_nm).is_not_null() &
                    pl.col(col_nm).cast(STR_TO_DTYPE[data_type], strict=False).is_null()
                )
                .select(
                    "row_number",
                    pl.lit(col_nm).alias("column_name"),
                    pl.lit("INVALID_DTYPE").alias("error_type"),
                    pl.col(col_nm).cast(pl.String).alias("invalid_value")
                )
        )
        error_frames.append(errors)

    rs = pl.concat(error_frames)
    print(rs.collect())
     

# -------------------------- Main Function --------------------------
def process_data(lf: pl.LazyFrame, type: str, source_table: str):    
    # Reading metadta
    with open(metadata_path, 'r') as f:
        md = yaml.safe_load(f)

    # null_validator(lf, md[type][source_table]["not_null"])
    # duplicate_validator(lf, md[type][source_table]["unique"])
    data_type_validator(lf, md[type][source_table]["data_types"])

    # print(md[type][source_table]["not_null"])
    # print("-" * 30)
    # print(yaml.dump(md[type][source_table], indent=4, sort_keys=False))

    # 2- Looping over the table
    pass


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
