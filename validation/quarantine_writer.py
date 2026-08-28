import duckdb
import polars as pl
from config.config_loader import load_config
import datetime
import json 
TABLE_NAME = "quarantine"

def get_connection():
    db_path = load_config()["database"]["duckdb"]["path"]

    return duckdb.connect(db_path)

def init_quarantine_table(con):

    con.execute("""
        CREATE SEQUENCE IF NOT EXISTS quarantine_id START 1
        """
    )

    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id                INTEGER PRIMARY KEY DEFAULT nextval("quarantine_id"),
            file_path         VARCHAR,
            file_hash         VARCHAR,
            file_type         VARCHAR,
            source            VARCHAR,
            record_id         VARCHAR,
            record_raw        VARCHAR,
            quarantine_reason VARCHAR,
            quarantined_at    TIMESTAMP

        )
        """
    )

def build_reasons(quarantine_lf: pl.LazyFrame) -> dict[int,str]:

    errors = quarantine_lf.with_columns(
        pl.col("column_name") 
        + ": "
        + pl.col("error_type")
        + " ("
        + pl.col("invalid_value").cast(pl.String).fill_null("NULL")
        + ")"
    ).alias("reason") \
    .group_by("row_number") \
    .agg(pl.col("reason").str.join(";")) \
    .collect()

    return dict(errors.iter_rows)


def store_quarantine(quarantine_lf: pl.LazyFrame,file_path: str,file_hash: str,
                     file_type: str,source: str,con=None) -> int:

   con  = con if con is None  else get_connection()

   try:

       init_quarantine_table()

       reasons = build_reasons(quarantine_lf.collect())
       quarantined_at = datetime.now()
       records = []

       for row in quarantine_lf.iter_rows(named = True):

            record_id = quarantine_lf.pop("row_number")
           
            records.append(
                (
                     file_path,
                    file_hash,
                    file_type,
                    source,
                    record_id,
                    json.dumps(row,default=str),
                    reasons.get(record_id,{}),
                    quarantined_at
                )
            )

       con.executemany(
            f"""
            INSERT INTO  {TABLE_NAME} (
               file_path,
                file_hash,
                file_type,
                source,
                record_id,
                record_raw,
                quarantine_reason,
                quarantined_at
            )
            VALUES(?,?,?,?,?,?,?,?)
            """
        , records)    
       return len(records)
   finally:
       con.close()