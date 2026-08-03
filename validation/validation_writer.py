import sys
import csv
import polars as pl
from pathlib import Path
from datetime import datetime

root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root))
from config.config_loader import load_config

FIELDNAMES = [
    "run_id", "file_id", "layer", "source_table",
    "row_number", "column_name", "error_type", "invalid_value", "recorded_at",
]


def validation_csv_path() -> Path:
    cfg = load_config()
    validation_dir = Path(cfg.get("paths", {}).get("validation_dir", "data/validation"))
    validation_dir.mkdir(parents=True, exist_ok=True)
    return validation_dir / "validation_errors.csv"


def write_validation(
    errors_df: pl.DataFrame,
    run_id: str,
    file_id: str,
    layer: str,
    source_table: str,
) -> None:
    print(f"Writing Phase for {source_table} table.", "\n------------------------------------\n")

    if errors_df.height == 0:
        return

    path = validation_csv_path()
    file_exists = path.exists() and path.stat().st_size > 0
    now = datetime.now().isoformat(timespec="seconds")
    print(f"[{source_table}] Writing {errors_df.height} validation errors to {path}.")
    with open(path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        if not file_exists:
            writer.writeheader()
        for row in errors_df.iter_rows(named=True):
            writer.writerow({
                "run_id": run_id,
                "file_id": file_id,
                "layer": layer,
                "source_table": source_table,
                "row_number": row["row_number"],
                "column_name": row["column_name"],
                "error_type": row["error_type"],
                "invalid_value": row["invalid_value"],
                "recorded_at": now,
            })
