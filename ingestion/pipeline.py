import sys
import polars as pl
from pathlib import Path
from datetime import date

root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root))
from ingestion.file_tracker import start_file_tracking, change_file_phase
from ingestion.parser import read_file
from validation.file_validation import validate_file, ValidationResult


def process_file(run_id: str, file_path: str, file_type: str, run_date: date, run_hour=None):
    # Tracking Part
    phase = "Tracking Phase"
    source_table = file_path.split("\\")[-1].split(".")[0]
    start_file_tracking(file_path, run_id, phase, source_table, run_date, run_hour)


    # Parsing Part
    lf = read_file(file_path, run_id, source_table)
    if lf is None:
        return
    else:
        print(f"Parsing Phase for {source_table} table.", "\n------------------------------------\n")
        change_file_phase(run_id, source_table, "Parsing")

    # Validation Part
    validate_file(lf, file_type, source_table)
    change_file_phase(run_id, source_table, "Validation")
    print(f"Validation for {source_table}")
    print("-" * 20)
 
    
    # Creating the file
    # write_validation(v_rs.errors_lf, run_id, file_path, file_type, source_table)
    # try:
    #     v_rs.errors_lf.collect().write_csv(f"{source_table}.csv")
    # except Exception as e:
    #     print(e)