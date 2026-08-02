import sys
from pathlib import Path

root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root))
<<<<<<< Updated upstream
from ingestion.file_tracker import start_file_tracking


def process_file(run_id, file_path, file_type, run_date, run_hour=None):
    # Tracking Part
    layer = "Tracking Phase"
    source_table = file_path.split("\\")[-1].split(".")[0]
    start_file_tracking(file_path, run_id, layer, source_table, run_date, run_hour)
=======
from ingestion.file_tracker import start_file_tracking, mark_processed
from ingestion.parser import read_file
from validation.file_validation import validate_file
from validation.validation_writer import write_validation


def process_file(run_id: str, file_path: str, file_type: str, run_date: date, run_hour=None):
    # ------------------ Tracking Phase ------------------
    layer = "Tracking Phase"
    source_table = Path(file_path).stem
    # ***** We must add the file_type in the table *****
    file_id = start_file_tracking(file_path, run_id, layer, source_table, run_date, run_hour)
>>>>>>> Stashed changes

    # ------------------ Parsing Phase ------------------
    lf = read_file(file_path, run_id, source_table)

    if lf is None:
        # mark_processed(file_id, status="failed", rows_read=0, rows_inserted=0, rows_rejected=0)
        return

<<<<<<< Updated upstream
    # Parsing Part
=======
    # ------------------ Validation Phase ------------------

    # print("aaaaaaaaaaaaaaa")
    result = validate_file(lf, layer=file_type, source_table=source_table)
    # print("b"*20)

    valid_df = result.valid_lf.collect()
    errors_df = result.errors_lf.collect()

    if result.error_row_count > 0:
        write_validation(
            errors_df=errors_df,
            run_id=run_id,
            file_id=file_id,
            layer=file_type,
            source_table=source_table,
        )
>>>>>>> Stashed changes
