import sys
from pathlib import Path

root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root))
from ingestion.file_tracker import start_file_tracking


def process_file(run_id, file_path, file_type, run_date, run_hour=None):
    # Tracking Part
    layer = "Tracking Phase"
    source_table = file_path.split("\\")[-1].split(".")[0]
    start_file_tracking(file_path, run_id, layer, source_table, run_date, run_hour)

    # Testing
    # print(f"run_id: {run_id}")
    # print(f"file_path: {file_path}")
    # print(f"file_type: {file_type}")
    # print(f"run_date: {run_date}")
    # print(f"run_hour: {run_hour}")
    # print(f"source_table: {source_table}")
    # print(f"layer: {layer}")
    # print("-" * 30)


    # Parsing Part