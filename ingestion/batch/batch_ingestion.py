# batch/batch_ingestion.py
import os
from datetime import datetime

def get_batch_files(batch_path: str) -> list:
    supported = {".csv", ".json"}
    if not os.path.exists(batch_path):
        print(f"[BATCH] Folder not found: {batch_path}")
        return []
    return [
        os.path.join(batch_path, f) for f in os.listdir(batch_path) if os.path.splitext(f)[1] in supported
    ]

def run_batch_pipeline():
    batch_date = datetime.today().strftime("%Y-%m-%d")
    batch_path = f"data/input/batch/{batch_date}/"
    print(f"[BATCH] Running pipeline for {batch_date}")
    files = get_batch_files(batch_path)
    for f in files:
        print(f"[BATCH] Processing → {f}")