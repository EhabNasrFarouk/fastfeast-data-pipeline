# ingestion/batch/batch_ingestion.py
from logging import root
import os
from datetime import datetime
import sys
from pathlib import Path
from config.config_loader import load_config
from ingestion.file_tracker import new_run_id,get_unprocessed_files



def batch_ingest(pipeline_mng, config=None):

    root = Path(__file__).resolve().parent.parent.parent
    sys.path.insert(0, str(root))  
    config = config or load_config()
    batch_date = datetime.today().strftime("%Y-%m-%d")
    batch_dir = config["Ingestion"]["Batch"]
    batch_path = os.path.join(batch_dir, batch_date)


    # ------------------ Actions ------------------
    # Generating run_id.
    run_id = new_run_id()

    # Getting unprocessed files.
    files_paths = [ os.path.join(batch_path, f) for f in os.listdir(batch_path) ]
    batch_files = get_unprocessed_files(files_paths, batch_date)
    print("Hello")
    # Passing the files to the thread pool.
    for f in batch_files:
        pipeline_mng.submit_file(run_id, f, "Batch", batch_date)



if __name__ == "__main__":
    batch_ingest()