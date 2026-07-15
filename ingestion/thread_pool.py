import concurrent.futures
import sys
from pathlib import Path
from datetime import date

root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(root))
from ingestion.pipeline import process_file

class PipelineManager:
    def __init__(self, max_workers=5):
        # The executor automatically manages the queue and the threads
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        
    def submit_file(self, run_id: str, file_path: str, file_type: str, run_date: date, run_hour: int = None):
        """Watchers call this to hand off a file to the pool."""
        # submit() puts the function and its arguments into the internal queue
        # and a free thread will pick it up automatically.
        self.executor.submit(process_file, run_id, file_path, file_type, run_date, run_hour)

    def shutdown(self):
        """Safely close the pool when the program exits."""
        print("Shutting down thread pool...")
        self.executor.shutdown(wait=True)