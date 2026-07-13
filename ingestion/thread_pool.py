import concurrent.futures
from ingestion.pipeline import process_file

class PipelineManager:
    def __init__(self, max_workers=5):
        # The executor automatically manages the queue and the threads
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        
    def submit_file(self, file_path: str, file_type: str):
        """Watchers call this to hand off a file to the pool."""
        # submit() puts the function and its arguments into the internal queue
        # and a free thread will pick it up automatically.
        self.executor.submit(process_file, file_path, file_type)

    def shutdown(self):
        """Safely close the pool when the program exits."""
        print("Shutting down thread pool...")
        self.executor.shutdown(wait=True)