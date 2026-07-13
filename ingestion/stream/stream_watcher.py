from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from datetime import date
from pathlib import Path
import time
import os
import sys


class Handler(FileSystemEventHandler):
    """
    Watches the ingestion directory for newly created files.

    When the first file appears inside a new micro-batch folder, the handler waits
    until the folder contents stop changing, then considers the folder ready
    for downstream processing.
    """

    def __init__(self, pipeline_mng: PipelineManager):
        """Store the last processed folder to avoid duplicate processing."""
        self.last_path = None
        self.pipeline_mng = pipeline_mng

    def take_action(path):
        date_info = path.parts
        hour = date_info[-2:][1]
        date = date.fromisoformat(date_info[-2:][0])
        print(f"date: {date}   hour: {hour}")
        print("-" * 30)

    def on_created(self, event):
        """
        Triggered whenever a file or directory is created.

        The parent folder of the created file is monitored until its file
        count remains unchanged for one second.
        """
        src_path = Path(event.src_path).parent

        # Process each folder only once.
        if not event.is_directory and src_path != self.last_path:
            self.last_path = src_path

            # Wait until no new files are added to the folder.
            while True:
                file_count_before = len(os.listdir(src_path))
                time.sleep(1)
                file_count_after = len(os.listdir(src_path))

                if file_count_before == file_count_after:
                    # ------------------ Testing ------------------
                    # print("-" * 30)
                    # print(f"New Folder Created: {src_path}")
                    date_info = src_path.parts
                    hour = date_info[-2:][1]
                    day = date.fromisoformat(date_info[-2:][0])
                    # print(f"date: {date}   hour: {hour}")
                    # print("-" * 30)

                    # ------------------ Actions ------------------
                    # Generating run_id.
                    run_id = new_run_id()

                    # Getting unprocessed files.
                    files_paths = [ os.path.join(src_path, f) for f in os.listdir(src_path) ]
                    stream_files = get_unprocessed_files(files_paths, day, hour)

                    # Passing the files to the thread pool.
                    for f in stream_files:
                        self.pipeline_mng.submit_file(run_id, f, "stream", day, hour)


                    break


# --------------------------------------------------------------------
# Load project configuration
# --------------------------------------------------------------------

# Navigate from:
# project_root/ingestion/stream/script.py
# to:
# project_root/
root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(root))
from config.config_loader import load_config
from ingestion.thread_pool import PipelineManager
from ingestion.file_tracker import get_unprocessed_files, new_run_id


data = load_config()

# Directory that will be monitored for incoming files.
STREAM_DIR = root / data["Ingestion"]["Stream"]

# --------------------------------------------------------------------
# Configure and start the watchdog observer
# --------------------------------------------------------------------

def run_stream_watcher(pipeline_mng: PipelineManager):
    obs = Observer()

    obs.schedule(
        Handler(pipeline_mng),
        path=STREAM_DIR,
        recursive=True
    )

    obs.start()

    try:
        # Keep the main thread alive while the observer runs in the background.
        while True:
            time.sleep(5)
            # print("Hello from streaaaaaaaaaaaaaaaam!")

    except KeyboardInterrupt:
        # Gracefully stop monitoring when the application is terminated.
        obs.stop()

    obs.join()