from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from pathlib import Path
import time
import os
import yaml


class Handler(FileSystemEventHandler):
    """
    Watches the ingestion directory for newly created files.

    When the first file appears inside a new micro-batch folder, the handler waits
    until the folder contents stop changing, then considers the folder ready
    for downstream processing.
    """

    def __init__(self):
        """Store the last processed folder to avoid duplicate processing."""
        self.last_path = None

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
                    print(f"New Folder Created: {src_path}")
                    print("-" * 30)
                    break


# --------------------------------------------------------------------
# Load project configuration
# --------------------------------------------------------------------

# Navigate from:
# project_root/ingestion/stream/script.py
# to:
# project_root/
root = Path(__file__).resolve().parent.parent.parent

config_path = root / "config" / "config.yaml"

with open(config_path, "r", encoding="utf-8") as file:
    data = yaml.safe_load(file)

# Directory that will be monitored for incoming files.
STREAM_DIR = root / data["Ingestion"]["Stream"]

# --------------------------------------------------------------------
# Configure and start the watchdog observer
# --------------------------------------------------------------------

obs = Observer()

obs.schedule(
    Handler(),
    path=STREAM_DIR,
    recursive=True
)

obs.start()

try:
    # Keep the main thread alive while the observer runs in the background.
    while True:
        time.sleep(5)

except KeyboardInterrupt:
    # Gracefully stop monitoring when the application is terminated.
    obs.stop()

obs.join()