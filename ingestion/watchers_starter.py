import threading
import time
import sys
from pathlib import Path


root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root))
from ingestion.stream.stream_watcher import run_stream_watcher
from ingestion.thread_pool import PipelineManager
from ingestion.batch.scheduler import run_batch_watcher
from ingestion.file_tracker import init_tracker_table

# Must run before either watcher starts — both read/write etl_file_tracker
# on the first file they see, and the table doesn't exist until this creates it.
init_tracker_table()

pipeline_mng = PipelineManager()

stop_event = threading.Event()
threads = [
    threading.Thread(target=run_stream_watcher, args=(pipeline_mng,), name="Stream Watcher", daemon=True),
    threading.Thread(target=run_batch_watcher, args=(pipeline_mng,), name="Batch Watcher", daemon=True)
]

for t in threads:
    t.start()

try:
    while True:
        # print("Hello from main program!")
        for t in threads:
            if not t.is_alive():
                print(f"{t.name} has crashed, now the the pipeline will stop.")
                exit(0)

        time.sleep(5)

except KeyboardInterrupt:
    print("Stopping the main program.")