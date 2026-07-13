import threading
import time
from ingestion.stream.stream_watcher import run_stream_watcher
from ingestion.thread_pool import PipelineManager
from ingestion.batch.scheduler import run_batch_watcher


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
        print("Hello from main program!")
        for t in threads:
            if not t.is_alive():
                print(f"{t.name} has crashed, now the the pipeline will stop.")
                exit(0)

        time.sleep(5)

except KeyboardInterrupt:
    print("Stopping the main program.")