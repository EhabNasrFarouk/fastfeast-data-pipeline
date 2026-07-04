import threading
import time
from stream.stream_watcher import run_stream_watcher
# from batch.batch_ingestion import run_batch_pipeline

def run_starter():
    threads = [
        threading.Thread(target=run_stream_watcher, name="Stream Watcher", daemon=True),
        # threading.Thread(target=run_batch_pipeline, name="Batch Watcher", daemon=True)
    ]

    for t in threads:
        t.start()

    try:
        while True:
            for t in threads:
                if not t.is_alive():
                    print(f"{t.name} has crashed, now the the pipeline will stop.")
                    return

            time.sleep(10)

    except KeyboardInterrupt:
        print("Stopping the main program.")

run_starter()