import time
from pathlib import Path
import sys
from apscheduler.schedulers.background import BackgroundScheduler
from ingestion.batch.batch_ingestion import batch_ingest
from config.config_loader import load_config



root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(root))
config = load_config()

def run_batch_watcher(pipeline_mng):
    scheduler = BackgroundScheduler()

    scheduler.add_job(
        batch_ingest,
        args=[pipeline_mng],
        trigger="cron",
        day_of_week=config["cron"]["day_of_week"],
        hour=config["cron"]["hour"],
        minute=config["cron"]["minute"],
    )

    scheduler.start()
    print("[BATCH] Batch scheduler started")

    try:
        # Keep the main thread alive while the scheduler runs in the background.
        while True:
            time.sleep(5)

    except KeyboardInterrupt:
        # Gracefully stop the scheduler when the application is terminated.
        scheduler.shutdown()        