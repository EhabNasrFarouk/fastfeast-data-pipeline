# main.py
import time
from apscheduler.schedulers.background import BackgroundScheduler
from ingestion.batch.batch_ingestion import run_batch_pipeline 
from config.config_loader import load_config

config = load_config()

if __name__ == "__main__":

    # 1. Batch Scheduler
    scheduler = BackgroundScheduler()
    scheduler.add_job(run_batch_pipeline,trigger="cron",day_of_week=config["cron"]["day_of_week"],hour=config["cron"]["hour"],minute=config["cron"]["minute"])
    scheduler.start()
    print("[MAIN] Batch scheduler started")

    # 2. Keep Alive
    print("[MAIN] Pipeline running — press Ctrl+C to stop")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[MAIN] Shutting down...")
        scheduler.shutdown()
        print("[MAIN] Done.")


        