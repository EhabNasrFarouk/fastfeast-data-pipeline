# main.py
import time
from apscheduler.schedulers.background import BackgroundScheduler
from test import run_batch_pipeline 
if __name__ == "__main__":

    # 1. Batch Scheduler
    scheduler = BackgroundScheduler()
    scheduler.add_job(run_batch_pipeline,trigger="cron",day_of_week="thu",hour=8,minute=0)
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


        