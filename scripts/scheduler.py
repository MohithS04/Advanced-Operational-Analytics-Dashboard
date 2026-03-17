"""
scheduler.py — APScheduler-based pipeline orchestrator.
Refreshes FRED + NYC data every 30 minutes and regenerates
synthetic internal operations data every 15 minutes.

Usage:
    python scripts/scheduler.py
"""

import sqlite3
import logging
from datetime import datetime
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import DB_PATH, LOG_DIR, REFRESH_INTERVAL_MINUTES

from apscheduler.schedulers.blocking import BlockingScheduler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "scheduler.log"),
    ],
)
log = logging.getLogger(__name__)


def job_api_ingestion():
    """Pulls live data from FRED + NYC and stores in bronze/silver layers."""
    from api_fetcher import run_ingestion
    log.info(f"[{datetime.utcnow().isoformat()}] Running API ingestion job ...")
    try:
        run_ingestion()
        log.info("API ingestion job completed.")
    except Exception as exc:
        log.error(f"API ingestion job FAILED: {exc}")


def job_synthetic_refresh():
    """Appends a small batch of synthetic internal ops records (simulates streaming)."""
    import pandas as pd
    import numpy as np
    from synthetic_data import generate_records, load_bronze, load_silver

    log.info(f"[{datetime.utcnow().isoformat()}] Running synthetic refresh job ...")
    try:
        # Generate only last 7 days of fresh data each cycle
        df = generate_records(n_days=7)
        conn = sqlite3.connect(DB_PATH)
        try:
            load_bronze(df, conn)
            load_silver(df, conn)
        finally:
            conn.close()
        log.info(f"Synthetic refresh: inserted {len(df)} rows.")
    except Exception as exc:
        log.error(f"Synthetic refresh job FAILED: {exc}")


def start_scheduler():
    scheduler = BlockingScheduler(timezone="UTC")

    # API pull every 30 minutes
    scheduler.add_job(
        job_api_ingestion,
        trigger="interval",
        minutes=REFRESH_INTERVAL_MINUTES,
        id="api_ingestion",
        name="FRED + NYC API Ingestion",
        next_run_time=datetime.utcnow(),  # run immediately on start
    )

    # Synthetic internal data refresh every 15 minutes
    scheduler.add_job(
        job_synthetic_refresh,
        trigger="interval",
        minutes=15,
        id="synthetic_refresh",
        name="Synthetic Ops Data Refresh",
    )

    log.info("Scheduler started. Press Ctrl+C to exit.")
    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        log.info("Scheduler stopped by user.")


if __name__ == "__main__":
    start_scheduler()
