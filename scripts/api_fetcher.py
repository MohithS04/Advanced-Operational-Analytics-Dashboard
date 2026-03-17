"""
api_fetcher.py — Fetches live data from FRED and NYC Open Data APIs.
Implements exponential-backoff retry, pagination, and bronze layer storage.
Falls back to synthetic/cached data when keys are missing.
"""

import httpx
import json
import time
import sqlite3
import logging
from datetime import datetime
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import (
    FRED_API_KEY, FRED_BASE_URL, FRED_SERIES, FRED_OBSERVATION_START,
    NYC_APP_TOKEN, NYC_BASE_URL, NYC_PAGE_LIMIT,
    DB_PATH, BRONZE_DIR, LOG_DIR,
    REQUEST_TIMEOUT, RETRY_MAX_ATTEMPTS, RETRY_BACKOFF_BASE,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "api_fetcher.log"),
    ],
)
log = logging.getLogger(__name__)


# ─── Retry helper ──────────────────────────────────────────────────────────────

def _fetch_with_retry(url: str, params: dict, source: str) -> tuple[dict | list | None, float]:
    """
    Fetches URL with exponential-backoff retries.
    Returns (parsed_json, elapsed_ms). Returns (None, elapsed_ms) on failure.
    """
    for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
        t0 = time.perf_counter()
        try:
            resp = httpx.get(url, params=params, timeout=REQUEST_TIMEOUT)
            elapsed = (time.perf_counter() - t0) * 1000
            resp.raise_for_status()
            return resp.json(), elapsed
        except httpx.HTTPStatusError as exc:
            elapsed = (time.perf_counter() - t0) * 1000
            log.warning(f"[{source}] HTTP {exc.response.status_code} on attempt {attempt}/{RETRY_MAX_ATTEMPTS}")
        except Exception as exc:
            elapsed = (time.perf_counter() - t0) * 1000
            log.warning(f"[{source}] Error on attempt {attempt}/{RETRY_MAX_ATTEMPTS}: {exc}")

        if attempt < RETRY_MAX_ATTEMPTS:
            backoff = RETRY_BACKOFF_BASE ** attempt
            log.info(f"[{source}] Retrying in {backoff}s ...")
            time.sleep(backoff)

    return None, elapsed


# ─── Audit logging ─────────────────────────────────────────────────────────────

def _log_audit(conn: sqlite3.Connection, source: str, records: int, ms: float,
               status: str, error_msg: str = "") -> None:
    conn.execute(
        """INSERT INTO pipeline_audit_log
           (run_at, source, records_fetched, response_ms, status, error_msg)
           VALUES (?,?,?,?,?,?)""",
        (datetime.utcnow().isoformat(), source, records, ms, status, error_msg),
    )
    conn.commit()


# ─── FRED ─────────────────────────────────────────────────────────────────────

def fetch_fred_series(series_id: str, conn: sqlite3.Connection) -> int:
    """Fetch FRED observations for a given series. Returns count of records saved."""
    if not FRED_API_KEY:
        log.warning(f"[FRED/{series_id}] No API key — skipping real fetch.")
        _log_audit(conn, f"FRED/{series_id}", 0, 0, "failure", "No API key configured.")
        return 0

    params = {
        "series_id":         series_id,
        "api_key":           FRED_API_KEY,
        "file_type":         "json",
        "observation_start": FRED_OBSERVATION_START,
    }
    data, ms = _fetch_with_retry(FRED_BASE_URL, params, f"FRED/{series_id}")

    if data is None:
        _log_audit(conn, f"FRED/{series_id}", 0, ms, "failure", "All retries exhausted.")
        return 0

    observations = data.get("observations", [])
    ingested_at = datetime.utcnow().isoformat()
    rows = [
        (ingested_at, series_id, obs["date"], float(obs["value"]) if obs["value"] != "." else None, json.dumps(obs))
        for obs in observations
    ]
    conn.executemany(
        """INSERT INTO bronze_fred_observations
           (ingested_at, series_id, observation_date, value, raw_json)
           VALUES (?,?,?,?,?)""",
        rows,
    )

    # Silver table upsert (non-null values only)
    silver_rows = [(series_id, obs["date"], float(obs["value"]))
                   for obs in observations if obs["value"] != "."]
    conn.executemany(
        """INSERT OR REPLACE INTO silver_fred_indicators
           (series_id, observation_date, value) VALUES (?,?,?)""",
        silver_rows,
    )
    conn.commit()

    # Save to bronze file
    fname = BRONZE_DIR / f"fred_{series_id}_{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}.json"
    fname.write_text(json.dumps(data, indent=2))
    log.info(f"[FRED/{series_id}] Fetched {len(rows)} observations in {ms:.0f}ms → {fname.name}")
    _log_audit(conn, f"FRED/{series_id}", len(rows), ms, "success")
    return len(rows)


def fetch_all_fred(conn: sqlite3.Connection) -> None:
    total = 0
    for series_id in FRED_SERIES:
        total += fetch_fred_series(series_id, conn)
    log.info(f"[FRED] Total observations fetched: {total}")


# ─── NYC Open Data ─────────────────────────────────────────────────────────────

def fetch_nyc_operations(conn: sqlite3.Connection, max_pages: int = 5) -> int:
    """
    Fetches NYC 311 Service Requests as a proxy for operational process records.
    Paginates through up to max_pages * PAGE_LIMIT records.
    """
    headers = {}
    if NYC_APP_TOKEN:
        headers["X-App-Token"] = NYC_APP_TOKEN

    total_records = 0
    ingested_at = datetime.utcnow().isoformat()

    for page in range(max_pages):
        params = {
            "$limit":  NYC_PAGE_LIMIT,
            "$offset": page * NYC_PAGE_LIMIT,
            "$order":  "created_date DESC",
        }
        data, ms = _fetch_with_retry(NYC_BASE_URL, params, f"NYC/page{page+1}")

        if data is None:
            _log_audit(conn, f"NYC/page{page+1}", 0, ms, "failure", "All retries exhausted.")
            break

        if not data:  # empty page → done
            break

        bronze_rows = [
            (ingested_at, r.get("unique_key"), r.get("created_date"), r.get("agency"),
             r.get("complaint_type"), r.get("descriptor"), r.get("city"), r.get("status"),
             json.dumps(r))
            for r in data
        ]
        conn.executemany(
            """INSERT INTO bronze_nyc_operations
               (ingested_at, unique_key, created_date, agency, complaint_type,
                descriptor, city, status, raw_json)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            bronze_rows,
        )

        silver_rows = [
            (r.get("unique_key"), r.get("created_date"), r.get("agency"),
             r.get("complaint_type"), r.get("descriptor"), r.get("city"), r.get("status"))
            for r in data
        ]
        conn.executemany(
            """INSERT OR IGNORE INTO silver_nyc_operations
               (unique_key, created_date, agency, complaint_type, descriptor, city, status)
               VALUES (?,?,?,?,?,?,?)""",
            silver_rows,
        )
        conn.commit()

        # Save bronze page file
        fname = BRONZE_DIR / f"nyc_ops_page{page+1}_{datetime.utcnow().strftime('%Y%m%dT%H%M%S')}.json"
        fname.write_text(json.dumps(data, indent=2))
        log.info(f"[NYC/page{page+1}] Fetched {len(data)} records in {ms:.0f}ms → {fname.name}")
        _log_audit(conn, f"NYC/page{page+1}", len(data), ms, "success")
        total_records += len(data)

    log.info(f"[NYC] Total records fetched: {total_records}")
    return total_records


# ─── Main entry ────────────────────────────────────────────────────────────────

def run_ingestion():
    log.info("=" * 60)
    log.info("Starting data ingestion run ...")
    conn = sqlite3.connect(DB_PATH)
    try:
        fetch_all_fred(conn)
        fetch_nyc_operations(conn)
    finally:
        conn.close()
    log.info("Ingestion run complete.")


if __name__ == "__main__":
    run_ingestion()
