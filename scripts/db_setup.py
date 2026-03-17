"""
db_setup.py — Initializes the SQLite operational database with bronze/silver/gold tables
and the pipeline audit log. Run once before executing ingestion.
"""

import sqlite3
import logging
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import DB_PATH, LOG_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "db_setup.log"),
    ],
)
log = logging.getLogger(__name__)


SCHEMA_SQL = """
-- ─────────────────────────────────────────────────────────────
-- AUDIT TABLE
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pipeline_audit_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    run_at          TEXT    NOT NULL,
    source          TEXT    NOT NULL,
    records_fetched INTEGER  DEFAULT 0,
    response_ms     REAL,
    status          TEXT    NOT NULL CHECK(status IN ('success','failure','partial')),
    error_msg       TEXT
);

-- ─────────────────────────────────────────────────────────────
-- BRONZE (raw ingest)
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bronze_fred_observations (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ingested_at     TEXT    NOT NULL,
    series_id       TEXT    NOT NULL,
    observation_date TEXT   NOT NULL,
    value           REAL,
    raw_json        TEXT
);

CREATE TABLE IF NOT EXISTS bronze_nyc_operations (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ingested_at     TEXT    NOT NULL,
    unique_key      TEXT,
    created_date    TEXT,
    agency          TEXT,
    complaint_type  TEXT,
    descriptor      TEXT,
    city            TEXT,
    status          TEXT,
    raw_json        TEXT
);

CREATE TABLE IF NOT EXISTS bronze_internal_operations (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    ingested_at     TEXT    NOT NULL,
    record_date     TEXT    NOT NULL,
    department      TEXT    NOT NULL,
    process_name    TEXT    NOT NULL,
    throughput      INTEGER,
    cycle_time_min  REAL,
    error_count     INTEGER,
    sla_target_min  REAL,
    sla_met         INTEGER  -- 1 = met, 0 = breached
);

-- ─────────────────────────────────────────────────────────────
-- SILVER (cleaned / validated)
-- ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS silver_fred_indicators (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    series_id       TEXT    NOT NULL,
    observation_date TEXT   NOT NULL,
    value           REAL    NOT NULL,
    UNIQUE(series_id, observation_date)
);

CREATE TABLE IF NOT EXISTS silver_nyc_operations (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    unique_key      TEXT    UNIQUE,
    created_date    TEXT,
    agency          TEXT,
    complaint_type  TEXT,
    descriptor      TEXT,
    city            TEXT,
    status          TEXT
);

CREATE TABLE IF NOT EXISTS silver_internal_operations (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    record_date     TEXT    NOT NULL,
    department      TEXT    NOT NULL,
    process_name    TEXT    NOT NULL,
    throughput      INTEGER NOT NULL,
    cycle_time_min  REAL    NOT NULL,
    error_count     INTEGER NOT NULL,
    error_rate      REAL,
    sla_target_min  REAL    NOT NULL,
    sla_met         INTEGER NOT NULL
);

-- ─────────────────────────────────────────────────────────────
-- GOLD (aggregated / reporting-ready)
-- ─────────────────────────────────────────────────────────────
CREATE VIEW IF NOT EXISTS vw_operational_efficiency AS
SELECT
    record_date,
    department,
    SUM(throughput)                                    AS total_throughput,
    ROUND(AVG(cycle_time_min), 2)                     AS avg_cycle_time_min,
    ROUND(SUM(error_count) * 1.0 / SUM(throughput), 4) AS error_rate
FROM silver_internal_operations
GROUP BY record_date, department;

CREATE VIEW IF NOT EXISTS vw_sla_compliance AS
SELECT
    department,
    strftime('%Y-%W', record_date)                     AS year_week,
    COUNT(*)                                           AS total_records,
    SUM(CASE WHEN sla_met = 0 THEN 1 ELSE 0 END)      AS breach_count,
    ROUND(
        SUM(CASE WHEN sla_met = 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
    )                                                   AS breach_rate_pct,
    ROUND(
        SUM(CASE WHEN sla_met = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
    )                                                   AS compliance_rate_pct
FROM silver_internal_operations
GROUP BY department, year_week;

CREATE VIEW IF NOT EXISTS vw_process_bottlenecks AS
SELECT
    process_name,
    department,
    COUNT(*)                             AS record_count,
    ROUND(AVG(cycle_time_min), 2)        AS avg_cycle_time_min,
    ROUND(MAX(cycle_time_min), 2)        AS max_cycle_time_min,
    SUM(error_count)                     AS total_errors,
    ROUND(AVG(error_count), 2)           AS avg_errors_per_run,
    RANK() OVER (ORDER BY AVG(cycle_time_min) DESC) AS bottleneck_rank
FROM silver_internal_operations
GROUP BY process_name, department;

CREATE VIEW IF NOT EXISTS vw_monthly_kpi_trends AS
SELECT
    strftime('%Y-%m', record_date)       AS year_month,
    department,
    SUM(throughput)                      AS monthly_throughput,
    ROUND(AVG(cycle_time_min), 2)        AS monthly_avg_cycle_time,
    ROUND(
        SUM(error_count) * 1.0 / SUM(throughput), 4
    )                                    AS monthly_error_rate,
    ROUND(
        SUM(CASE WHEN sla_met = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
    )                                    AS monthly_compliance_rate
FROM silver_internal_operations
GROUP BY year_month, department;
"""


def init_database():
    log.info(f"Initializing SQLite database at: {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.executescript(SCHEMA_SQL)
        conn.commit()
        log.info("Database schema created successfully.")
    finally:
        conn.close()


if __name__ == "__main__":
    init_database()
