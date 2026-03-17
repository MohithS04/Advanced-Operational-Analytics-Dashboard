"""
gold_layer_export.py — Queries the gold-layer SQLite views and exports
them to structured CSVs for Tableau consumption and Python modeling.
"""

import sqlite3
import pandas as pd
import logging
from datetime import datetime
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import DB_PATH, GOLD_DIR, LOG_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "gold_export.log"),
    ],
)
log = logging.getLogger(__name__)

GOLD_QUERIES = {
    "operational_efficiency": "SELECT * FROM vw_operational_efficiency ORDER BY record_date, department",
    "sla_compliance":         "SELECT * FROM vw_sla_compliance ORDER BY year_week, department",
    "process_bottlenecks":    "SELECT * FROM vw_process_bottlenecks ORDER BY bottleneck_rank",
    "monthly_kpi_trends":     "SELECT * FROM vw_monthly_kpi_trends ORDER BY year_month, department",
}

FRED_QUERY = """
SELECT series_id, observation_date, value
FROM   silver_fred_indicators
ORDER  BY series_id, observation_date
"""

NYC_QUERY = """
SELECT agency, complaint_type, city,
       COUNT(*)                                             AS record_count,
       SUM(CASE WHEN status='Closed' THEN 1 ELSE 0 END)   AS closed_count,
       ROUND(
           SUM(CASE WHEN status='Closed' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
       )                                                    AS resolution_rate_pct
FROM   silver_nyc_operations
GROUP  BY agency, complaint_type, city
ORDER  BY record_count DESC
LIMIT  5000
"""


def export_view(name: str, query: str, conn: sqlite3.Connection) -> pd.DataFrame:
    df = pd.read_sql_query(query, conn)
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    path = GOLD_DIR / f"{name}_{ts}.csv"
    df.to_csv(path, index=False)
    # Also write a "latest" version for Tableau
    latest = GOLD_DIR / f"{name}_latest.csv"
    df.to_csv(latest, index=False)
    log.info(f"Exported {name}: {len(df)} rows → {latest.name}")
    return df


def run_export():
    log.info("Starting gold layer export ...")
    conn = sqlite3.connect(DB_PATH)
    exported = {}
    try:
        for name, query in GOLD_QUERIES.items():
            exported[name] = export_view(name, query, conn)

        # FRED economic indicators
        df_fred = pd.read_sql_query(FRED_QUERY, conn)
        if not df_fred.empty:
            df_fred_pivot = df_fred.pivot(index="observation_date", columns="series_id", values="value").reset_index()
            df_fred_pivot.to_csv(GOLD_DIR / "fred_indicators_latest.csv", index=False)
            log.info(f"Exported FRED indicators: {len(df_fred_pivot)} rows")
            exported["fred_pivot"] = df_fred_pivot

        # NYC ops summary
        df_nyc = pd.read_sql_query(NYC_QUERY, conn)
        if not df_nyc.empty:
            df_nyc.to_csv(GOLD_DIR / "nyc_ops_summary_latest.csv", index=False)
            log.info(f"Exported NYC ops summary: {len(df_nyc)} rows")
            exported["nyc_summary"] = df_nyc

    finally:
        conn.close()

    log.info("Gold layer export complete.")
    return exported


if __name__ == "__main__":
    run_export()
