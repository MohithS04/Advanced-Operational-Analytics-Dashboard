"""
synthetic_data.py — Generates realistic synthetic operational data and loads it
into the bronze/silver internal operations tables.
"""

import sqlite3
import json
import random
import logging
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd
import numpy as np
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import DB_PATH, BRONZE_DIR, SILVER_DIR, LOG_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "synthetic_data.log"),
    ],
)
log = logging.getLogger(__name__)

# ─── Configuration ─────────────────────────────────────────────────────────────
DEPARTMENTS = ["Operations", "Finance", "HR", "IT", "Logistics", "Risk", "Compliance"]
PROCESSES = {
    "Operations": ["Order Fulfillment", "Inventory Replenishment", "Quality Check"],
    "Finance":    ["Invoice Processing", "Budget Reconciliation", "Expense Audit"],
    "HR":         ["Onboarding", "Payroll Processing", "Benefits Enrollment"],
    "IT":         ["Incident Resolution", "Change Management", "Deployment Pipeline"],
    "Logistics":  ["Shipment Tracking", "Customs Clearance", "Route Optimization"],
    "Risk":       ["Credit Assessment", "Fraud Review", "Compliance Check"],
    "Compliance": ["Regulatory Reporting", "AML Screening", "Policy Review"],
}
SLA_TARGETS = {
    "Order Fulfillment": 480, "Inventory Replenishment": 240, "Quality Check": 60,
    "Invoice Processing": 120, "Budget Reconciliation": 360, "Expense Audit": 90,
    "Onboarding": 2880, "Payroll Processing": 120, "Benefits Enrollment": 240,
    "Incident Resolution": 240, "Change Management": 480, "Deployment Pipeline": 120,
    "Shipment Tracking": 1440, "Customs Clearance": 2880, "Route Optimization": 60,
    "Credit Assessment": 180, "Fraud Review": 30, "Compliance Check": 720,
    "Regulatory Reporting": 480, "AML Screening": 60, "Policy Review": 1440,
}

np.random.seed(42)


def generate_records(n_days: int = 365) -> pd.DataFrame:
    """Generate n_days of daily synthetic operational records for all processes."""
    records = []
    start = datetime(2024, 1, 1)

    for day_offset in range(n_days):
        record_date = start + timedelta(days=day_offset)
        is_weekend = record_date.weekday() >= 5

        for dept, processes in PROCESSES.items():
            for process in processes:
                sla_target = SLA_TARGETS[process]

                # Simulate realistic distributions with seasonality + noise
                seasonal_factor = 1 + 0.15 * np.sin(2 * np.pi * day_offset / 365)
                base_throughput = np.random.poisson(50 * seasonal_factor)
                throughput = max(1, base_throughput // 3 if is_weekend else base_throughput)

                # Cycle time ~ Normal(mean=sla*0.8, std=sla*0.2), clipped at 0
                mean_cycle = sla_target * 0.8
                std_cycle = sla_target * 0.2
                cycle_time = max(1.0, np.random.normal(mean_cycle, std_cycle))

                # Inject anomalies ~5% of the time
                if random.random() < 0.05:
                    cycle_time *= random.uniform(1.5, 3.0)

                error_count = np.random.poisson(max(0.5, throughput * 0.02))
                sla_met = 1 if cycle_time <= sla_target else 0

                records.append({
                    "record_date":    record_date.strftime("%Y-%m-%d"),
                    "department":     dept,
                    "process_name":   process,
                    "throughput":     int(throughput),
                    "cycle_time_min": round(cycle_time, 2),
                    "error_count":    int(error_count),
                    "sla_target_min": float(sla_target),
                    "sla_met":        int(sla_met),
                })

    return pd.DataFrame(records)


def load_bronze(df: pd.DataFrame, conn: sqlite3.Connection) -> None:
    ingested_at = datetime.utcnow().isoformat()
    rows = [
        (ingested_at, r.record_date, r.department, r.process_name,
         int(r.throughput), float(r.cycle_time_min), int(r.error_count),
         float(r.sla_target_min), int(r.sla_met))
        for r in df.itertuples(index=False)
    ]
    conn.executemany(
        """INSERT INTO bronze_internal_operations
           (ingested_at, record_date, department, process_name,
            throughput, cycle_time_min, error_count, sla_target_min, sla_met)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        rows,
    )
    conn.commit()
    log.info(f"Loaded {len(rows)} rows into bronze_internal_operations.")


def load_silver(df: pd.DataFrame, conn: sqlite3.Connection) -> None:
    df2 = df.copy()
    df2["error_rate"] = (df2["error_count"] / df2["throughput"].clip(lower=1)).round(4)
    rows = [
        (r.record_date, r.department, r.process_name, int(r.throughput),
         float(r.cycle_time_min), int(r.error_count), float(r.error_rate),
         float(r.sla_target_min), int(r.sla_met))
        for r in df2.itertuples(index=False)
    ]
    conn.executemany(
        """INSERT OR IGNORE INTO silver_internal_operations
           (record_date, department, process_name, throughput, cycle_time_min,
            error_count, error_rate, sla_target_min, sla_met)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        rows,
    )
    conn.commit()
    log.info(f"Loaded {len(rows)} rows into silver_internal_operations.")


def save_csv(df: pd.DataFrame) -> None:
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
    bronze_path = BRONZE_DIR / f"internal_ops_{ts}.csv"
    silver_path = SILVER_DIR / f"internal_ops_clean_{ts}.csv"
    df.to_csv(bronze_path, index=False)
    df_silver = df.copy()
    df_silver["error_rate"] = (df_silver["error_count"] / df_silver["throughput"].clip(lower=1)).round(4)
    df_silver.to_csv(silver_path, index=False)
    log.info(f"Saved bronze CSV: {bronze_path}")
    log.info(f"Saved silver CSV: {silver_path}")


def run():
    log.info("Generating synthetic operational records (365 days) ...")
    df = generate_records(n_days=365)
    save_csv(df)
    conn = sqlite3.connect(DB_PATH)
    try:
        load_bronze(df, conn)
        load_silver(df, conn)
    finally:
        conn.close()
    log.info("Synthetic data generation complete.")


if __name__ == "__main__":
    run()
