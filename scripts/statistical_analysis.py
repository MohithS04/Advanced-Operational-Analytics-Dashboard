"""
statistical_analysis.py — Time series forecasting, anomaly detection, clustering,
and correlation analysis on the cleaned operational metrics.
Exports forecast CSVs and cluster labels for dashboard use.
"""

import sqlite3
import warnings
import logging
from datetime import datetime, timedelta
from pathlib import Path
import sys

import numpy as np
import pandas as pd
from scipy import stats
from sklearn.ensemble import IsolationForest
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from statsmodels.tsa.seasonal import seasonal_decompose
from statsmodels.tsa.statespace.sarimax import SARIMAX

warnings.filterwarnings("ignore")

sys.path.insert(0, str(Path(__file__).resolve().parent))
from config import DB_PATH, GOLD_DIR, LOG_DIR

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "statistical_analysis.log"),
    ],
)
log = logging.getLogger(__name__)


# ─── Load data ─────────────────────────────────────────────────────────────────

def load_operations_data() -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query(
        "SELECT * FROM silver_internal_operations ORDER BY record_date, department, process_name",
        conn,
    )
    conn.close()
    df["record_date"] = pd.to_datetime(df["record_date"])
    return df


# ─── Time Series Decomposition ─────────────────────────────────────────────────

def decompose_timeseries(df: pd.DataFrame) -> dict:
    """Decompose daily throughput into trend + seasonal + residual."""
    log.info("Running time series decomposition ...")
    daily = df.groupby("record_date")["throughput"].sum().reset_index()
    daily = daily.set_index("record_date").sort_index()
    daily_full = daily.asfreq("D").fillna(method="ffill")

    result = seasonal_decompose(daily_full["throughput"], model="additive", period=7)
    decomp_df = pd.DataFrame({
        "date":      daily_full.index,
        "observed":  result.observed,
        "trend":     result.trend,
        "seasonal":  result.seasonal,
        "residual":  result.resid,
    }).dropna()

    out_path = GOLD_DIR / "timeseries_decomposition_latest.csv"
    decomp_df.to_csv(out_path, index=False)
    log.info(f"Decomposition saved: {out_path.name} ({len(decomp_df)} rows)")
    return {"decomposition": decomp_df, "result": result}


# ─── ARIMA Forecasting ─────────────────────────────────────────────────────────

def forecast_arima(df: pd.DataFrame, horizon_days: int = 30) -> pd.DataFrame:
    """Fit a SARIMA model on daily throughput and forecast 30 days ahead."""
    log.info("Fitting SARIMA model for throughput forecast ...")
    daily = df.groupby("record_date")["throughput"].sum().reset_index()
    daily = daily.set_index("record_date").sort_index()
    daily = daily.asfreq("D").fillna(method="ffill")

    # SARIMA(1,1,1)(1,1,0)[7]
    model = SARIMAX(daily["throughput"], order=(1, 1, 1), seasonal_order=(1, 1, 0, 7),
                    enforce_stationarity=False, enforce_invertibility=False)
    fit = model.fit(disp=False)

    forecast = fit.get_forecast(steps=horizon_days)
    summary = forecast.summary_frame(alpha=0.05)
    last_date = daily.index[-1]
    summary.index = [last_date + timedelta(days=i + 1) for i in range(horizon_days)]

    forecast_df = pd.DataFrame({
        "date":          summary.index,
        "forecast":      summary["mean"],
        "lower_95":      summary["mean_ci_lower"],
        "upper_95":      summary["mean_ci_upper"],
    }).reset_index(drop=True)

    historical_df = daily.reset_index().rename(columns={"record_date": "date", "throughput": "actual"})
    combined = historical_df.merge(forecast_df, on="date", how="outer").sort_values("date")

    out_path = GOLD_DIR / "arima_forecast_latest.csv"
    combined.to_csv(out_path, index=False)
    log.info(f"ARIMA forecast saved: {out_path.name} ({horizon_days} days ahead)")
    return combined


# ─── Anomaly Detection ─────────────────────────────────────────────────────────

def detect_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    """Apply Isolation Forest and Z-score flagging to error_count & cycle_time."""
    log.info("Running anomaly detection (Isolation Forest + Z-score) ...")
    feats = df[["throughput", "cycle_time_min", "error_count", "error_rate"]].fillna(0)

    # Isolation Forest
    iso = IsolationForest(contamination=0.05, random_state=42)
    df = df.copy()
    df["anomaly_iso"] = iso.fit_predict(feats)  # -1 = anomaly

    # Z-score on error_count
    df["z_error"] = stats.zscore(df["error_count"].fillna(0))
    df["anomaly_zscore"] = (df["z_error"].abs() > 2.5).astype(int)
    df["is_anomaly"] = ((df["anomaly_iso"] == -1) | (df["anomaly_zscore"] == 1)).astype(int)

    out_path = GOLD_DIR / "anomaly_detection_latest.csv"
    df[["record_date", "department", "process_name", "throughput", "cycle_time_min",
        "error_count", "error_rate", "is_anomaly", "anomaly_iso", "anomaly_zscore"]].to_csv(out_path, index=False)
    n_anomalies = df["is_anomaly"].sum()
    log.info(f"Anomaly detection saved: {out_path.name} ({n_anomalies} anomalies flagged)")
    return df


# ─── Correlation Analysis ──────────────────────────────────────────────────────

def correlation_analysis(df: pd.DataFrame) -> pd.DataFrame:
    """Compute Pearson correlations between key operational metrics."""
    log.info("Computing Pearson correlation matrix ...")
    metrics = df[["throughput", "cycle_time_min", "error_count", "error_rate", "sla_met"]].dropna()
    corr = metrics.corr(method="pearson").round(4)

    out_path = GOLD_DIR / "correlation_matrix_latest.csv"
    corr.to_csv(out_path)
    log.info(f"Correlation matrix saved: {out_path.name}")
    return corr


# ─── K-Means Clustering ────────────────────────────────────────────────────────

def cluster_departments(df: pd.DataFrame, k: int = 4) -> pd.DataFrame:
    """Cluster processes by operational behavior using K-Means."""
    log.info(f"Running K-Means clustering (k={k}) on department-level aggregates ...")
    agg = df.groupby(["department", "process_name"]).agg(
        avg_throughput=("throughput", "mean"),
        avg_cycle_time=("cycle_time_min", "mean"),
        avg_error_rate=("error_rate", "mean"),
        compliance_rate=("sla_met", "mean"),
    ).reset_index()

    scaler = StandardScaler()
    features = scaler.fit_transform(agg[["avg_throughput", "avg_cycle_time", "avg_error_rate", "compliance_rate"]])

    km = KMeans(n_clusters=k, random_state=42, n_init="auto")
    agg["cluster"] = km.fit_predict(features)

    out_path = GOLD_DIR / "cluster_labels_latest.csv"
    agg.to_csv(out_path, index=False)
    log.info(f"Clustering saved: {out_path.name} — {k} clusters across {len(agg)} processes")
    return agg


# ─── SLA Forecast ─────────────────────────────────────────────────────────────

def forecast_sla_breach(df: pd.DataFrame, horizon_days: int = 30) -> pd.DataFrame:
    """Forecast daily SLA breach rate for the next 30 days using SARIMA."""
    log.info("Fitting SARIMA on SLA breach rate ...")
    daily = df.groupby("record_date").agg(
        total=("sla_met", "count"),
        breaches=("sla_met", lambda x: (x == 0).sum()),
    )
    daily["breach_rate"] = daily["breaches"] / daily["total"]
    daily = daily.asfreq("D").fillna(method="ffill")

    model = SARIMAX(daily["breach_rate"], order=(1, 1, 1), seasonal_order=(1, 0, 0, 7),
                    enforce_stationarity=False, enforce_invertibility=False)
    fit = model.fit(disp=False)
    forecast = fit.get_forecast(steps=horizon_days)
    summary = forecast.summary_frame(alpha=0.05)
    last_date = daily.index[-1]
    summary.index = [last_date + timedelta(days=i + 1) for i in range(horizon_days)]

    forecast_df = pd.DataFrame({
        "date":        summary.index,
        "breach_rate_forecast": summary["mean"].clip(0, 1),
        "lower_95":    summary["mean_ci_lower"].clip(0, 1),
        "upper_95":    summary["mean_ci_upper"].clip(0, 1),
    }).reset_index(drop=True)

    out_path = GOLD_DIR / "sla_breach_forecast_latest.csv"
    forecast_df.to_csv(out_path, index=False)
    log.info(f"SLA breach forecast saved: {out_path.name}")
    return forecast_df


# ─── Run all ───────────────────────────────────────────────────────────────────

def run_all():
    df = load_operations_data()
    log.info(f"Loaded {len(df)} records from silver_internal_operations")
    decompose_timeseries(df)
    forecast_arima(df)
    detect_anomalies(df)
    correlation_analysis(df)
    cluster_departments(df)
    forecast_sla_breach(df)
    log.info("All statistical analyses complete.")


if __name__ == "__main__":
    run_all()
