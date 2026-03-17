"""
config.py — Centralized configuration for the Analytics Dashboard pipeline.
Loads settings from environment variables / .env file.
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env file if present
load_dotenv()

# ── Project Root ──────────────────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# ── API Configuration ─────────────────────────────────────────────────────────
FRED_API_KEY = os.getenv("FRED_API_KEY", "")
FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

NYC_APP_TOKEN = os.getenv("NYC_APP_TOKEN", "")
NYC_BASE_URL = "https://data.cityofnewyork.us/resource/833y-fsy8.json"  # 311 Service Requests

# ── Database ──────────────────────────────────────────────────────────────────
DB_PATH = os.getenv("DB_PATH", str(PROJECT_ROOT / "data" / "operational.db"))

# ── Data Layer Paths ──────────────────────────────────────────────────────────
BRONZE_DIR = Path(os.getenv("BRONZE_DIR", str(PROJECT_ROOT / "data" / "bronze")))
SILVER_DIR = Path(os.getenv("SILVER_DIR", str(PROJECT_ROOT / "data" / "silver")))
GOLD_DIR = Path(os.getenv("GOLD_DIR", str(PROJECT_ROOT / "data" / "gold")))
LOG_DIR = Path(os.getenv("LOG_DIR", str(PROJECT_ROOT / "logs")))

# ── API Fetch Settings ────────────────────────────────────────────────────────
FRED_SERIES = [
    "GDP",           # US Gross Domestic Product
    "CPIAUCSL",      # Consumer Price Index
    "UNRATE",        # Unemployment Rate
    "FEDFUNDS",      # Federal Funds Rate
    "INDPRO",        # Industrial Production Index
]
FRED_OBSERVATION_START = "2020-01-01"
NYC_PAGE_LIMIT = 1000
REQUEST_TIMEOUT = 30  # seconds
RETRY_MAX_ATTEMPTS = 3
RETRY_BACKOFF_BASE = 2  # seconds (exponential backoff)

# ── Scheduler ─────────────────────────────────────────────────────────────────
REFRESH_INTERVAL_MINUTES = 30

# ── Ensure directories exist ──────────────────────────────────────────────────
for _dir in [BRONZE_DIR, SILVER_DIR, GOLD_DIR, LOG_DIR]:
    _dir.mkdir(parents=True, exist_ok=True)
