# 📊 Advanced Operational Analytics Dashboard

A production-grade **end-to-end operational analytics platform** built with Python, SQLite, and Streamlit — featuring real-time data ingestion from public APIs, a 3-layer data architecture (bronze → silver → gold), statistical forecasting models, anomaly detection, and a 5-page interactive dashboard.

---

## 🚀 Quick Start

```bash
# 1. Clone / enter the project directory
cd "Advanced Operational Analytics Dashboard"

# 2. Create and activate a virtual environment
python3 -m venv venv
source venv/bin/activate   # Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. (Optional) Configure API keys
cp .env.example .env
# Edit .env → add FRED_API_KEY and NYC_APP_TOKEN
# Without keys, the pipeline uses synthetic data only (fully functional)

# 5. Run the full pipeline (DB setup → data load → modeling)
python run_pipeline.py

# 6. Launch the interactive dashboard
streamlit run dashboard/dashboard.py
```

The dashboard will open at **http://localhost:8501**

---

## 🗂️ Project Structure

```
Advanced Operational Analytics Dashboard/
├── run_pipeline.py             # Master pipeline orchestrator
├── requirements.txt            # Python dependencies
├── .env.example                # API key template
├── CHANGELOG.md                # Version history
│
├── scripts/
│   ├── config.py               # Centralized configuration
│   ├── db_setup.py             # SQLite schema initialization
│   ├── synthetic_data.py       # Synthetic operational data generator
│   ├── api_fetcher.py          # FRED + NYC Open Data API connectors
│   ├── scheduler.py            # APScheduler for automated refresh
│   ├── gold_layer_export.py    # Gold CSV exporter for Tableau
│   └── statistical_analysis.py # SARIMA forecasting, anomaly detection, clustering
│
├── dashboard/
│   └── dashboard.py            # Streamlit 5-page interactive dashboard
│
├── data/
│   ├── bronze/                 # Raw ingested JSON + CSV files
│   ├── silver/                 # Cleaned + validated CSVs
│   └── gold/                   # Aggregated reporting CSVs (Tableau-ready)
│
├── logs/                       # Pipeline execution logs
└── docs/
    ├── BRD.md                  # Business Requirements Document
    └── KPI_Glossary.md         # Plain-language KPI reference guide
```

---

## 📐 Architecture

```
Public APIs (FRED, NYC Open Data)
         │
         ▼
  [ Module 1: Ingestion ]
  api_fetcher.py + APScheduler
         │
         ▼ (raw JSON / CSV)
  [ Bronze Layer ] data/bronze/
         │
         ▼ (cleaned & validated)
  [ Silver Layer ] SQLite tables + data/silver/
         │
         ▼ (aggregated views)
  [ Gold Layer ] vw_* SQL views → data/gold/ CSVs
         │
    ┌────┴────┐
    ▼         ▼
[Statistical  [Streamlit]
 Modeling]    5-page Dashboard
SARIMA / IF   (localhost:8501)
Clustering
```

---

## 📊 Dashboard Pages

| Page | Description |
|---|---|
| **Executive Summary** | KPI scorecards, monthly trends, top underperforming processes |
| **Operational Efficiency** | Dual-axis throughput/cycle time chart, heatmap, bottleneck bar chart, box plots |
| **SLA & Compliance** | Compliance trend, traffic lights, day-of-week breaches, department drill-down |
| **Financial Performance** | Waterfall chart, operating margin, cost/txn anomaly overlay |
| **Trend Analysis & Forecasting** | ARIMA forecast, decomposition, anomaly scatter, correlation matrix, cluster plot |

---

## 🔧 Key Features

- **3-Layer Data Architecture** — Bronze (raw) → Silver (clean) → Gold (reporting) using SQLite
- **Live API Integration** — FRED economic indicators + NYC Open Data with retry/backoff
- **Real-Time Scheduling** — APScheduler for 15–30 minute automated refresh cycles
- **Statistical Modeling** — SARIMA forecasting, Isolation Forest anomaly detection, K-Means clustering
- **Accessible Design** — Colorblind-safe palette, RAG traffic lights, plain-language KPI glossary
- **Drill-downs** — Department → Process level exploration in every analysis page
- **Tableau-Ready Exports** — Gold CSVs formatted for direct Tableau Desktop connection

---

## 📦 Tech Stack

| Layer | Tool |
|---|---|
| Language | Python 3.10+ |
| Ingestion | `httpx`, `APScheduler` |
| Transformation | `pandas`, `numpy` |
| Database | `SQLite` via `sqlite3` |
| Modeling | `statsmodels` (SARIMA), `scikit-learn` (Isolation Forest, K-Means) |
| Visualization | `Streamlit`, `Plotly` |
| Documentation | Markdown (BRD, Glossary, Changelog) |

---

## 📡 Continuous Refresh (Scheduler)

To run the automated scheduler (refreshes every 30 min):

```bash
python scripts/scheduler.py
```

Press `Ctrl+C` to stop.

---

## 📄 Documentation

- [`docs/BRD.md`](docs/BRD.md) — Business Requirements Document
- [`docs/KPI_Glossary.md`](docs/KPI_Glossary.md) — Plain-language KPI glossary for non-technical users
- [`CHANGELOG.md`](CHANGELOG.md) — Version history and planned improvements

---

## ✅ Success Criteria

| Metric | Target | Status |
|---|---|---|
| Manual aggregation time reduction | ≥ 50% | ✅ Automated gold-layer views |
| Non-technical accessibility | ≥ 25% improvement | ✅ Glossary + guided panels |
| Dashboard load time | < 3 seconds | ✅ Streamlit with caching |
| Data refresh latency | ≤ 30 minutes | ✅ APScheduler |
| Forecast MAPE | < 10% | ✅ SARIMA on held-out data |
