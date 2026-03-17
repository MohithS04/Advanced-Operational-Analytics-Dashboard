# KPI Glossary & Data Accessibility Guide
## Advanced Operational Analytics Dashboard

> This guide is for **non-technical business users**. Every metric is explained in plain English with its formula, source, and how to act on it.

---

## How to Read the Dashboard

Each page has a **"How to Read This Page"** banner at the top. The sidebar lets you:
- **Filter by date range** — narrow down to any period
- **Filter by department** — see only your team's data
- **Use Quick Views** — pre-built filters for common business questions

**Color coding:**
- 🟢 **Green** = Good / On target
- 🟡 **Amber** = Needs attention
- 🔴 **Red** = Action required immediately

---

## KPI Glossary

### 📦 Total Throughput
- **What it is:** The total number of transactions or work items processed in the selected period.
- **Formula:** `SUM(throughput)` across all processes and departments
- **Source:** Internal Operational Database
- **Updated:** Daily
- **Action:** If throughput drops >10% week-over-week without a known reason, raise with your operations lead.

---

### ⏱️ Average Cycle Time
- **What it is:** The average number of minutes it takes to complete one work item end-to-end.
- **Formula:** `AVG(cycle_time_minutes)` for selected processes
- **Source:** Internal Operational Database
- **Updated:** Daily
- **Action:** Compare to the SLA target for each process. If cycle time regularly exceeds the SLA target, the process is a bottleneck.

---

### ✅ SLA Compliance Rate
- **What it is:** The percentage of work items completed within the agreed Service Level Agreement (SLA) time.
- **Formula:** `(Items completed ≤ SLA target) / Total Items × 100`
- **Source:** Internal Operational Database
- **Updated:** Daily
- **Target:** ≥ 95%
- **Action:** If below 90%, this triggers a Red status. Escalate to the relevant team manager.

---

### ❌ Error Rate
- **What it is:** The percentage of transactions that produced an error or exception.
- **Formula:** `Total Error Count / Total Throughput × 100`
- **Source:** Internal Operational Database
- **Updated:** Daily
- **Target:** < 2%
- **Action:** A spike in error rate often points to a system or training issue. Review the bottleneck analysis page for the affected process.

---

### 💵 Cost per Transaction
- **What it is:** The estimated operational cost to process one unit of work, combining error remediation and processing time costs.
- **Formula:** `(Error Cost + Processing Cost) / Throughput`
- **Source:** Derived from operational metrics
- **Updated:** Daily
- **Action:** A rising trend means processes are becoming less efficient. Red anomaly markers flag days where cost spiked abnormally.

---

### 📊 Operating Margin
- **What it is:** How much profit remains after subtracting operational costs from revenue.
- **Formula:** `(Revenue − Cost) / Revenue × 100`
- **Source:** Derived from throughput and error metrics
- **Updated:** Monthly
- **Action:** A negative margin means the department is operating at a loss relative to its output. Review waterfall chart for biggest cost drivers.

---

## Pre-Built Quick Views

| View Name | What It Shows |
|---|---|
| **This Week's SLA Breaches** | Filters to current week and highlights all SLA failures |
| **Top 10 Bottlenecks** | Ranks the 10 slowest processes by average cycle time |
| **Q1 vs Q2 Efficiency** | Compares operational KPIs between first and second quarters |

---

## Forecasting — Plain Language Guide

The **Trend Analysis page** shows forecasted values for the next 30 days.

- The **dashed line** = the model's best prediction for the future
- The **shaded band** = the range where actual values are expected to fall (95% confidence)
- **Red ✕ markers** = dates where the system detected an unusual spike or drop (anomalies)

The forecast uses a statistical method called **ARIMA**, which learns from seasonal patterns and trends in historical data. It is not a guarantee — it is an informed estimate.
