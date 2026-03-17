"""
dashboard.py — Streamlit interactive analytics dashboard.
Run with:  streamlit run dashboard/dashboard.py

Pages:
  1. Executive Summary
  2. Operational Efficiency Deep Dive
  3. SLA & Compliance Monitoring
  4. Financial Performance
  5. Trend Analysis & Forecasting
"""

import sqlite3
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st
from pathlib import Path
import sys

# ── path setup ──────────────────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))
from config import DB_PATH, GOLD_DIR

# ── page config ─────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Advanced Operational Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── color palette (colorblind-safe, Tableau Color Blind 10 inspired) ────────────
PALETTE = ["#006BA4", "#FF800E", "#ABABAB", "#595959", "#5F9ED1",
           "#C85200", "#898989", "#A2C8EC", "#FFBC79", "#CFCFCF"]

DEPT_COLORS = {
    "Operations": "#006BA4", "Finance": "#FF800E", "HR": "#595959",
    "IT": "#5F9ED1", "Logistics": "#C85200", "Risk": "#898989", "Compliance": "#CFCFCF",
}

# ── Custom CSS ──────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

    .metric-card {
        background: linear-gradient(135deg, #1e3a5f 0%, #0d2137 100%);
        border-radius: 12px;
        padding: 20px 24px;
        color: #fff;
        border-left: 4px solid #5F9ED1;
        margin-bottom: 10px;
    }
    .metric-card .label { font-size: 0.78rem; color: #A2C8EC; font-weight: 500; text-transform: uppercase; letter-spacing: 0.05em; }
    .metric-card .value { font-size: 2rem; font-weight: 700; margin: 4px 0; }
    .metric-card .delta { font-size: 0.82rem; }
    .delta-pos { color: #5FD17A; }
    .delta-neg { color: #FF6B6B; }

    .section-header {
        font-size: 1.1rem; font-weight: 600; color: #1e3a5f;
        border-bottom: 2px solid #5F9ED1; padding-bottom: 6px; margin: 20px 0 12px;
    }
    .how-to-box {
        background: #f0f6ff; border-left: 4px solid #006BA4;
        border-radius: 6px; padding: 14px 18px; margin-bottom: 14px;
        font-size: 0.85rem; color: #334;
    }
    .rag-badge {
        display: inline-block; padding: 4px 12px; border-radius: 20px;
        font-size: 0.78rem; font-weight: 600; margin: 2px;
    }
    /* Sidebar dark */
    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0d2137 0%, #1e3a5f 100%);
    }
    [data-testid="stSidebar"] label, [data-testid="stSidebar"] .stMarkdown { color: #A2C8EC !important; }
    [data-testid="stSidebar"] h1, [data-testid="stSidebar"] h2 { color: #fff !important; }
</style>
""", unsafe_allow_html=True)


# ── data loaders ───────────────────────────────────────────────────────────────

@st.cache_data(ttl=60)
def load_silver_ops() -> pd.DataFrame:
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM silver_internal_operations", conn)
    conn.close()
    df["record_date"] = pd.to_datetime(df["record_date"])
    return df


@st.cache_data(ttl=60)
def load_gold_csv(name: str) -> pd.DataFrame:
    path = GOLD_DIR / f"{name}_latest.csv"
    if path.exists():
        return pd.read_csv(path)
    return pd.DataFrame()


@st.cache_data(ttl=300)
def load_forecast() -> pd.DataFrame:
    path = GOLD_DIR / "arima_forecast_latest.csv"
    if path.exists():
        df = pd.read_csv(path)
        df["date"] = pd.to_datetime(df["date"])
        return df
    return pd.DataFrame()


@st.cache_data(ttl=300)
def load_anomalies() -> pd.DataFrame:
    path = GOLD_DIR / "anomaly_detection_latest.csv"
    if path.exists():
        df = pd.read_csv(path)
        df["record_date"] = pd.to_datetime(df["record_date"])
        return df
    return pd.DataFrame()


@st.cache_data(ttl=300)
def load_clusters() -> pd.DataFrame:
    return load_gold_csv("cluster_labels")


@st.cache_data(ttl=300)
def load_corr() -> pd.DataFrame:
    path = GOLD_DIR / "correlation_matrix_latest.csv"
    if path.exists():
        df = pd.read_csv(path, index_col=0)
        return df
    return pd.DataFrame()


@st.cache_data(ttl=300)
def load_decomposition() -> pd.DataFrame:
    path = GOLD_DIR / "timeseries_decomposition_latest.csv"
    if path.exists():
        df = pd.read_csv(path)
        df["date"] = pd.to_datetime(df["date"])
        return df
    return pd.DataFrame()


@st.cache_data(ttl=300)
def load_sla_forecast() -> pd.DataFrame:
    path = GOLD_DIR / "sla_breach_forecast_latest.csv"
    if path.exists():
        df = pd.read_csv(path)
        df["date"] = pd.to_datetime(df["date"])
        return df
    return pd.DataFrame()


# ── sidebar filters ────────────────────────────────────────────────────────────

def sidebar_filters(df: pd.DataFrame) -> tuple:
    st.sidebar.title("📊 Analytics Dashboard")
    st.sidebar.markdown("---")
    st.sidebar.subheader("Global Filters")

    min_date = df["record_date"].min().date()
    max_date = df["record_date"].max().date()
    date_range = st.sidebar.date_input("Date Range", value=(min_date, max_date), min_value=min_date, max_value=max_date)

    departments = ["All"] + sorted(df["department"].unique().tolist())
    dept = st.sidebar.selectbox("Business Unit / Department", departments)

    st.sidebar.markdown("---")
    st.sidebar.subheader("Quick Views")
    if st.sidebar.button("🔴 This Week's SLA Breaches"):
        st.session_state["quick_filter"] = "sla_breaches"
    if st.sidebar.button("📊 Top 10 Bottlenecks"):
        st.session_state["quick_filter"] = "bottlenecks"
    if st.sidebar.button("📈 Q1 vs Q2 Efficiency"):
        st.session_state["quick_filter"] = "q1q2"

    return date_range, dept


def apply_filters(df: pd.DataFrame, date_range, dept: str) -> pd.DataFrame:
    if len(date_range) == 2:
        df = df[(df["record_date"].dt.date >= date_range[0]) & (df["record_date"].dt.date <= date_range[1])]
    if dept != "All":
        df = df[df["department"] == dept]
    return df


# ── metric card helper ─────────────────────────────────────────────────────────

def metric_card(label: str, value: str, delta: str = "", positive_delta: bool = True):
    delta_class = "delta-pos" if positive_delta else "delta-neg"
    delta_icon = "▲" if positive_delta else "▼"
    delta_html = f'<div class="delta {delta_class}">{delta_icon} {delta}</div>' if delta else ""
    st.markdown(f"""
    <div class="metric-card">
        <div class="label">{label}</div>
        <div class="value">{value}</div>
        {delta_html}
    </div>""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────────────
# PAGE 1: Executive Summary
# ─────────────────────────────────────────────────────────────────────────────

def page_executive_summary(df: pd.DataFrame):
    st.title("📊 Executive Summary")
    st.markdown('<div class="how-to-box">📖 <b>How to read this page:</b> The KPI scorecards at the top show overall performance. Use the sidebar filters to narrow by date range or department. Positive changes (green ▲) indicate improvement; negative (red ▼) indicate regression.</div>', unsafe_allow_html=True)

    # KPIs
    total_throughput = df["throughput"].sum()
    avg_cycle_time = df["cycle_time_min"].mean()
    sla_compliance = df["sla_met"].mean() * 100
    error_rate = df["error_rate"].mean() * 100
    total_errors = df["error_count"].sum()

    c1, c2, c3, c4, c5 = st.columns(5)
    with c1: metric_card("Total Throughput", f"{total_throughput:,}", "Overall records processed")
    with c2: metric_card("Avg Cycle Time", f"{avg_cycle_time:.1f} min", f"Target < SLA", positive_delta=avg_cycle_time < 300)
    with c3: metric_card("SLA Compliance", f"{sla_compliance:.1f}%", f"Target ≥ 95%", positive_delta=sla_compliance >= 95)
    with c4: metric_card("Avg Error Rate", f"{error_rate:.2f}%", f"Lower is better", positive_delta=error_rate < 2)
    with c5: metric_card("Total Errors", f"{total_errors:,}", "Flagged process errors")

    st.markdown('<div class="section-header">Monthly KPI Trends</div>', unsafe_allow_html=True)
    monthly = df.copy()
    monthly["year_month"] = monthly["record_date"].dt.to_period("M").astype(str)
    monthly_agg = monthly.groupby("year_month").agg(
        throughput=("throughput", "sum"),
        compliance_rate=("sla_met", "mean"),
        error_rate=("error_rate", "mean"),
    ).reset_index()

    fig = make_subplots(rows=1, cols=2, subplot_titles=["Monthly Throughput", "SLA Compliance & Error Rate"])
    fig.add_trace(go.Bar(x=monthly_agg["year_month"], y=monthly_agg["throughput"],
                         marker_color=PALETTE[0], name="Throughput"), row=1, col=1)
    fig.add_trace(go.Scatter(x=monthly_agg["year_month"], y=monthly_agg["compliance_rate"] * 100,
                             mode="lines+markers", name="Compliance %", line=dict(color=PALETTE[1], width=2.5)), row=1, col=2)
    fig.add_trace(go.Scatter(x=monthly_agg["year_month"], y=monthly_agg["error_rate"] * 100,
                             mode="lines+markers", name="Error Rate %", line=dict(color="#FF6B6B", width=2, dash="dash")), row=1, col=2)
    fig.add_hline(y=95, line_dash="dot", line_color="green", annotation_text="95% target", row=1, col=2)
    fig.update_layout(height=380, showlegend=True, plot_bgcolor="#f9fbff", paper_bgcolor="#f9fbff",
                      font=dict(family="Inter"), legend=dict(bgcolor="rgba(0,0,0,0)"))
    st.plotly_chart(fig, use_container_width=True)

    st.markdown('<div class="section-header">Top 5 Underperforming Processes (by Avg Cycle Time)</div>', unsafe_allow_html=True)
    bottleneck = df.groupby(["department", "process_name"]).agg(
        avg_cycle=("cycle_time_min", "mean"),
        avg_errors=("error_count", "mean"),
        compliance=("sla_met", "mean"),
    ).reset_index().nlargest(5, "avg_cycle")

    fig2 = px.bar(bottleneck, x="avg_cycle", y="process_name", orientation="h",
                  color="department", color_discrete_map=DEPT_COLORS,
                  labels={"avg_cycle": "Avg Cycle Time (min)", "process_name": "Process"},
                  title="Top 5 Slowest Processes")
    fig2.update_layout(height=300, plot_bgcolor="#f9fbff", paper_bgcolor="#f9fbff",
                       font=dict(family="Inter"), yaxis=dict(autorange="reversed"))
    st.plotly_chart(fig2, use_container_width=True)


# ─────────────────────────────────────────────────────────────────────────────
# PAGE 2: Operational Efficiency Deep Dive
# ─────────────────────────────────────────────────────────────────────────────

def page_operational_efficiency(df: pd.DataFrame):
    st.title("⚙️ Operational Efficiency Deep Dive")
    st.markdown('<div class="how-to-box">📖 <b>How to read this page:</b> The dual-axis chart shows throughput (bars) vs. average cycle time (line) — ideally throughput goes up while cycle time stays flat or drops. The heatmap reveals weekly patterns by department.</div>', unsafe_allow_html=True)

    daily = df.groupby("record_date").agg(
        throughput=("throughput", "sum"),
        avg_cycle=("cycle_time_min", "mean"),
    ).reset_index()

    # Dual-axis chart
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Bar(x=daily["record_date"], y=daily["throughput"],
                         name="Throughput", marker_color=PALETTE[0], opacity=0.7), secondary_y=False)
    fig.add_trace(go.Scatter(x=daily["record_date"], y=daily["avg_cycle"],
                             name="Avg Cycle Time (min)", line=dict(color=PALETTE[1], width=2)),
                  secondary_y=True)
    fig.update_layout(title="Daily Throughput vs. Average Cycle Time",
                      height=380, plot_bgcolor="#f9fbff", paper_bgcolor="#f9fbff", font=dict(family="Inter"))
    fig.update_yaxes(title_text="Total Throughput", secondary_y=False)
    fig.update_yaxes(title_text="Avg Cycle Time (min)", secondary_y=True)
    st.plotly_chart(fig, use_container_width=True)

    col1, col2 = st.columns(2)
    with col1:
        # Department heatmap by week
        df["week"] = df["record_date"].dt.isocalendar().week.astype(int)
        heat = df.groupby(["department", "week"])["sla_met"].mean().reset_index()
        heat_pivot = heat.pivot(index="department", columns="week", values="sla_met")
        fig3 = px.imshow(heat_pivot, color_continuous_scale="RdYlGn", labels=dict(color="Compliance Rate"),
                         title="Weekly Compliance Heatmap by Department", aspect="auto",
                         zmin=0, zmax=1)
        fig3.update_layout(height=350, font=dict(family="Inter"))
        st.plotly_chart(fig3, use_container_width=True)

    with col2:
        # Bottleneck bar chart with drill-down
        bottleneck = df.groupby(["department", "process_name"])["cycle_time_min"].mean().reset_index()
        bottleneck = bottleneck.nlargest(10, "cycle_time_min")
        fig4 = px.bar(bottleneck, x="cycle_time_min", y="process_name", orientation="h",
                      color="department", color_discrete_map=DEPT_COLORS,
                      title="Top 10 Process Bottlenecks (Avg Cycle Time)",
                      labels={"cycle_time_min": "Avg Cycle Time (min)", "process_name": "Process"})
        fig4.update_layout(height=350, font=dict(family="Inter"), yaxis=dict(autorange="reversed"),
                           plot_bgcolor="#f9fbff", paper_bgcolor="#f9fbff")
        st.plotly_chart(fig4, use_container_width=True)

    # Process step Gantt-style duration analysis (simulate using box plot)
    st.markdown('<div class="section-header">Cycle Time Distribution by Process</div>', unsafe_allow_html=True)
    top_procs = df["process_name"].value_counts().head(8).index.tolist()
    fig5 = px.box(df[df["process_name"].isin(top_procs)], x="cycle_time_min", y="process_name",
                  color="department", color_discrete_map=DEPT_COLORS,
                  labels={"cycle_time_min": "Cycle Time (min)", "process_name": "Process"},
                  title="Cycle Time Distribution (Box Plot) — Top 8 Processes")
    fig5.update_layout(height=370, plot_bgcolor="#f9fbff", paper_bgcolor="#f9fbff",
                       font=dict(family="Inter"), yaxis=dict(autorange="reversed"))
    st.plotly_chart(fig5, use_container_width=True)


# ─────────────────────────────────────────────────────────────────────────────
# PAGE 3: SLA & Compliance Monitoring
# ─────────────────────────────────────────────────────────────────────────────

def page_sla_compliance(df: pd.DataFrame):
    st.title("🚦 SLA & Compliance Monitoring")
    st.markdown('<div class="how-to-box">📖 <b>How to read this page:</b> The compliance trend should stay above the red 95% target line. Traffic lights show at-a-glance status per team. Click any bar to drill from team level to process level.</div>', unsafe_allow_html=True)

    daily_sla = df.groupby("record_date")["sla_met"].mean().reset_index()
    daily_sla.columns = ["record_date", "compliance_rate"]

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=daily_sla["record_date"], y=daily_sla["compliance_rate"] * 100,
                             fill="tozeroy", line=dict(color=PALETTE[0], width=2),
                             fillcolor="rgba(0,107,164,0.15)", name="Compliance %"))
    fig.add_hline(y=95, line_dash="dash", line_color="red", annotation_text="95% Target")
    fig.update_layout(title="Daily SLA Compliance Rate", yaxis_ticksuffix="%",
                      height=330, plot_bgcolor="#f9fbff", paper_bgcolor="#f9fbff", font=dict(family="Inter"))
    st.plotly_chart(fig, use_container_width=True)

    col1, col2 = st.columns(2)
    with col1:
        dept_sla = df.groupby("department")["sla_met"].agg(
            total="count", met="sum"
        ).reset_index()
        dept_sla["compliance_rate"] = dept_sla["met"] / dept_sla["total"] * 100
        dept_sla["breach_count"] = dept_sla["total"] - dept_sla["met"]

        fig2 = px.bar(dept_sla, x="department", y="breach_count", color="compliance_rate",
                      color_continuous_scale="RdYlGn", title="SLA Breaches & Compliance by Department",
                      labels={"breach_count": "Breach Count", "compliance_rate": "Compliance %"})
        fig2.update_layout(height=320, plot_bgcolor="#f9fbff", paper_bgcolor="#f9fbff", font=dict(family="Inter"))
        st.plotly_chart(fig2, use_container_width=True)

    with col2:
        st.markdown("**Traffic Light Status by Department**")
        for _, row in dept_sla.iterrows():
            rate = row["compliance_rate"]
            if rate >= 95:
                color, label = "#22c55e", "🟢 GREEN"
            elif rate >= 90:
                color, label = "#f59e0b", "🟡 AMBER"
            else:
                color, label = "#ef4444", "🔴 RED"
            st.markdown(
                f'<span class="rag-badge" style="background:{color}22;color:{color};border:1px solid {color}">'
                f'{label} &nbsp; <b>{row["department"]}</b> &nbsp; {rate:.1f}%</span>',
                unsafe_allow_html=True,
            )
        # SLA breach by day of week
        df["day_of_week"] = df["record_date"].dt.day_name()
        dow_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        dow_sla = df.groupby("day_of_week")["sla_met"].apply(lambda x: (x == 0).sum()).reset_index()
        dow_sla.columns = ["day_of_week", "breach_count"]
        dow_sla["day_of_week"] = pd.Categorical(dow_sla["day_of_week"], categories=dow_order, ordered=True)
        dow_sla = dow_sla.sort_values("day_of_week")
        fig3 = px.bar(dow_sla, x="day_of_week", y="breach_count",
                      title="Breach Count by Day of Week",
                      color="breach_count", color_continuous_scale="Reds")
        fig3.update_layout(height=250, plot_bgcolor="#f9fbff", paper_bgcolor="#f9fbff", font=dict(family="Inter"))
        st.plotly_chart(fig3, use_container_width=True)

    # Drill down: select department → show process breakdown
    st.markdown('<div class="section-header">Drill Down: Department → Process Level</div>', unsafe_allow_html=True)
    selected_dept = st.selectbox("Select Department for Drill-Down", sorted(df["department"].unique()))
    proc_sla = df[df["department"] == selected_dept].groupby("process_name")["sla_met"].agg(
        total="count", met="sum"
    ).reset_index()
    proc_sla["compliance_rate"] = proc_sla["met"] / proc_sla["total"] * 100
    proc_sla["breach_count"] = proc_sla["total"] - proc_sla["met"]
    fig4 = px.bar(proc_sla, x="process_name", y=["met", "breach_count"],
                  barmode="stack", labels={"value": "Records", "process_name": "Process"},
                  color_discrete_map={"met": "#22c55e", "breach_count": "#ef4444"},
                  title=f"SLA Breakdown: {selected_dept}")
    fig4.update_layout(height=320, plot_bgcolor="#f9fbff", paper_bgcolor="#f9fbff", font=dict(family="Inter"))
    st.plotly_chart(fig4, use_container_width=True)


# ─────────────────────────────────────────────────────────────────────────────
# PAGE 4: Financial Performance
# ─────────────────────────────────────────────────────────────────────────────

def page_financial_performance(df: pd.DataFrame):
    st.title("💰 Financial Performance")
    st.markdown('<div class="how-to-box">📖 <b>How to read this page:</b> Financials are derived from operational metrics using assumptions (cost per error, revenue per throughput unit). Red dots on the cost trend indicate anomalies flagged by the model.</div>', unsafe_allow_html=True)

    # Simulate financial metrics from operational data
    fin = df.groupby(["department", "record_date"]).agg(
        throughput=("throughput", "sum"),
        errors=("error_count", "sum"),
        cycle_time=("cycle_time_min", "mean"),
    ).reset_index()
    fin["revenue"] = fin["throughput"] * 25          # $25 per unit
    fin["cost"] = fin["errors"] * 150 + fin["cycle_time"] * fin["throughput"] * 0.05
    fin["operating_margin"] = ((fin["revenue"] - fin["cost"]) / fin["revenue"] * 100).clip(-100, 100)
    fin["cost_per_txn"] = (fin["cost"] / fin["throughput"].clip(lower=1)).round(2)

    dept_fin = fin.groupby("department").agg(
        revenue=("revenue", "sum"),
        cost=("cost", "sum"),
    ).reset_index()
    dept_fin["margin"] = ((dept_fin["revenue"] - dept_fin["cost"]) / dept_fin["revenue"] * 100).round(2)
    dept_fin = dept_fin.sort_values("revenue", ascending=False)

    col1, col2 = st.columns(2)
    with col1:
        # Waterfall
        items = list(dept_fin["department"]) + ["Net"]
        values = list(dept_fin["revenue"] - dept_fin["cost"])
        waterfall_vals = values + [sum(values)]
        fig = go.Figure(go.Waterfall(
            name="Operating Profit", orientation="v",
            measure=["relative"] * len(dept_fin) + ["total"],
            x=items, y=waterfall_vals,
            connector={"line": {"color": "#888"}},
            increasing={"marker": {"color": "#22c55e"}},
            decreasing={"marker": {"color": "#ef4444"}},
            totals={"marker": {"color": "#006BA4"}},
        ))
        fig.update_layout(title="Revenue − Cost Waterfall by Department",
                          height=380, plot_bgcolor="#f9fbff", paper_bgcolor="#f9fbff",
                          font=dict(family="Inter"), yaxis_tickprefix="$")
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        # Operating margin by department
        fig2 = px.bar(dept_fin, x="department", y="margin", color="margin",
                      color_continuous_scale="RdYlGn", title="Operating Margin (%) by Department",
                      labels={"margin": "Operating Margin (%)"})
        fig2.add_hline(y=0, line_dash="dash", line_color="#888")
        fig2.update_layout(height=380, plot_bgcolor="#f9fbff", paper_bgcolor="#f9fbff",
                           font=dict(family="Inter"))
        st.plotly_chart(fig2, use_container_width=True)

    # Cost per transaction trend with anomaly overlay
    st.markdown('<div class="section-header">Cost per Transaction Trend (with Anomaly Flags)</div>', unsafe_allow_html=True)
    daily_cost = fin.groupby("record_date").agg(
        cost_per_txn=("cost_per_txn", "mean"),
    ).reset_index()
    z = (daily_cost["cost_per_txn"] - daily_cost["cost_per_txn"].mean()) / daily_cost["cost_per_txn"].std()
    daily_cost["is_anomaly"] = (z.abs() > 2.5).astype(bool)

    fig3 = go.Figure()
    fig3.add_trace(go.Scatter(x=daily_cost["record_date"], y=daily_cost["cost_per_txn"],
                              mode="lines", name="Cost / Txn", line=dict(color=PALETTE[0], width=2)))
    anomalies = daily_cost[daily_cost["is_anomaly"]]
    fig3.add_trace(go.Scatter(x=anomalies["record_date"], y=anomalies["cost_per_txn"],
                              mode="markers", name="Anomaly", marker=dict(color="red", size=10, symbol="x")))
    fig3.update_layout(title="Daily Avg Cost per Transaction", height=320,
                       plot_bgcolor="#f9fbff", paper_bgcolor="#f9fbff",
                       yaxis_tickprefix="$", font=dict(family="Inter"))
    st.plotly_chart(fig3, use_container_width=True)

    # ARIMA forecast overlay on margin
    forecast = load_forecast()
    if not forecast.empty:
        monthly_margin = fin.groupby(fin["record_date"].dt.to_period("M")).agg(
            revenue=("revenue", "sum"), cost=("cost", "sum")
        ).reset_index()
        monthly_margin["date"] = monthly_margin["record_date"].dt.to_timestamp()
        monthly_margin["margin"] = ((monthly_margin["revenue"] - monthly_margin["cost"]) / monthly_margin["revenue"] * 100)
        fig4 = px.line(monthly_margin, x="date", y="margin",
                       title="Monthly Operating Margin (%) with Trend",
                       labels={"margin": "Operating Margin (%)", "date": "Month"})
        fig4.update_layout(height=300, plot_bgcolor="#f9fbff", paper_bgcolor="#f9fbff",
                           yaxis_ticksuffix="%", font=dict(family="Inter"))
        st.plotly_chart(fig4, use_container_width=True)


# ─────────────────────────────────────────────────────────────────────────────
# PAGE 5: Trend Analysis & Forecasting
# ─────────────────────────────────────────────────────────────────────────────

def page_trend_forecasting():
    st.title("📈 Trend Analysis & Forecasting")
    st.markdown('<div class="how-to-box">📖 <b>How to read this page:</b> The top chart shows historical throughput with a 30-day ARIMA forecast and 95% confidence interval (shaded). Anomalies are shown as red ✕ markers. The correlation heatmap reveals relationships between key metrics.</div>', unsafe_allow_html=True)

    forecast = load_forecast()
    if not forecast.empty:
        fig = go.Figure()
        hist = forecast.dropna(subset=["actual"])
        fcast = forecast.dropna(subset=["forecast"])
        fig.add_trace(go.Scatter(x=hist["date"], y=hist["actual"],
                                 name="Actual Throughput", line=dict(color=PALETTE[0], width=2)))
        fig.add_trace(go.Scatter(x=fcast["date"], y=fcast["forecast"],
                                 name="ARIMA Forecast", line=dict(color=PALETTE[1], width=2, dash="dash")))
        if "upper_95" in fcast.columns:
            fig.add_trace(go.Scatter(
                x=pd.concat([fcast["date"], fcast["date"][::-1]]),
                y=pd.concat([fcast["upper_95"], fcast["lower_95"][::-1]]),
                fill="toself", fillcolor="rgba(255,128,14,0.15)", line=dict(color="rgba(255,255,255,0)"),
                name="95% Confidence Interval",
            ))
        fig.update_layout(title="30-Day ARIMA Throughput Forecast", height=380,
                          plot_bgcolor="#f9fbff", paper_bgcolor="#f9fbff", font=dict(family="Inter"))
        st.plotly_chart(fig, use_container_width=True)

    decomp = load_decomposition()
    if not decomp.empty:
        st.markdown('<div class="section-header">Time Series Decomposition (Trend · Seasonal · Residual)</div>', unsafe_allow_html=True)
        fig2 = make_subplots(rows=3, cols=1, shared_xaxes=True,
                             subplot_titles=["Trend", "Seasonality", "Residual"])
        fig2.add_trace(go.Scatter(x=decomp["date"], y=decomp["trend"], name="Trend",
                                  line=dict(color=PALETTE[0])), row=1, col=1)
        fig2.add_trace(go.Scatter(x=decomp["date"], y=decomp["seasonal"], name="Seasonal",
                                  line=dict(color=PALETTE[1])), row=2, col=1)
        fig2.add_trace(go.Scatter(x=decomp["date"], y=decomp["residual"], name="Residual",
                                  line=dict(color=PALETTE[4])), row=3, col=1)
        fig2.update_layout(height=480, showlegend=False, plot_bgcolor="#f9fbff",
                           paper_bgcolor="#f9fbff", font=dict(family="Inter"))
        st.plotly_chart(fig2, use_container_width=True)

    # Anomaly scatter
    anomalies = load_anomalies()
    if not anomalies.empty:
        st.markdown('<div class="section-header">Anomaly Detection — Flagged Records</div>', unsafe_allow_html=True)
        fig3 = go.Figure()
        normal = anomalies[anomalies["is_anomaly"] == 0]
        anoms = anomalies[anomalies["is_anomaly"] == 1]
        fig3.add_trace(go.Scatter(x=normal["record_date"], y=normal["throughput"],
                                  mode="markers", name="Normal", marker=dict(color=PALETTE[0], size=4, opacity=0.4)))
        fig3.add_trace(go.Scatter(x=anoms["record_date"], y=anoms["throughput"],
                                  mode="markers", name="Anomaly",
                                  marker=dict(color="red", size=9, symbol="x")))
        fig3.update_layout(title="Process Anomalies on Daily Throughput", height=320,
                           plot_bgcolor="#f9fbff", paper_bgcolor="#f9fbff", font=dict(family="Inter"))
        st.plotly_chart(fig3, use_container_width=True)

    # Correlation matrix
    corr = load_corr()
    if not corr.empty:
        st.markdown('<div class="section-header">Pearson Correlation Matrix — Key Operational Metrics</div>', unsafe_allow_html=True)
        fig4 = px.imshow(corr, color_continuous_scale="RdBu_r", zmin=-1, zmax=1,
                         text_auto=".2f", title="Metric Correlation Heatmap", aspect="auto")
        fig4.update_layout(height=400, font=dict(family="Inter"))
        st.plotly_chart(fig4, use_container_width=True)

    # Cluster visualization
    clusters = load_clusters()
    if not clusters.empty:
        st.markdown('<div class="section-header">K-Means Process Clusters (Operational Benchmarking)</div>', unsafe_allow_html=True)
        fig5 = px.scatter(clusters, x="avg_cycle_time", y="avg_throughput",
                          color="cluster", symbol="department",
                          hover_data=["process_name", "avg_error_rate", "compliance_rate"],
                          color_continuous_scale=px.colors.qualitative.Set2,
                          title="Processes Clustered by Operational Behavior",
                          labels={"avg_cycle_time": "Avg Cycle Time (min)", "avg_throughput": "Avg Throughput"})
        fig5.update_layout(height=420, plot_bgcolor="#f9fbff", paper_bgcolor="#f9fbff",
                           font=dict(family="Inter"))
        st.plotly_chart(fig5, use_container_width=True)

    # SLA breach forecast
    sla_fcast = load_sla_forecast()
    if not sla_fcast.empty:
        st.markdown('<div class="section-header">30-Day SLA Breach Rate Forecast</div>', unsafe_allow_html=True)
        fig6 = go.Figure()
        fig6.add_trace(go.Scatter(x=sla_fcast["date"], y=sla_fcast["breach_rate_forecast"] * 100,
                                  name="Forecasted Breach Rate %", line=dict(color=PALETTE[2], width=2.5, dash="dash")))
        fig6.add_trace(go.Scatter(
            x=pd.concat([sla_fcast["date"], sla_fcast["date"][::-1]]),
            y=pd.concat([sla_fcast["upper_95"] * 100, sla_fcast["lower_95"][::-1] * 100]),
            fill="toself", fillcolor="rgba(90,90,90,0.12)", line=dict(color="rgba(0,0,0,0)"),
            name="95% CI",
        ))
        fig6.add_hline(y=5, line_dash="dot", line_color="red", annotation_text="5% breach threshold")
        fig6.update_layout(title="SLA Breach Rate Forecast (next 30 days)", height=320,
                           yaxis_ticksuffix="%", plot_bgcolor="#f9fbff", paper_bgcolor="#f9fbff",
                           font=dict(family="Inter"))
        st.plotly_chart(fig6, use_container_width=True)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN ROUTER
# ─────────────────────────────────────────────────────────────────────────────

def main():
    df_raw = load_silver_ops()
    if df_raw.empty:
        st.error("⚠️ No data found. Please run `python scripts/run_pipeline.py` first to load data.")
        st.stop()

    date_range, dept = sidebar_filters(df_raw)
    df = apply_filters(df_raw, date_range, dept)

    pages = {
        "📊 Executive Summary":               page_executive_summary,
        "⚙️ Operational Efficiency":          page_operational_efficiency,
        "🚦 SLA & Compliance Monitoring":     page_sla_compliance,
        "💰 Financial Performance":           page_financial_performance,
        "📈 Trend Analysis & Forecasting":    page_trend_forecasting,
    }

    st.sidebar.markdown("---")
    st.sidebar.subheader("Dashboard Pages")
    page_choice = st.sidebar.radio("Navigate to", list(pages.keys()), label_visibility="collapsed")
    st.sidebar.markdown("---")
    st.sidebar.caption(f"Data last loaded: {df_raw['record_date'].max().strftime('%b %d, %Y')}")
    st.sidebar.caption(f"Records in view: {len(df):,}")

    if page_choice == "📈 Trend Analysis & Forecasting":
        pages[page_choice]()
    else:
        pages[page_choice](df)


if __name__ == "__main__":
    main()
