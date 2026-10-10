"""
NYC Taxi Analytics Dashboard
Author: Parth Alagh | USICT GGSIPU

Configuration (environment variables):
  MODEL_PATH   — path to xgboost_demand_model.pkl
  METRICS_PATH — path to evaluation_metrics.json
  S3_STAGING   — Athena query results S3 URI
  AWS_REGION   — AWS region (default: us-east-1)
"""

import os
import json
import pickle
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor

# ── Paths (env-configurable, no hardcoded machine paths) ──────
_HERE = Path(__file__).parent
MODEL_PATH   = Path(os.environ.get("MODEL_PATH",   str(_HERE / "model" / "xgboost_demand_model.pkl")))
METRICS_PATH = Path(os.environ.get("METRICS_PATH", str(_HERE / "model" / "evaluation_metrics.json")))
ZONES_CSV    = _HERE / "taxi_zone_lookup.csv"
S3_STAGING   = os.environ.get("S3_STAGING", "s3://nyc-taxi-de-parth-2026/athena-results/")
AWS_REGION   = os.environ.get("AWS_REGION", "us-east-1")

st.set_page_config(
    page_title="NYC Taxi Analytics",
    page_icon=None,
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ── CSS ───────────────────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=Space+Grotesk:wght@500;600;700&display=swap');

    :root {
        --page: #0d1117;
        --surface: #141a22;
        --surface-raised: #181f29;
        --border: #293241;
        --text: #f2f4f7;
        --muted: #9aa6b5;
        --blue: #4f8dff;
        --taxi: #f5c518;
    }

    html, body, [class*="css"] {
        font-family: 'DM Sans', -apple-system, BlinkMacSystemFont, sans-serif;
        color: var(--text);
    }

    .stApp, [data-testid="stAppViewContainer"] { background: var(--page); }
    .block-container { padding: 2.5rem 3rem 2rem; max-width: 1500px; }

    .dash-header {
        position: relative;
        border-bottom: 1px solid var(--border);
        padding: 0 0 1.6rem 1.1rem;
        margin-bottom: 1.8rem;
    }
    .dash-header:before {
        content: "";
        position: absolute;
        left: 0; top: 0.2rem; bottom: 1.65rem;
        width: 3px;
        background: var(--taxi);
    }
    .dash-title {
        font-family: 'Space Grotesk', sans-serif;
        font-size: clamp(2.3rem, 4vw, 3.35rem) !important;
        font-weight: 700 !important;
        color: var(--text) !important;
        letter-spacing: -0.055em !important;
        margin: 0 !important;
        line-height: 1.05 !important;
    }
    .dash-sub {
        font-size: 0.78rem;
        color: var(--muted);
        margin-top: 0.65rem;
        font-weight: 500;
        letter-spacing: 0.035em;
        text-transform: uppercase;
    }

    .section-label {
        font-family: 'Space Grotesk', sans-serif;
        font-size: 0.9rem;
        font-weight: 600;
        letter-spacing: 0.015em;
        color: var(--text);
        margin: 0 0 0.65rem 0;
        padding-left: 0.7rem;
        border-left: 2px solid var(--blue);
    }

    .metric-grid {
        display: grid;
        grid-template-columns: repeat(4, minmax(0, 1fr));
        gap: 0.8rem;
        margin-bottom: 2.35rem;
    }
    .metric-card {
        background: var(--surface);
        border: 1px solid var(--border);
        border-top: 2px solid #354154;
        border-radius: 5px;
        padding: 1.15rem 1.3rem 1.2rem;
        min-height: 112px;
    }
    .metric-card:first-child { border-top-color: var(--taxi); }
    .metric-label {
        font-size: 0.7rem;
        color: var(--muted);
        text-transform: uppercase;
        letter-spacing: 0.09em;
        font-weight: 600;
        margin-bottom: 0.55rem;
    }
    .metric-value {
        font-family: 'Space Grotesk', sans-serif;
        font-size: clamp(1.45rem, 2vw, 1.8rem);
        font-weight: 600;
        color: var(--text);
        letter-spacing: -0.045em;
        line-height: 1.2;
    }
    .metric-sub { font-size: 0.74rem; color: var(--muted); margin-top: 0.4rem; }

    [data-testid="stPlotlyChart"] {
        background: var(--surface);
        border: 1px solid var(--border);
        border-radius: 5px;
        padding: 0.55rem 0.55rem 0.1rem;
        margin-bottom: 1.3rem;
    }

    .pred-box {
        background: var(--surface);
        border: 1px solid var(--border);
        border-left: 3px solid var(--taxi);
        border-radius: 5px;
        padding: 1.4rem 1.5rem;
        margin-top: 1rem;
    }
    .pred-result {
        font-family: 'Space Grotesk', sans-serif;
        font-size: 2rem;
        font-weight: 600;
        color: var(--text);
        letter-spacing: -0.035em;
        margin-top: 0.2rem;
    }
    .pred-result-sub { font-size: 0.82rem; color: var(--muted); margin-top: 0.35rem; }
    .pred-note { font-size: 0.78rem; color: var(--muted); margin-top: 0.8rem; border-top: 1px solid var(--border); padding-top: 0.7rem; }

    .stButton > button {
        background: var(--taxi) !important;
        color: #171717 !important;
        border: 1px solid var(--taxi) !important;
        border-radius: 3px !important;
        font-size: 0.85rem !important;
        font-weight: 700 !important;
        padding: 0.55rem 1.15rem !important;
    }
    .stButton > button:hover {
        background: #ffd84d !important;
        border-color: #ffd84d !important;
    }

    [data-testid="stNumberInput"] input,
    [data-testid="stSelectbox"] [data-baseweb="select"] > div {
        background: var(--surface) !important;
        border-color: var(--border) !important;
        border-radius: 3px !important;
    }
    [data-testid="stWidgetLabel"] p { color: #c4ccd6 !important; font-size: 0.82rem !important; }
    .stDivider, hr { border-color: var(--border) !important; }
    footer { display: none !important; }
    #MainMenu { display: none !important; }

    @media (max-width: 900px) {
        .block-container { padding: 1.5rem 1.2rem; }
        .metric-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    }
</style>
""", unsafe_allow_html=True)

# ── Header ────────────────────────────────────────────────────
st.markdown("""
<div class="dash-header">
    <p class="dash-title">NYC Taxi Analytics</p>
    <p class="dash-sub">PYSPARK / AWS S3 / ATHENA / DBT / XGBOOST &nbsp;|&nbsp; JANUARY 2023 DATASET</p>
</div>
""", unsafe_allow_html=True)

# ── Athena Connection ─────────────────────────────────────────
@st.cache_resource
def get_connection():
    return connect(
        s3_staging_dir=S3_STAGING,
        region_name=AWS_REGION,
        cursor_class=PandasCursor
    )

@st.cache_data(ttl=3600)
def run_query(sql):
    return get_connection().cursor().execute(sql).as_pandas()

@st.cache_resource
def load_model():
    with MODEL_PATH.open("rb") as f:
        return pickle.load(f)

@st.cache_data
def load_metrics():
    if METRICS_PATH.exists():
        with METRICS_PATH.open() as f:
            return json.load(f)
    return {"r2": "N/A", "rmse": "N/A"}

@st.cache_data
def load_zones():
    if ZONES_CSV.exists():
        z = pd.read_csv(ZONES_CSV)
        z["label"] = z["Borough"] + " · " + z["Zone"]
        return dict(zip(z["LocationID"], z["label"]))
    return {}

CHART_LAYOUT = dict(
    template="plotly_dark",
    plot_bgcolor="#141a22",
    paper_bgcolor="#141a22",
    font=dict(color="#c4ccd6", family="DM Sans, sans-serif", size=12),
    margin=dict(l=12, r=14, t=28, b=12),
    hoverlabel=dict(bgcolor="#202a37", bordercolor="#4f8dff",
                    font=dict(color="#f2f4f7", family="DM Sans, sans-serif", size=12)),
)
AXIS = dict(gridcolor="#293241", linecolor="#293241", tickcolor="#465366",
            tickfont=dict(color="#9aa6b5"), zeroline=False)

# ── Load data ─────────────────────────────────────────────────
with st.spinner("Querying Athena..."):
    kpi_df = run_query("""
        SELECT
            SUM(trip_count)                                AS total_trips,
            SUM(avg_fare * trip_count) / SUM(trip_count)  AS weighted_avg_fare,
            SUM(avg_duration_mins * trip_count) / SUM(trip_count) AS weighted_avg_duration
        FROM nyc_taxi.trips_hourly_demand
    """)
    hourly_df = run_query("""
        SELECT pickup_hour, SUM(trip_count) AS total_trips
        FROM nyc_taxi.trips_hourly_demand
        GROUP BY pickup_hour ORDER BY pickup_hour
    """)
    zone_df = run_query("""
        SELECT pulocationid, SUM(trip_count) AS total_trips
        FROM nyc_taxi.trips_hourly_demand
        GROUP BY pulocationid ORDER BY total_trips DESC LIMIT 10
    """)
    heatmap_df = run_query("""
        SELECT pickup_day_of_week, pickup_hour,
               SUM(avg_fare * trip_count) / SUM(trip_count) AS avg_fare
        FROM nyc_taxi.trips_hourly_demand
        GROUP BY pickup_day_of_week, pickup_hour
        ORDER BY pickup_day_of_week, pickup_hour
    """)
    duration_df = run_query("""
        SELECT avg_duration_mins FROM nyc_taxi.trips_hourly_demand
        WHERE avg_duration_mins BETWEEN 1 AND 120
    """)

metrics   = load_metrics()
zone_map  = load_zones()
total_trips       = int(kpi_df["total_trips"].iloc[0])
weighted_fare     = float(kpi_df["weighted_avg_fare"].iloc[0])
weighted_duration = float(kpi_df["weighted_avg_duration"].iloc[0])
r2_val = metrics.get("r2", "N/A")

# ── KPI cards ─────────────────────────────────────────────────
st.markdown(f"""
<div class="metric-grid">
    <div class="metric-card">
        <div class="metric-label">Trips Processed</div>
        <div class="metric-value">{total_trips:,}</div>
        <div class="metric-sub">After PySpark cleaning</div>
    </div>
    <div class="metric-card">
        <div class="metric-label">Weighted Avg Fare</div>
        <div class="metric-value">${weighted_fare:.2f}</div>
        <div class="metric-sub">Weighted by trip volume</div>
    </div>
    <div class="metric-card">
        <div class="metric-label">Weighted Avg Duration</div>
        <div class="metric-value">{weighted_duration:.1f} min</div>
        <div class="metric-sub">Weighted by trip volume</div>
    </div>
    <div class="metric-card">
        <div class="metric-label">Model R&sup2; (temporal holdout)</div>
        <div class="metric-value">{r2_val}</div>
        <div class="metric-sub">XGBoost demand prediction</div>
    </div>
</div>
""", unsafe_allow_html=True)

# ── Row 1 ─────────────────────────────────────────────────────
col1, col2 = st.columns(2, gap="large")

with col1:
    st.markdown('<p class="section-label">Trip Volume by Hour of Day</p>', unsafe_allow_html=True)
    fig1 = px.line(hourly_df, x="pickup_hour", y="total_trips",
                   markers=True, labels={"pickup_hour": "Hour", "total_trips": "Trips"})
    fig1.update_traces(
        line=dict(color="#4f8dff", width=2.5),
        marker=dict(color="#f5c518", size=5, line=dict(width=1, color="#141a22")),
        hovertemplate="Hour: %{x}:00<br>Trips: %{y:,.0f}<extra></extra>"
    )
    fig1.update_layout(**CHART_LAYOUT)
    fig1.update_xaxes(**AXIS, tickmode="linear", dtick=3)
    fig1.update_yaxes(**AXIS)
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    st.markdown('<p class="section-label">Top 10 Pickup Zones</p>', unsafe_allow_html=True)
    zone_df["zone_name"] = zone_df["pulocationid"].map(zone_map).fillna(
        "Zone " + zone_df["pulocationid"].astype(str)
    )
    fig2 = px.bar(zone_df, x="total_trips", y="zone_name", orientation="h",
                  labels={"zone_name": "", "total_trips": "Trips"})
    fig2.update_traces(
        marker_color="#4f8dff", marker_line_width=0,
        hovertemplate="%{y}<br>Trips: %{x:,.0f}<extra></extra>"
    )
    fig2.update_layout(**CHART_LAYOUT)
    fig2.update_xaxes(**AXIS)
    fig2.update_yaxes(**AXIS, autorange="reversed")
    st.plotly_chart(fig2, use_container_width=True)

# ── Row 2 ─────────────────────────────────────────────────────
col3, col4 = st.columns(2, gap="large")

with col3:
    st.markdown('<p class="section-label">Avg Fare by Day and Hour (weighted by trip volume)</p>',
                unsafe_allow_html=True)
    # Explicit Mon-Sun ordering
    DAY_ORDER = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    day_map_int = {1:"Mon", 2:"Tue", 3:"Wed", 4:"Thu", 5:"Fri", 6:"Sat", 7:"Sun"}
    heatmap_df["day"] = heatmap_df["pickup_day_of_week"].map(day_map_int)
    pivot = (heatmap_df.pivot(index="day", columns="pickup_hour", values="avg_fare")
                       .reindex(DAY_ORDER))
    fig3 = px.imshow(pivot,
                     labels=dict(x="Hour", y="Day", color="Avg Fare ($)"),
                     color_continuous_scale=[[0, "#18202b"], [0.5, "#2859a8"], [1, "#f5c518"]],
                     aspect="auto")
    fig3.update_layout(**CHART_LAYOUT)
    fig3.update_xaxes(**AXIS)
    fig3.update_yaxes(**AXIS)
    st.plotly_chart(fig3, use_container_width=True)

with col4:
    st.markdown('<p class="section-label">Distribution of Avg Trip Duration per Zone-Hour</p>',
                unsafe_allow_html=True)
    fig4 = px.histogram(duration_df, x="avg_duration_mins", nbins=50,
                        labels={"avg_duration_mins": "Avg Duration (mins)"})
    fig4.update_traces(marker_color="#4f8dff", marker_line_width=0, opacity=0.92)
    fig4.update_layout(**CHART_LAYOUT)
    fig4.update_xaxes(**AXIS)
    fig4.update_yaxes(**AXIS)
    st.plotly_chart(fig4, use_container_width=True)

# ── Prediction ────────────────────────────────────────────────
st.markdown('<hr style="border-color:#293241; margin: 2rem 0;">', unsafe_allow_html=True)
st.markdown('<p class="section-label">Demand Prediction</p>', unsafe_allow_html=True)
st.caption(
    "Predicts the number of taxi pickups for a given zone and time window. "
    "Uses only inputs available before trips occur: zone ID, hour, day, and month. "
    f"Evaluated on a temporal holdout — RMSE {metrics.get('rmse','N/A')} trips, "
    f"R² {metrics.get('r2','N/A')} (vs baseline RMSE {metrics.get('baseline_rmse','N/A')})."
)

model = load_model()

c1, c2, c3, c4 = st.columns(4)
zone_id     = c1.number_input("Zone ID (1-265)", min_value=1, max_value=265, value=132)
hour        = c2.number_input("Hour (0-23)", min_value=0, max_value=23, value=8)
day_of_week = c3.selectbox("Day",  [1,2,3,4,5,6,7],
                            format_func=lambda x: DAY_ORDER[x-1])
month       = c4.number_input("Month (1-12)", min_value=1, max_value=12, value=1)

if st.button("Run Prediction"):
    features = np.array([[hour, day_of_week, month, zone_id]])
    pred = int(model.predict(features)[0])
    zone_label = zone_map.get(zone_id, f"Zone {zone_id}")
    st.markdown(f"""
    <div class="pred-box">
        <div class="pred-result">{pred} trips</div>
        <div class="pred-result-sub">
            Predicted demand — {zone_label} — {DAY_ORDER[day_of_week-1]} at {hour:02d}:00
        </div>
        <div class="pred-note">
            This is a point estimate. The model predicts demand based on historical patterns
            for this zone and time. Actual demand may vary due to weather, events, or
            other factors not captured in the training data.
        </div>
    </div>
    """, unsafe_allow_html=True)

# ── Footer ────────────────────────────────────────────────────
st.markdown('<hr style="border-color:#293241; margin: 2.5rem 0 1rem;">', unsafe_allow_html=True)
st.markdown(
    '<p style="font-size:0.75rem; color:#7f8b9a; font-family: DM Sans, sans-serif;">'
    'Parth Alagh &nbsp;/&nbsp; USICT, GGSIPU &nbsp;/&nbsp; 2026'
    '</p>',
    unsafe_allow_html=True
)
