# NYC Taxi Analytics Pipeline

End-to-end batch data pipeline on AWS. Takes raw NYC TLC taxi data, cleans it at scale, transforms it with dbt, trains a demand prediction model, and serves results through a dashboard.

---

## What it does

Raw NYC TLC Yellow Taxi trip records (January 2023, ~3M rows) are processed through six layers:

1. **PySpark** — distributed cleaning: removes null fares, zero-distance trips, invalid passenger counts, and out-of-range timestamps
2. **AWS S3** — cleaned output stored as Parquet in a data lake
3. **AWS Athena** — serverless SQL over S3, no database server required
4. **dbt** — modular SQL transformations: staging model and hourly demand aggregation by pickup zone
5. **XGBoost** — demand prediction model trained on zone + time features only
6. **Streamlit** — live dashboard querying Athena directly

Orchestration is handled by an Apache Airflow DAG scheduled monthly.

---

## Model results

The XGBoost model predicts hourly taxi demand per pickup zone. Only features available before trips occur are used — no fare, distance, or duration data.

| Metric | Value |
|--------|-------|
| Features | Zone ID, hour, day of week, month |
| Validation | Temporal holdout (last 20% of days) |
| RMSE | 21.63 trips |
| MAE | 12.42 trips |
| R² | 0.91 |
| Baseline RMSE | 72.42 (mean predictor) |
| Improvement | 70.1% over baseline |

Feature importance: pickup zone (66%) > hour of day (21%) > day of week (8%) > month (5%).

Zone ID dominates — consistent with urban mobility research showing spatial clustering as the primary demand signal.

---

## Pipeline stats

| Stage | Count |
|-------|-------|
| Raw records | 3,066,766 |
| After cleaning | 2,881,179 |
| Drop rate | 6.0% |
| Avg fare | $18.54 |
| JFK trips (zone 132) | 151,831 |

---

## Stack

| Layer | Tool |
|-------|------|
| Processing | Apache PySpark 3.5 |
| Storage | AWS S3 |
| Query engine | AWS Athena (serverless, Presto) |
| Transformation | dbt-core + dbt-athena-community |
| ML | XGBoost, scikit-learn |
| Dashboard | Streamlit + Plotly |
| Orchestration | Apache Airflow |
| Language | Python 3.9 |

---

## Project structure

```
nyc-taxi-aws/
├── ingestion/
│   ├── spark/clean_trips.py          # PySpark cleaning job
│   └── upload_to_s3.py               # S3 upload
├── transform/
│   ├── nyc_taxi_transform/           # dbt project
│   │   ├── models/staging/           # stg_trips_cleaned.sql
│   │   └── models/marts/             # trips_hourly_demand.sql
│   └── xgboost_demand_model.py       # Demand prediction model
├── orchestration/
│   └── dags/nyc_taxi_pipeline_dag.py # Airflow DAG
└── dashboard/
    └── app.py                         # Streamlit dashboard
```

---

## Running it

**Prerequisites:** Python 3.9+, Java 17 (PySpark), AWS credentials, `dbt-athena-community`

```bash
pip install dbt-athena-community xgboost scikit-learn streamlit plotly pyathena pyspark

# Set env vars
export PROJECT_ROOT=/path/to/nyc-taxi-aws
export S3_BUCKET=your-s3-bucket
export AWS_REGION=us-east-1

# Run dbt transforms
cd transform/nyc_taxi_transform && dbt run

# Train demand model
cd transform && python3 xgboost_demand_model.py

# Launch dashboard
streamlit run dashboard/app.py
```

---

## Data source

NYC TLC Yellow Taxi Trip Records — January 2023. Public dataset: [nyc.gov/tlc](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

---

Parth Alagh · USICT, GGSIPU · 2026
