# NYC Taxi Analytics Pipeline

End-to-end batch data pipeline on AWS. Takes raw NYC TLC taxi data, cleans it at scale, transforms it with dbt, trains a demand prediction model, and serves it through a dashboard.

Built this to learn production data engineering — not a tutorial follow-along.

---

## What it does

- Cleans 3M+ raw trip records using PySpark (drops nulls, invalid fares, zero-distance trips)
- Loads cleaned data to S3 and queries it serverlessly via Athena
- Runs SQL transformations with dbt to produce staging and aggregated models
- Trains an XGBoost regressor to predict taxi demand by zone and hour
- Visualises results in a Streamlit dashboard connected live to Athena

## Results

| Metric | Value |
|--------|-------|
| Raw records | 3,066,766 |
| After cleaning | 2,881,179 |
| Avg fare | $18.54 |
| XGBoost RMSE | 21.47 trips |
| XGBoost R² | 0.91 |

Top predictors: passenger volume, pickup zone ID, trip distance.

## Stack

```
PySpark → AWS S3 → AWS Athena → dbt → XGBoost → Streamlit
                                              ↑
                                         Apache Airflow (orchestration)
```

| Layer | Tool |
|-------|------|
| Processing | Apache PySpark 3.5 |
| Storage | AWS S3 |
| Query | AWS Athena (serverless) |
| Transform | dbt-core + dbt-athena-community |
| ML | XGBoost, scikit-learn |
| Dashboard | Streamlit + Plotly |
| Orchestration | Apache Airflow |

## Structure

```
nyc-taxi-aws/
├── ingestion/
│   ├── spark/clean_trips.py       # PySpark cleaning job
│   └── upload_to_s3.py            # S3 upload
├── transform/
│   ├── nyc_taxi_transform/        # dbt project
│   │   ├── models/staging/        # stg_trips_cleaned.sql
│   │   └── models/marts/          # trips_hourly_demand.sql
│   └── xgboost_demand_model.py    # ML training script
├── orchestration/
│   └── dags/nyc_taxi_pipeline_dag.py
└── dashboard/
    └── app.py                     # Streamlit app
```

## Running locally

**Requirements:** Python 3.9+, AWS credentials configured (`aws configure`), Java 17 (for Spark)

```bash
# Install dependencies
pip install dbt-athena-community xgboost scikit-learn streamlit plotly pyathena

# Run dbt models
cd transform/nyc_taxi_transform
dbt run

# Train XGBoost model
cd transform
python xgboost_demand_model.py

# Launch dashboard
streamlit run dashboard/app.py
```

## Data

NYC TLC Yellow Taxi Trip Records — January 2023. Public dataset from [nyc.gov/tlc](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).

Raw files stored in S3 (`/raw/`), cleaned output in `/cleaned/`, Athena query results in `/athena-results/`.

---

Parth Alagh · USICT, GGSIPU · 2026
