"""
NYC Taxi Demand Prediction using XGBoost
Paper: Urban Mobility Analytics Using Cloud-Native Big Data Pipelines
Author: Parth Alagh, USICT GGSIPU

Pipeline:
  S3 (raw) → PySpark (clean) → Athena → dbt (transform) → XGBoost (predict)
"""

import boto3
import pandas as pd
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor
from xgboost import XGBRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import numpy as np
import warnings
warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────
# 1. PULL DATA FROM ATHENA (dbt mart)
# ─────────────────────────────────────────────

print("=" * 60)
print("NYC TAXI DEMAND PREDICTION — XGBoost Model")
print("=" * 60)
print("\n[1/5] Pulling data from Athena dbt mart...")

conn = connect(
    s3_staging_dir="s3://nyc-taxi-de-parth-2026/athena-results/",
    region_name="us-east-1",
    cursor_class=PandasCursor
)

query = """
SELECT
    pickup_hour,
    pickup_day_of_week,
    pickup_month,
    pulocationid,
    trip_count,
    avg_trip_distance,
    avg_fare,
    avg_passengers,
    avg_duration_mins
FROM nyc_taxi.trips_hourly_demand
LIMIT 500000
"""

df = conn.cursor().execute(query).as_pandas()
print(f"   Rows fetched: {len(df):,}")
print(f"   Columns: {list(df.columns)}")

# ─────────────────────────────────────────────
# 2. FEATURE ENGINEERING
# ─────────────────────────────────────────────

print("\n[2/5] Engineering features...")

df = df.dropna()

# Features: time + location + trip characteristics
FEATURES = [
    "pickup_hour",
    "pickup_day_of_week",
    "pickup_month",
    "pulocationid",
    "avg_trip_distance",
    "avg_fare",
    "avg_passengers",
    "avg_duration_mins"
]

TARGET = "trip_count"

X = df[FEATURES]
y = df[TARGET]

print(f"   Feature matrix shape: {X.shape}")
print(f"   Target range: {y.min()} – {y.max()} trips/hour/zone")

# ─────────────────────────────────────────────
# 3. TRAIN / TEST SPLIT
# ─────────────────────────────────────────────

print("\n[3/5] Splitting train/test (80/20)...")

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

print(f"   Train size: {len(X_train):,} | Test size: {len(X_test):,}")

# ─────────────────────────────────────────────
# 4. TRAIN XGBoost MODEL
# ─────────────────────────────────────────────

print("\n[4/5] Training XGBoost Regressor...")

model = XGBRegressor(
    n_estimators=200,
    max_depth=6,
    learning_rate=0.1,
    subsample=0.8,
    colsample_bytree=0.8,
    random_state=42,
    verbosity=0
)

model.fit(X_train, y_train)
print("   Training complete.")

# ─────────────────────────────────────────────
# 5. EVALUATE
# ─────────────────────────────────────────────

print("\n[5/5] Evaluating model...")

y_pred = model.predict(X_test)

rmse  = np.sqrt(mean_squared_error(y_test, y_pred))
mae   = mean_absolute_error(y_test, y_pred)
r2    = r2_score(y_test, y_pred)

print("\n" + "=" * 60)
print("MODEL PERFORMANCE")
print("=" * 60)
print(f"   RMSE (Root Mean Squared Error):  {rmse:.2f} trips")
print(f"   MAE  (Mean Absolute Error):      {mae:.2f} trips")
print(f"   R²   (Variance Explained):       {r2:.4f}")
print("=" * 60)

# Feature importance
print("\nFEATURE IMPORTANCE (for research paper):")
importance = dict(zip(FEATURES, model.feature_importances_))
for feat, score in sorted(importance.items(), key=lambda x: -x[1]):
    bar = "█" * int(score * 40)
    print(f"   {feat:<25} {bar} {score:.4f}")

# Save model
import pickle, os
model_dir = "/Users/parthalagh/MAIN/nyc-taxi-aws/transform/model"
os.makedirs(model_dir, exist_ok=True)
with open(f"{model_dir}/xgboost_demand_model.pkl", "wb") as f:
    pickle.dump(model, f)

print(f"\nModel saved to: {model_dir}/xgboost_demand_model.pkl")
print("\nDone. This output goes directly into the research paper results section.")
