"""
NYC Taxi Demand Prediction using XGBoost
Paper: Urban Mobility Analytics Using Cloud-Native Big Data Pipelines
Author: Parth Alagh, USICT GGSIPU

Features used at prediction time only:
  - pickup_hour, pickup_day_of_week, pickup_month, pulocationid

Features intentionally excluded (would not be known before trips occur):
  - avg_fare, avg_trip_distance, avg_passengers, avg_duration_mins

Validation: temporal split — earlier months train, later month tests.
This simulates a real forecast scenario where the model is asked to
predict future demand using only calendar and location inputs.
"""

import pandas as pd
from pyathena import connect
from pyathena.pandas.cursor import PandasCursor
from xgboost import XGBRegressor
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
import numpy as np
import pickle, os, warnings
warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────
# 1. PULL DATA FROM ATHENA
# ─────────────────────────────────────────────
print("=" * 60)
print("NYC TAXI DEMAND PREDICTION — XGBoost")
print("=" * 60)
print("\n[1/5] Querying Athena dbt mart...")

conn = connect(
    s3_staging_dir="s3://nyc-taxi-de-parth-2026/athena-results/",
    region_name="us-east-1",
    cursor_class=PandasCursor
)

df = conn.cursor().execute("""
    SELECT
        pickup_date,
        pickup_hour,
        pickup_day_of_week,
        pickup_month,
        pulocationid,
        trip_count
    FROM nyc_taxi.trips_hourly_demand
    ORDER BY pickup_date, pickup_hour
""").as_pandas()

df["pickup_date"] = pd.to_datetime(df["pickup_date"])
print(f"   Rows: {len(df):,} | Date range: {df['pickup_date'].min().date()} → {df['pickup_date'].max().date()}")

# ─────────────────────────────────────────────
# 2. TEMPORAL TRAIN / TEST SPLIT
# ─────────────────────────────────────────────
print("\n[2/5] Applying temporal split (no data leakage)...")

# Use the last 20% of days as the holdout set — simulates forecasting future demand
cutoff = df["pickup_date"].quantile(0.80)
train_df = df[df["pickup_date"] <= cutoff]
test_df  = df[df["pickup_date"] >  cutoff]

print(f"   Train: {len(train_df):,} rows | up to {cutoff.date()}")
print(f"   Test:  {len(test_df):,} rows  | after {cutoff.date()}")

# ─────────────────────────────────────────────
# 3. FEATURES — only what is known before trips happen
# ─────────────────────────────────────────────
FEATURES = [
    "pickup_hour",        # hour of day (0-23)
    "pickup_day_of_week", # day of week (1=Mon)
    "pickup_month",       # month (1-12)
    "pulocationid",       # pickup zone (NYC TLC zone ID)
]
TARGET = "trip_count"

print(f"\n[3/5] Features used: {FEATURES}")
print(f"   Note: fare, distance, passengers excluded — unavailable at forecast time.")

X_train = train_df[FEATURES]
y_train = train_df[TARGET]
X_test  = test_df[FEATURES]
y_test  = test_df[TARGET]

# ─────────────────────────────────────────────
# 4. BASELINE — mean predictor
# ─────────────────────────────────────────────
baseline_pred = np.full(len(y_test), y_train.mean())
baseline_rmse = np.sqrt(mean_squared_error(y_test, baseline_pred))
print(f"\n[4/5] Baseline (mean predictor) RMSE: {baseline_rmse:.2f}")

# ─────────────────────────────────────────────
# 5. TRAIN XGBOOST
# ─────────────────────────────────────────────
print("\n[5/5] Training XGBoost regressor...")

model = XGBRegressor(
    n_estimators=300,
    max_depth=6,
    learning_rate=0.08,
    subsample=0.8,
    colsample_bytree=0.8,
    random_state=42,
    verbosity=0
)
model.fit(X_train, y_train)

y_pred = model.predict(X_test)
rmse = np.sqrt(mean_squared_error(y_test, y_pred))
mae  = mean_absolute_error(y_test, y_pred)
r2   = r2_score(y_test, y_pred)

print("\n" + "=" * 60)
print("RESULTS (temporal holdout)")
print("=" * 60)
print(f"   RMSE:               {rmse:.2f}  trips  (baseline: {baseline_rmse:.2f})")
print(f"   MAE:                {mae:.2f}  trips")
print(f"   R²:                 {r2:.4f}")
print(f"   Improvement over baseline RMSE: {((baseline_rmse - rmse) / baseline_rmse * 100):.1f}%")
print("=" * 60)

# Feature importance
print("\nFEATURE IMPORTANCE:")
for feat, score in sorted(zip(FEATURES, model.feature_importances_), key=lambda x: -x[1]):
    bar = "█" * int(score * 40)
    print(f"   {feat:<25} {bar} {score:.4f}")

# Save model
model_dir = os.path.join(os.path.dirname(__file__), "model")
os.makedirs(model_dir, exist_ok=True)
model_path = os.path.join(model_dir, "xgboost_demand_model.pkl")
with open(model_path, "wb") as f:
    pickle.dump(model, f)

print(f"\nModel saved: {model_path}")
