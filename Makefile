# Common tasks. Override the month: make month MONTH=2023-02
# Settings (bucket, Athena locations) come from .env if it exists.
ifneq (,$(wildcard .env))
include .env
export
endif

PYTHON ?= python3
DBT    ?= dbt
MONTH  ?= 2023-01
DBT_DIR = transform/nyc_taxi_transform

.PHONY: test download spark-clean dbt-local month backfill train-local dry-run-aws aws-load aws-first-load aws-check

test:                      ## all tests (PySpark, dbt on DuckDB, DAG if Airflow is installed)
	$(PYTHON) -m pytest

download:                  ## fetch raw TLC file for MONTH into raw/
	$(PYTHON) ingestion/download_tlc.py --month $(MONTH)

spark-clean:               ## clean MONTH into data/lake/trips_cleaned/year=/month=/
	$(PYTHON) ingestion/spark/clean_trips.py --month $(MONTH)

dbt-local:                 ## build + test dbt models on DuckDB for MONTH (incremental)
	cd $(DBT_DIR) && $(DBT) build --profiles-dir profiles --target local --vars '{"month": "$(MONTH)"}'

month: download spark-clean dbt-local   ## whole local pipeline for one MONTH

backfill:                  ## e.g. make backfill MONTHS="2023-01 2023-02 2023-03"
	@for m in $(MONTHS); do $(MAKE) --no-print-directory month MONTH=$$m || exit 1; done

train-local:               ## retrain XGBoost on the local mart (writes to data/lake/model/)
	DATA_SOURCE=duckdb MODEL_DIR=data/lake/model $(PYTHON) transform/xgboost_demand_model.py

dry-run-aws:               ## print the S3 upload plan and Athena SQL for MONTH without touching AWS
	$(PYTHON) ingestion/upload_to_s3.py --month $(MONTH) --dry-run
	$(PYTHON) ingestion/athena_register.py --month $(MONTH) --dry-run

aws-load:                  ## put MONTH on AWS: upload to S3, register in Athena, rebuild dbt on Athena
	$(PYTHON) ingestion/upload_to_s3.py --month $(MONTH)
	$(PYTHON) ingestion/athena_register.py --month $(MONTH)
	cd $(DBT_DIR) && $(DBT) build --profiles-dir profiles --target athena --vars '{"month": "$(MONTH)"}' $(DBT_FLAGS)

aws-first-load:            ## first time only: January, and rebuild dbt tables from scratch
	$(MAKE) --no-print-directory aws-load MONTH=2023-01 DBT_FLAGS=--full-refresh

aws-check:                 ## ask Athena how many zone-hours and trips each month has
	cd $(DBT_DIR) && $(DBT) show --profiles-dir profiles --target athena --inline \
	  "select month_key, count(*) as zone_hours, sum(trip_count) as trips from {{ ref('trips_hourly_demand') }} group by 1 order by 1"
