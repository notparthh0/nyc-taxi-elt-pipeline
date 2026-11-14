"""
Canonical schema for the cleaned NYC TLC Yellow Taxi table.

TLC files change column types between months (for example passenger_count is
sometimes a double and sometimes an integer). The cleaning job casts every
month to this one schema, so all partitions of the lake table line up, and the
Athena DDL is generated from the same list, so the two can never drift apart.

Each entry: (column name, Spark SQL type, Athena type)
"""

CLEANED_COLUMNS = [
    ("VendorID",              "int",           "int"),
    ("tpep_pickup_datetime",  "timestamp_ntz", "timestamp"),
    ("tpep_dropoff_datetime", "timestamp_ntz", "timestamp"),
    ("passenger_count",       "int",           "int"),
    ("trip_distance",         "double",        "double"),
    ("RatecodeID",            "int",           "int"),
    ("store_and_fwd_flag",    "string",        "string"),
    ("PULocationID",          "int",           "int"),
    ("DOLocationID",          "int",           "int"),
    ("payment_type",          "int",           "int"),
    ("fare_amount",           "double",        "double"),
    ("extra",                 "double",        "double"),
    ("mta_tax",               "double",        "double"),
    ("tip_amount",            "double",        "double"),
    ("tolls_amount",          "double",        "double"),
    ("improvement_surcharge", "double",        "double"),
    ("total_amount",          "double",        "double"),
    ("congestion_surcharge",  "double",        "double"),
    ("airport_fee",           "double",        "double"),
    # derived in the cleaning job
    ("trip_duration_minutes", "double",        "double"),
    ("cost_per_mile",         "double",        "double"),
]

# Partition columns of the lake table (Hive-style folders: year=2023/month=01)
PARTITION_COLUMNS = [("year", "string"), ("month", "string")]
