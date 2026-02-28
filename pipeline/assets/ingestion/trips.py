"""@bruin
name: ingestion.trips
type: python
image: python:3.11

connection: duckdb-default

materialization:
  type: table
  strategy: append

columns:
  - name: pickup_datetime
    type: timestamp
    description: "When the meter was engaged"
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    description: "When the meter was disengaged"
  - name: pickup_location_id
    type: integer
    description: "Pickup location ID"
  - name: dropoff_location_id
    type: integer
    description: "Dropoff location ID"
  - name: fare_amount
    type: float64
    description: "Fare amount in USD"
  - name: payment_type
    type: integer
    description: "Payment type code"
  - name: taxi_type
    type: string
    description: "Type of taxi (yellow or green)"
  - name: extracted_at
    type: timestamp
    description: "When the record was extracted"

@bruin"""

import os
import json
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta


def materialize():
    start_date = os.environ["BRUIN_START_DATE"]
    end_date = os.environ["BRUIN_END_DATE"]
    taxi_types = json.loads(os.environ["BRUIN_VARS"]).get("taxi_types", ["yellow"])

    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    # Generate list of year-month combinations
    months = []
    current = start.replace(day=1)
    while current <= end:
        months.append((current.year, current.month))
        current += relativedelta(months=1)

    dfs = []
    for taxi_type in taxi_types:
        for year, month in months:
            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year}-{month:02d}.parquet"
            print(f"Fetching: {url}")
            try:
                df = pd.read_parquet(url)

                # Normalize column names for yellow vs green taxis
                col_map = {}
                if "tpep_pickup_datetime" in df.columns:
                    col_map["tpep_pickup_datetime"] = "pickup_datetime"
                    col_map["tpep_dropoff_datetime"] = "dropoff_datetime"
                elif "lpep_pickup_datetime" in df.columns:
                    col_map["lpep_pickup_datetime"] = "pickup_datetime"
                    col_map["lpep_dropoff_datetime"] = "dropoff_datetime"
                if "PULocationID" in df.columns:
                    col_map["PULocationID"] = "pickup_location_id"
                    col_map["DOLocationID"] = "dropoff_location_id"
                if "RatecodeID" in df.columns:
                    col_map["RatecodeID"] = "ratecode_id"

                df = df.rename(columns=col_map)

                # Keep only needed columns
                keep = ["pickup_datetime", "dropoff_datetime",
                        "pickup_location_id", "dropoff_location_id",
                        "fare_amount", "payment_type"]
                df = df[[c for c in keep if c in df.columns]]

                df["taxi_type"] = taxi_type
                df["extracted_at"] = datetime.utcnow()

                dfs.append(df)
            except Exception as e:
                print(f"Error fetching {url}: {e}")

    if not dfs:
        return pd.DataFrame()

    return pd.concat(dfs, ignore_index=True)