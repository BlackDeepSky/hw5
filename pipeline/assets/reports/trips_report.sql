/* @bruin
name: reports.trips_report
type: duckdb.sql

depends:
  - staging.trips

materialization:
  type: table
  strategy: time_interval
  incremental_key: trip_date
  time_granularity: date

columns:
  - name: trip_date
    type: date
    description: "Date of the trip"
    primary_key: true
  - name: taxi_type
    type: string
    description: "Type of taxi"
    primary_key: true
  - name: payment_type
    type: string
    description: "Payment type name"
    primary_key: true
  - name: trip_count
    type: bigint
    description: "Number of trips"
  - name: total_fare
    type: float64
    description: "Total fare amount"
  - name: avg_fare
    type: float64
    description: "Average fare amount"

@bruin */

SELECT
    CAST(pickup_datetime AS DATE) AS trip_date,
    taxi_type,
    payment_type_name AS payment_type,
    COUNT(*) AS trip_count,
    SUM(fare_amount) AS total_fare,
    AVG(fare_amount) AS avg_fare
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY 1, 2, 3