# Module 5 Homework: NYC Taxi Data Pipeline with Bruin

A complete end-to-end ELT pipeline built with [Bruin](https://getbruin.com/) for the Data Engineering Zoomcamp 2026. The pipeline ingests NYC taxi data, transforms it through a three-layer architecture, and produces aggregated reports — all orchestrated locally with DuckDB.

## Pipeline Architecture

```
ingestion.trips          ──┐
(Python, append)           ├──▶  staging.trips  ──▶  reports.trips_report
ingestion.payment_lookup  ──┘    (SQL, time_interval)  (SQL, time_interval)
(Seed CSV)
```

**Three-layer design:**

- **Ingestion** — fetches raw Parquet files from the NYC TLC public dataset, normalizes column names across yellow/green taxi types, appends into DuckDB
- **Staging** — joins trips with payment lookup, deduplicates, filters to time window
- **Reports** — aggregates by date, taxi type, and payment method

## Project Structure

```
my-taxi-pipeline/
├── .bruin.yml               # Local env + DuckDB connection (gitignored)
├── .gitignore
├── README.md
└── pipeline/
    ├── pipeline.yml         # Pipeline config: schedule, variables, connections
    └── assets/
        ├── ingestion/
        │   ├── trips.py             # Python ingestion asset
        │   ├── requirements.txt     # pandas, pyarrow, requests, python-dateutil
        │   ├── payment_lookup.asset.yml  # Seed asset
        │   └── payment_lookup.csv   # Payment type reference data
        ├── staging/
        │   └── trips.sql            # Dedup + enrich with time_interval strategy
        └── reports/
            └── trips_report.sql     # Daily aggregation report
```

## Quick Start

### 1. Install Bruin CLI

```bash
curl -LsSf https://getbruin.com/install/cli | sh
```

### 2. Clone and enter the project

```bash
git clone <this-repo>
cd my-taxi-pipeline
```

### 3. Create `.bruin.yml` (local only, never committed)

```yaml
default_environment: default

environments:
  default:
    connections:
      duckdb:
        - name: duckdb-default
          path: ./duckdb.db
```

### 4. Validate

```bash
bruin validate pipeline/pipeline.yml
```

### 5. Run the pipeline

```bash
# First run — creates all tables from scratch
bruin run pipeline/pipeline.yml \
  --start-date 2022-01-01 \
  --end-date 2022-02-01 \
  --full-refresh
```

### 6. Query results

```bash
bruin query --connection duckdb-default \
  --query "SELECT * FROM reports.trips_report LIMIT 10"
```

## Key Bruin Concepts Used

| Concept | Usage in this project |
|---|---|
| `pipeline.yml` | Defines schedule, DuckDB connection, `taxi_types` variable |
| Python asset | Ingests Parquet files from NYC TLC, returns DataFrame |
| Seed asset | Loads `payment_lookup.csv` into DuckDB |
| `time_interval` strategy | Incremental processing in staging and reports |
| `append` strategy | Raw ingestion layer — never overwrites existing rows |
| Quality checks | `not_null`, `unique`, `non_negative`, custom row count check |
| `--full-refresh` | Rebuilds all tables from scratch on first run |
| `bruin lineage` | Visualize dependency graph between assets |

## Pipeline Variables

The pipeline supports a `taxi_types` variable to control which taxi types are ingested:

```bash
# Process only yellow taxis
bruin run pipeline/pipeline.yml --var 'taxi_types=["yellow"]'

# Process yellow and green (default)
bruin run pipeline/pipeline.yml --var 'taxi_types=["yellow", "green"]'
```

## Data Source

NYC Taxi and Limousine Commission (TLC) Trip Record Data — publicly available Parquet files:

```
https://d37ci6vzurychx.cloudfront.net/trip-data/{type}_tripdata_{year}-{month}.parquet
```

## Results (January 2022)

| Table | Row Count |
|---|---|
| `ingestion.trips` | 5,575,256 |
| `staging.trips` | 2,516,871 |
| `reports.trips_report` | 312 |

---

Built as part of **[Data Engineering Zoomcamp 2026](https://github.com/DataTalksClub/data-engineering-zoomcamp)** by DataTalksClub — a free, open-source data engineering course covering the full modern data stack.
