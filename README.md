# Uber Data Engineering Project (Azure + Databricks)

An end-to-end data engineering pipeline for Uber ride data. It combines **batch ingestion** (Azure Data Factory), **streaming ingestion** (Azure Event Hubs), and a **medallion-style lakehouse** built in Databricks with a star-schema gold layer.

## Architecture

```
GitHub (Data/) → ADF (HTTPToADLS) → ADLS Gen2 (raw/ingestion) ─┐
                                                               ├→ Databricks → bronze → staging → silver_obt → gold (fact + dims)
Python producer → Event Hubs (ubertopic) ──────────────────────┘
```

![Databricks Pipeline Graph](Uber_Data_Engineer_Project\Screenshots\Databricks_pipeline_graph.png)

## Tech Stack

- **Azure Data Factory**: batch ingestion pipeline
- **Azure Data Lake Storage Gen2**: raw storage
- **Azure Event Hubs**: streaming ride events
- **Databricks (Free Edition)**: PySpark, Declarative Pipelines, Delta tables, SQL
- **Python**: event producer and pipeline code

## Data

- `bulk_rides.json`: historical ride records
- Seven `map_*.json` lookup files: cities, payment methods, ride statuses, vehicle makes, vehicle types, cancellation reasons
- Streaming ride events (JSON) with fields such as `ride_id`, `passenger_id`, `driver_id`, `vehicle_id`, pickup/dropoff location, payment method, and ride status

## Pipeline Steps

### 1. Batch ingestion (Azure Data Factory)
Pipeline `HTTPToADLS` uses a **Lookup** activity to read the file list (`files_array`), then a **ForEach** loop runs a **Copy** activity per file. Each Uber dataset is downloaded from GitHub over HTTP into `raw/ingestion` in ADLS Gen2. The pipeline ran 8 times between Oct 24 and Nov 1, all successful.

![ADF Pipeline](Uber_Data_Engineer_Project\Screenshots\screenshots03_adf_pipeline.png.png)
![ADF Runs](Uber_Data_Engineer_Project\Screenshots\screenshots04b_adf_run_detail.png.png)

### 2. Streaming ingestion (Azure Event Hubs)
Ride events are sent to the `ubertopic` event hub (namespace `uberevents7`, 1 partition, 168h retention) and consumed by Databricks into `rides_raw`.

![Event Hubs](Uber_Data_Engineer_Project\Screenshots\screenshots05_event_hubs_metrics.png.png)
![Events](Uber_Data_Engineer_Project\Screenshots\screenshots05b_ubertopicc_events.png.png)

### 3. Raw storage (ADLS Gen2)
Storage account `dluberprojectdev7`, container `raw`, folder `ingestion` holding the bulk and mapping files.

![ADLS](Uber_Data_Engineer_Project\Screenshots\screenshots02c_adls_ingestion.png.png)

### 4. Databricks lakehouse
- **Bronze:** `rides_raw` (streaming from Event Hubs) and `bulk_rides` (historical)
- **Staging:** `stg_rides` merges streaming and bulk data
- **Silver:** `silver_obt` joins staging with the mapping tables into one 

### 5. Gold layer validation
A row-count query confirms the star schema loaded: `fact` holds 2,022 rides, matched by `dim_driver`, `dim_passenger`, `dim_vehicle` and `dim_booking` (2,022 rows each), plus `dim_location` (14 rows) and `dim_payments` (4 rows).Dimensions use SCD Type 2 (`__END_AT IS NULL` selects the current record).

![Catalog](Uber_Data_Engineer_Project\Screenshots\Databricks_catlog.png)
![Gold Layer Test](Uber_Data_Engineer_Project\Screenshots\gold_layer_test.png)

## Project Structure

```
UBER_DATA_ENGINEERING_PROJECT/
├── Code_Files/            # Databricks notebooks: Bronze_adls, ingest, Silver, silver_obt, model
├── Data/                  # Source JSON files (bulk_rides, map_*)
├── adf/                   # Azure Data Factory JSON (pipeline, datasets, linked services)
├── screenshots/           # Evidence of Azure and Databricks resources
├── templates/             # <describe>
├── api.py                 # Event producer / API script
├── connection.py          # Event Hubs connection (no secrets committed)
├── architecture.png       # Architecture diagram
├── files_array.json       # File list used by the ADF ForEach loop
├── requirements.txt
└── README.md
```

## Azure Resources Used

![Resource Group](Uber_Data_Engineer_Project\Screenshots\screenshots01_resource_group.png.png)

| Resource | Name | Purpose |
|---|---|---|
| Data Factory | `adf-uber-dev7` | Batch ingestion |
| Storage account | `dluberprojectdev7` | ADLS Gen2 raw layer |
| Event Hubs namespace | `uberevents7` | Streaming events |

## How to Run

1. Create an ADLS Gen2 storage account with a `raw` container.
2. Create an Event Hubs namespace and an event hub named `ubertopic`.
3. Import the JSON files from `adf/` into Azure Data Factory and run `HTTPToADLS`.
4. Start the Python producer to send events to `ubertopic`.
5. Import the code from `Code_Files/` into Databricks and run the pipeline.
6. Add your own connection strings in Databricks (never commit them).

## Key Learnings

- Combining batch and streaming ingestion into one lakehouse
- Building a medallion architecture with Declarative Pipelines
- Designing a star schema with SCD Type 2 dimensions
- Parameterized ADF pipelines using Lookup and ForEach

## Notes

The Azure resources were created on a free trial account that has since expired. Screenshots and exported ADF definitions are included as evidence. No secrets are stored in this repository.