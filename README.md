# E-Commerce Real-Time GCP Data Pipeline

End-to-end e-commerce data pipeline built using Google Cloud Platform (GCP).

> This project uses **Dataproc Serverless with PySpark** for data transformation and does not use Google Cloud Dataflow.

## GCP Services Used

* **Google Cloud Pub/Sub** — streaming order ingestion
* **Google Cloud Storage (GCS)** — Bronze and Silver data layers
* **Dataproc Serverless** — PySpark transformation
* **Cloud Spanner** — operational data storage
* **BigQuery** — Gold analytics and federated querying
* **BigQuery Spanner External Connection** — Spanner-to-BigQuery federation
* **Cloud Composer / Apache Airflow** — workflow orchestration

## Architecture

```text
E-Commerce Events
       ↓
Python Publisher
       ↓
Google Cloud Pub/Sub
       ↓
GCS Bronze - Raw JSON
       ↓
Cloud Composer / Airflow
       ↓
Dataproc Serverless + PySpark
       ↓
   ┌───┴────────────┐
   ↓                ↓
GCS Silver      Cloud Spanner
  Parquet        Transactions
                    ↓
                 BigQuery
              EXTERNAL_QUERY
                    ↓
              Gold Analytics
```

## Pipeline Flow

1. Generate e-commerce order events using Python.
2. Publish events to Google Cloud Pub/Sub.
3. Store raw JSON data in the GCS Bronze layer.
4. Airflow triggers the Dataproc Serverless PySpark job.
5. PySpark cleans, transforms, and deduplicates the data.
6. Store processed data as Parquet in the GCS Silver layer.
7. Write transactional data to Cloud Spanner.
8. BigQuery reads Cloud Spanner using `EXTERNAL_QUERY`.
9. Generate product-level analytics in the Gold layer.
10. Run sanity checks to validate the pipeline.

## Data Quality Checks

The project includes checks for:

* Data volume
* Data completeness
* Schema integrity
* Data types
* Deduplication
* Idempotency
* Business reconciliation
* Data freshness
* Cloud Spanner validation
* BigQuery federated query validation

## Repository Structure

```text
gcp-realtime-ecommerce-data-pipeline/
│
├── README.md
├── LICENSE
├── .gitignore
│
├── architecture/
│   └── gcp-realtime-ecommerce-pipeline-architecture.png
│
├── docs/
│   ├── 01-architecture-and-implementation.docx
│   ├── 02-code-documentation.docx
│   └── 03-sanity-checks.docx
│
├── src/
│   ├── publisher/
│   │   └── publisher.py
│   ├── pyspark/
│   │   └── clean_ecom.py
│   └── airflow/
│       └── ecom_pipeline_dag.py
│
├── sql/
│   ├── spanner_schema.sql
│   ├── bigquery_aggregation.sql
│   └── verification_queries.sql
│
└── tests/
    └── sanity_checks.sql
```

## Project Execution Order

1. Set up the required GCP services and IAM permissions.
2. Configure Pub/Sub and GCS Bronze storage.
3. Create the Cloud Spanner database and table.
4. Run the Python publisher.
5. Deploy and run the PySpark transformation.
6. Configure Cloud Composer and deploy the Airflow DAG.
7. Configure the BigQuery-Spanner external connection.
8. Run the BigQuery aggregation.
9. Run the sanity checks.

## Project Configuration

The values used in the project documentation are:

```text
GCP Project ID       : snappy-mapper-498509-e0
GCS Bucket            : pub-sub-dag-rawdata
Pub/Sub Topic         : ecom-orders-topic
Spanner Instance      : ecom-spanner-instance
Spanner Database      : ecom-db
BigQuery Dataset      : ecom_analytics2
Spanner Connection    : spanner-ecom-conn
Composer Environment  : ecom-composer-env
```

> **Note:** These values are based on the project documentation and represent the POC environment. If publishing this repository publicly, replace environment-specific values with placeholders and do not commit credentials, service-account keys, API keys, or passwords.

## Documentation

Detailed project documentation is available in the `docs/` folder:

* **Architecture & Implementation**
* **Code Documentation**
* **Pipeline Sanity Checks**

## Project Status

**Proof of Concept (POC)**
