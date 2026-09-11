# gcp-realtime-ecommerce-data-pipeline
Production-style GCP streaming data pipeline implementing Bronze/Silver/Gold architecture with Pub/Sub, GCS, PySpark on Dataproc Serverless, Cloud Spanner, BigQuery federated queries, and Airflow orchestration.

# GCP Real-Time E-Commerce Data Pipeline

An end-to-end streaming data engineering pipeline built on Google Cloud Platform.

## Architecture

Pub/Sub
   ↓
Cloud Storage - Bronze
   ↓
Dataproc Serverless / PySpark
   ↓
Cloud Storage - Silver
   ↓
Cloud Spanner
   ↓
BigQuery Federated Query
   ↓
Gold Analytics

Cloud Composer / Airflow
        ↓
Orchestrates the pipeline

## Technologies

- Google Cloud Pub/Sub
- Google Cloud Storage
- Dataproc Serverless
- PySpark
- Cloud Spanner
- BigQuery
- BigQuery EXTERNAL_QUERY
- Cloud Composer
- Apache Airflow
- Python
- SQL

## Project Overview

This project implements an automated streaming e-commerce data pipeline
using a Bronze/Silver/Gold architecture.

Raw e-commerce order events are published to Google Cloud Pub/Sub
and delivered to Cloud Storage.

Dataproc Serverless executes PySpark transformations to clean,
deduplicate and transform the raw data.

The processed data is stored in Parquet format in the Silver layer
and transactional records are written to Cloud Spanner.

BigQuery uses a federated connection to Cloud Spanner to generate
analytical summaries in the Gold layer.

Cloud Composer / Apache Airflow orchestrates the processing workflow.

## Pipeline Flow

1. E-commerce events are generated
2. Events are published to Pub/Sub
3. Pub/Sub delivers raw JSON to GCS Bronze
4. Airflow triggers Dataproc Serverless
5. PySpark cleans and deduplicates data
6. Clean data is stored as Parquet
7. Transactional data is written to Cloud Spanner
8. BigQuery queries Spanner using EXTERNAL_QUERY
9. Aggregated metrics are written to the Gold layer
10. Sanity checks validate the pipeline

## Data Quality

The project includes sanity checks for:

- Data volume
- Completeness
- Schema integrity
- Data types
- Deduplication
- Idempotency
- Business reconciliation
- Data freshness
- Cloud Spanner connectivity
- BigQuery federated queries

## Repository Structure

```text
docs/       → Project documentation
src/        → Python, PySpark and Airflow code
sql/        → SQL scripts
tests/      → Data pipeline sanity checks
architecture/ → Architecture diagrams
