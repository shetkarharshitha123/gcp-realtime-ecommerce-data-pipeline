
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
# Default arguments for DAG execution
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG pipeline
with DAG(
    'ecom_data_pipeline',
    default_args=default_args,
    description='End-to-End Pipeline: Dataproc Serverless cleans GCS raw JSONs to Spanner & BigQuery aggregates via External Connection',
    schedule_interval='0 0 * * *',
    catchup=False,
) as dag:
    # Task 1: Dataproc Serverless PySpark Batch Job
    run_dataproc_batch = DataprocCreateBatchOperator(
        task_id='run_dataproc_pyspark_batch',
        project_id='snappy-mapper-498509-e0',
        region='asia-south1',
        batch_id='clean-ecom-{{ ts_nodash.lower() }}-{{ task_instance.try_number }}',
        batch={
            'pyspark_batch': {
                'main_python_file_uri': 'gs://pub-sub-cleandata-dag/scripts/clean_ecom2.py',
                'jar_file_uris': [
                    'gs://pub-sub-cleandata-dag/jars/spark-3.5-spanner-1.4.0.jar'
                ],
            },
            'runtime_config': {
                'version': '2.2',
            },
        },
    )

    # Task 2: BigQuery Federated Aggregation Query
    run_bigquery_summary = BigQueryInsertJobOperator(
        task_id='run_bigquery_aggregation',
        configuration={
            "query": {
                "query": """
                    INSERT INTO `snappy-mapper-498509-e0.ecom_analytics2.daily_product_summary` (
                        product_id,
                        total_items_sold,
                        total_orders,
                        aggregated_at
                    )
                    SELECT 
                        product_id,
                        SUM(quantity) AS total_items_sold,
                        COUNT(DISTINCT order_id) AS total_orders,
                        CURRENT_TIMESTAMP() AS aggregated_at
                    FROM EXTERNAL_QUERY(
                        "projects/snappy-mapper-498509-e0/locations/asia-south1/connections/spanner-ecom-conn",
                        "SELECT order_id, product_id, quantity FROM EcomOrders"
                    )
                    GROUP BY product_id;
                """,
                "useLegacySql": False,
            }
        },
    )

    # Dependency Order
    run_dataproc_batch >> run_bigquery_summary
