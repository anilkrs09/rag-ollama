"""
DAG: GCS → BigQuery Batch Pipeline via Dataflow Flex Template
-------------------------------------------------------------
1️⃣ Runs a Dataflow batch job (GCS Text → BigQuery)
2️⃣ Moves processed input files to a /processed/ folder after successful run
3️⃣ Avoids re-processing duplicate data
"""

from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
from airflow.providers.google.cloud.operators.gcs import GCSToGCSOperator

# 🧩 --- CONFIGURATION (Edit these values) ---
PROJECT_ID = "my-gcp-project"
REGION = "us-central1"

# GCS locations
INPUT_BUCKET = "my-input-bucket"
TEMP_BUCKET = "my-temp-bucket"
SCHEMA_PATH = f"gs://{INPUT_BUCKET}/schema/schema.json"
TRANSFORM_JS = f"gs://{INPUT_BUCKET}/udf/transform.js"

# BigQuery output
BQ_DATASET = "my_dataset"
BQ_TABLE = "my_table"

# Dataflow template (Flex Template)
GCS_TEMPLATE_PATH = (
    "gs://dataflow-templates-us-central1/latest/flex/GCS_Text_to_BigQuery_Flex"
)

# Service account that has Dataflow + GCS + BigQuery permissions
DATAFLOW_SA = "dataflow-runner@my-gcp-project.iam.gserviceaccount.com"

# 🕒 --- DAG DEFINITION ---
with DAG(
    dag_id="gcs_to_bq_batch_dag",
    description="Trigger Dataflow batch job to load GCS text into BigQuery and archive processed files.",
    schedule_interval="@daily",   # or None for manual
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dataflow", "bigquery", "gcs", "batch"],
) as dag:

    # 🚀 1️⃣ Run Dataflow Flex Template
    run_dataflow = DataflowStartFlexTemplateOperator(
        task_id="run_gcs_to_bq",
        project_id=PROJECT_ID,
        location=REGION,
        body={
            "launchParameter": {
                "jobName": "gcs-to-bq-{{ ds_nodash }}",
                "containerSpecGcsPath": GCS_TEMPLATE_PATH,
                "parameters": {
                    "inputFilePattern": f"gs://{INPUT_BUCKET}/input/*.txt",
                    "JSONPath": SCHEMA_PATH,
                    "outputTable": f"{PROJECT_ID}:{BQ_DATASET}.{BQ_TABLE}",
                    "bigQueryLoadingTemporaryDirectory": f"gs://{TEMP_BUCKET}/tmp/",
                    "javascriptTextTransformGcsPath": TRANSFORM_JS,
                    "javascriptTextTransformFunctionName": "transform",
                },
                "environment": {
                    "tempLocation": f"gs://{TEMP_BUCKET}/tmp/",
                    "serviceAccountEmail": DATAFLOW_SA,
                },
            }
        },
    )

    # 📦 2️⃣ Move processed files to /processed/
    move_processed_files = GCSToGCSOperator(
        task_id="move_processed_files",
        source_bucket=INPUT_BUCKET,
        source_object="input/*.txt",
        destination_bucket=INPUT_BUCKET,
        destination_object="processed/",
        move_object=True,  # Moves instead of copying
    )

    # ✅ DAG flow
    run_dataflow >> move_processed_files

How this works
Step	Description
DataflowStartFlexTemplateOperator	Launches a batch Dataflow job that reads all files in gs://my-input-bucket/input/*.txt and inserts them into BigQuery
GCSToGCSOperator	Moves successfully processed files to gs://my-input-bucket/processed/ to avoid reprocessing next run
DAG Schedule	Runs daily (@daily) — or change to None for manual triggers
Parameters	Reads schema and transform JS from GCS; you can edit them easily
Permissions	Uses the specified service account (DATAFLOW_SA)


gcloud composer environments storage dags import \
    --environment=my-composer-env \
    --location=us-central1 \
    --source=gcs_to_bq_batch_dag.py

