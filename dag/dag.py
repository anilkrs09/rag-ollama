composer-dataflow-pipeline/
├── dags/
│   └── gcs_text_to_bq_flex_dag.py
│
├── scripts/
│   ├── deploy_dag.py
│   └── trigger_dag.py
│
├── Dockerfile
├── entrypoint.sh
├── requirements.txt
├── kubernetes-job.yaml
└── README.md

gcs_text_to_bq_flex_dag.py
  
  """
Airflow DAG to trigger the Dataflow Flex Template: GCS_Text_to_BigQuery
and move processed GCS files to a 'processed' folder.
"""

from datetime import datetime
from airflow import models
from airflow.providers.google.cloud.operators.dataflow import DataflowStartFlexTemplateOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator, GCSToGCSOperator
from airflow.utils.trigger_rule import TriggerRule
import os

# -----------------------------------------------------------------------------
# Configurations (env vars or Airflow variables)
# -----------------------------------------------------------------------------
PROJECT_ID = os.getenv("GCP_PROJECT", "your-project-id")
REGION = os.getenv("DATAFLOW_REGION", "us-central1")
SERVICE_ACCOUNT = os.getenv("DATAFLOW_SERVICE_ACCOUNT", f"dataflow-sa@{PROJECT_ID}.iam.gserviceaccount.com")

BUCKET_NAME = os.getenv("GCS_BUCKET", "your-bucket")
INPUT_PREFIX = os.getenv("INPUT_PREFIX", "input/")
PROCESSED_PREFIX = os.getenv("PROCESSED_PREFIX", "processed/")

TEMP_LOCATION = f"gs://{BUCKET_NAME}/temp"
STAGING_LOCATION = f"gs://{BUCKET_NAME}/staging"

# -----------------------------------------------------------------------------
# DAG Definition
# -----------------------------------------------------------------------------
with models.DAG(
    dag_id="gcs_text_to_bq_flex_dag",
    description="Triggers Dataflow Flex Template (GCS_Text_to_BigQuery) and moves processed file",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dataflow", "gcs-to-bq", "composer"],
) as dag:

    # List input files
    list_files = GCSListObjectsOperator(
        task_id="list_input_files",
        bucket=BUCKET_NAME,
        prefix=INPUT_PREFIX,
    )

    # Run Dataflow Flex Template
    start_dataflow_job = DataflowStartFlexTemplateOperator(
        task_id="start_dataflow_gcs_to_bq",
        project_id=PROJECT_ID,
        location=REGION,
        body={
            "launchParameter": {
                "jobName": "gcs-text-to-bq-{{ ts_nodash }}",
                "containerSpecGcsPath": "gs://dataflow-templates/latest/flex/GCS_Text_to_BigQuery",
                "parameters": {
                    "inputFilePattern": f"gs://{BUCKET_NAME}/{INPUT_PREFIX}*.csv",
                    "outputTable": f"{PROJECT_ID}:your_dataset.your_table",
                    "bigQueryLoadingTemporaryDirectory": TEMP_LOCATION,
                },
                "environment": {
                    "tempLocation": TEMP_LOCATION,
                    "stagingLocation": STAGING_LOCATION,
                    "serviceAccountEmail": SERVICE_ACCOUNT,
                    "workerRegion": REGION,
                    "subnetwork": f"regions/{REGION}/subnetworks/default",
                },
            }
        },
    )

    # Move processed files
    move_processed_files = GCSToGCSOperator(
        task_id="move_processed_files",
        source_bucket=BUCKET_NAME,
        source_object=f"{INPUT_PREFIX}*.csv",
        destination_bucket=BUCKET_NAME,
        destination_object=f"{PROCESSED_PREFIX}",
        move_object=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    list_files >> start_dataflow_job >> move_processed_files

│   ├── deploy_dag.py

import subprocess
import json
import os

def get_composer_bucket(project_id, region, composer_env):
    """Get the Composer DAG GCS bucket path."""
    cmd = [
        "gcloud", "composer", "environments", "describe",
        composer_env,
        "--project", project_id,
        "--location", region,
        "--format", "json"
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    data = json.loads(result.stdout)
    return data["config"]["dagGcsPrefix"]

def upload_dag(dag_file, bucket_path):
    """Upload DAG to Composer's DAG folder."""
    subprocess.run(["gsutil", "cp", dag_file, f"{bucket_path}/dags/"], check=True)
    print(f"✅ Uploaded {dag_file} to {bucket_path}/dags/")

if __name__ == "__main__":
    project_id = os.getenv("PROJECT_ID")
    region = os.getenv("REGION")
    composer_env = os.getenv("COMPOSER_ENV")

    dag_file = "dags/gcs_text_to_bq_flex_dag.py"
    bucket_path = get_composer_bucket(project_id, region, composer_env)
    upload_dag(dag_file, bucket_path)


triggerdag.
import subprocess
import os
import json

def trigger_dag(project_id, region, composer_env, dag_id, conf=None):
    """Trigger Airflow DAG run."""
    cmd = [
        "gcloud", "composer", "environments", "run", composer_env,
        "--project", project_id,
        "--location", region,
        "dags", "trigger", "--", dag_id
    ]
    if conf:
        cmd.extend(["--conf", json.dumps(conf)])

    subprocess.run(cmd, check=True)
    print(f"✅ Triggered DAG {dag_id}")

if __name__ == "__main__":
    project_id = os.getenv("PROJECT_ID")
    region = os.getenv("REGION")
    composer_env = os.getenv("COMPOSER_ENV")

    dag_id = "gcs_text_to_bq_flex_dag"
    conf = {
        "inputFilePattern": "gs://your-bucket/input/*.csv",
        "outputTable": f"{project_id}:your_dataset.your_table"
    }

    trigger_dag(project_id, region, composer_env, dag_id, conf)


EntryPoint.sh
#!/bin/bash
set -e

echo "🔹 Authenticating to GCP..."
if [[ -n "${GOOGLE_APPLICATION_CREDENTIALS}" && -f "${GOOGLE_APPLICATION_CREDENTIALS}" ]]; then
  gcloud auth activate-service-account --key-file="${GOOGLE_APPLICATION_CREDENTIALS}"
else
  echo "Using Workload Identity or default credentials"
fi

gcloud config set project "${PROJECT_ID}"
gcloud config set compute/region "${REGION}"

echo "🔹 Deploying DAG to Composer..."
python3 scripts/deploy_dag.py

echo "🔹 Triggering DAG..."
python3 scripts/trigger_dag.py

echo "✅ Workflow completed successfully."


req.txt
google-cloud-storage
google-cloud-composer


Dockerfile
FROM google/cloud-sdk:slim

WORKDIR /app

# Install Python and dependencies
COPY requirements.txt .
RUN apt-get update && apt-get install -y python3-pip && pip install -r requirements.txt

# Copy project files
COPY dags/ ./dags/
COPY scripts/ ./scripts/
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]


apiVersion: batch/v1
kind: Job
metadata:
  name: composer-dataflow-job
spec:
  template:
    spec:
      serviceAccountName: gcp-deploy-sa
      restartPolicy: Never
      containers:
      - name: composer-dataflow
        image: gcr.io/your-project/composer-dataflow-pipeline:latest
        env:
        - name: PROJECT_ID
          value: your-project
        - name: REGION
          value: us-central1
        - name: COMPOSER_ENV
          value: composer-prod
        volumeMounts:
        - name: gcp-key
          mountPath: /secrets
          readOnly: true
      volumes:
      - name: gcp-key
        secret:
          secretName: gcp-sa-key


