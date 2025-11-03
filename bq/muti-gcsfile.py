env:
  - name: GCS_BUCKET
    value: "{{ .Values.env.gcsBucket }}"
  - name: GCS_INPUT_PREFIX
    value: "{{ .Values.env.gcsInputPrefix }}"
  - name: GCS_PROCESSED_PREFIX
    value: "{{ .Values.env.gcsProcessedPrefix }}"
  - name: BQ_PROJECT
    value: "{{ .Values.env.bqProject }}"
  - name: BQ_DATASET
    value: "{{ .Values.env.bqDataset }}"
  - name: BQ_TABLE
    value: "{{ .Values.env.bqTable }}"

import os
import pandas as pd
from google.cloud import storage, bigquery
from io import BytesIO
from datetime import datetime

storage_client = storage.Client()
bq_client = bigquery.Client()

def list_csv_files(bucket_name, prefix):
    """List all .csv files under prefix in the bucket."""
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    csv_files = [b.name for b in blobs if b.name.lower().endswith(".csv")]
    print(f"Found {len(csv_files)} CSV files under gs://{bucket_name}/{prefix}")
    return csv_files

def download_csv(bucket_name, blob_name):
    """Download CSV file from GCS to pandas DataFrame."""
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    content = blob.download_as_bytes()
    df = pd.read_csv(BytesIO(content))
    print(f"Downloaded {blob_name} → {len(df)} rows")
    return df

def transform(df):
    """Simple transformation example."""
    df = df.dropna()
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    df["processed_at"] = pd.Timestamp.utcnow()
    return df

def load_to_bq(df, project_id, dataset_id, table_id):
    """Append DataFrame to BigQuery."""
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    job = bq_client.load_table_from_dataframe(
        df,
        table_ref,
        job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND"),
    )
    job.result()
    print(f"Loaded {len(df)} rows into {table_ref}")

def move_file(bucket_name, source_name, dest_prefix):
    """Move file to a processed/archive folder."""
    bucket = storage_client.bucket(bucket_name)
    source_blob = bucket.blob(source_name)
    new_name = f"{dest_prefix}/{os.path.basename(source_name)}"
    bucket.copy_blob(source_blob, bucket, new_name)
    source_blob.delete()
    print(f"Moved {source_name} → {new_name}")

def main():
    bucket_name = os.environ["GCS_BUCKET"]
    input_prefix = os.environ.get("GCS_INPUT_PREFIX", "incoming")
    processed_prefix = os.environ.get("GCS_PROCESSED_PREFIX", "processed")
    project_id = os.environ["BQ_PROJECT"]
    dataset_id = os.environ["BQ_DATASET"]
    table_id = os.environ["BQ_TABLE"]

    csv_files = list_csv_files(bucket_name, input_prefix)
    if not csv_files:
        print("No new CSV files found. Exiting.")
        return

    for file_path in csv_files:
        try:
            df = download_csv(bucket_name, file_path)
            df = transform(df)
            load_to_bq(df, project_id, dataset_id, table_id)
            move_file(bucket_name, file_path, processed_prefix)
        except Exception as e:
            print(f"❌ Failed to process {file_path}: {e}")

    print("✅ ETL job completed successfully.")

if __name__ == "__main__":
    main()
