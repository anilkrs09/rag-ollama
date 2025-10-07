fastapi
uvicorn[standard]
google-cloud-bigquery
pydantic


app/main.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import os
from .bq_client import get_bq_client, query_table, insert_rows


app = FastAPI(title="bq-microservice")


class RowIn(BaseModel):
name: str
age: int


# Read configuration from environment
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")
BQ_DATASET = os.getenv("BQ_DATASET")
BQ_TABLE = os.getenv("BQ_TABLE")


if not (BQ_PROJECT_ID and BQ_DATASET and BQ_TABLE):
# It's okay to run without env vars during development, but warn.
print("WARNING: BQ_PROJECT_ID, BQ_DATASET or BQ_TABLE not set — endpoints may fail.")


@app.get("/query")
def read_rows(limit: int = 10):
client = get_bq_client()
table_ref = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
try:
rows = query_table(client, table_ref, limit)
return {"rows": rows}
except Exception as e:
raise HTTPException(status_code=500, detail=str(e))


@app.post("/insert")
def add_row(row: RowIn):
client = get_bq_client()
table_ref = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
try:
errors = insert_rows(client, table_ref, [row.dict()])
if errors:
raise HTTPException(status_code=500, detail=str(errors))
return {"status": "success"}
except Exception as e:
raise HTTPException(status_code=500, detail=str(e))

app/bq_client.py

from google.cloud import bigquery
import os

# If the service account JSON is mounted, set the env var to point to it.
DEFAULT_CREDENTIAL_PATH = "/var/secrets/google/service_account.json"
if os.path.exists(DEFAULT_CREDENTIAL_PATH):
os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", DEFAULT_CREDENTIAL_PATH)

def get_bq_client():
project = os.getenv("BQ_PROJECT_ID")
# If project is None, the bigquery client will try to read from env or default creds
return bigquery.Client(project=project)

def query_table(client: bigquery.Client, table_ref: str, limit: int = 10):
sql = f"SELECT * FROM `{table_ref}` LIMIT {int(limit)}"
query_job = client.query(sql)
results = query_job.result()
return [dict(row) for row in results]

def insert_rows(client: bigquery.Client, table_ref: str, rows: list):
# rows should be list of dicts matching table schema
errors = client.insert_rows_json(table_ref, rows)
return errors


