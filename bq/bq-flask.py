# GCP BigQuery Microservice — Flask Version


This repository contains a simple Flask microservice that queries and updates a BigQuery table in a *separate* GCP project.


## Features
- Flask API endpoints to `query` and `insert` into BigQuery
- Dockerfile for containerization
- Kubernetes deployment manifest (k8s/) for quick testing
- Helm chart (helm-chart/) for parameterized installs


## Authentication options
1. **Service Account JSON mounted as a Secret** (easy, works everywhere)
- Create a service account in the BigQuery project and grant it the appropriate BigQuery roles.
- Download the JSON key and store it in the Kubernetes project as a secret.
- Mount the secret at `/var/secrets/google/service_account.json`.
2. **Workload Identity** (recommended for GKE)
- Configure Workload Identity so your GKE service account can impersonate the BigQuery service account. No JSON keys required.


## Quick start (local)
1. Build the image:
```bash
docker build -t gcr.io/YOUR_K8S_PROJECT/bq-microservice:latest .



Flask
google-cloud-bigquery
gunicorn

FROM python:3.11-slim


WORKDIR /app


COPY app/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt


COPY app /app/app
COPY . /app


ENV PYTHONUNBUFFERED=1


EXPOSE 8080
CMD ["gunicorn", "app.main:app", "--bind", "0.0.0.0:8080"]




# GCP BigQuery Microservice — Flask Version Starter Repo

This starter repo contains a **Flask** Python microservice that runs on GKE (Kubernetes) and reads/writes a BigQuery table located in a different GCP project. The repo includes a Dockerfile, Kubernetes manifests, and a Helm chart so you can deploy to a GKE cluster in another project and mount the BigQuery service account (or use Workload Identity if you prefer).

---

## File tree

```
README.md
app/
  └─ main.py
  └─ bq_client.py
  └─ requirements.txt
Dockerfile
.gitignore
k8s/
  └─ deployment.yaml
  └─ service.yaml
helm-chart/
  └─ Chart.yaml
  └─ values.yaml
  └─ templates/
      └─ deployment.yaml
      └─ service.yaml
      └─ secret.yaml
```

---

## README.md

````markdown
# GCP BigQuery Microservice — Flask Version

This repository contains a simple Flask microservice that queries and updates a BigQuery table in a *separate* GCP project.

## Features
- Flask API endpoints to `query` and `insert` into BigQuery
- Dockerfile for containerization
- Kubernetes deployment manifest (k8s/) for quick testing
- Helm chart (helm-chart/) for parameterized installs

## Authentication options
1. **Service Account JSON mounted as a Secret** (easy, works everywhere)
   - Create a service account in the BigQuery project and grant it the appropriate BigQuery roles.
   - Download the JSON key and store it in the Kubernetes project as a secret.
   - Mount the secret at `/var/secrets/google/service_account.json`.
2. **Workload Identity** (recommended for GKE)
   - Configure Workload Identity so your GKE service account can impersonate the BigQuery service account. No JSON keys required.

## Quick start (local)
1. Build the image:
   ```bash
   docker build -t gcr.io/YOUR_K8S_PROJECT/bq-microservice:latest .
````

2. Run locally (set `GOOGLE_APPLICATION_CREDENTIALS` to a service account JSON path):

   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
   gunicorn app.main:app --bind 0.0.0.0:8080
   ```

## Quick start (GKE with secret)

1. Create a secret in your Kubernetes project:

   ```bash
   kubectl create secret generic bq-sa-key --from-file=service_account.json=/path/to/key.json
   ```
2. Deploy the manifest in `k8s/` or use the Helm chart.

## Endpoints

* `GET /query` — returns sample rows from your BigQuery table
* `POST /insert` — insert a row: JSON body `{ "name": "Alice", "age": 30 }`

## Environment variables

* `BQ_PROJECT_ID` — the BigQuery project that hosts the dataset (required)
* `BQ_DATASET` — dataset name (required)
* `BQ_TABLE` — table name (required)
* If using mounted JSON credentials, mount to `/var/secrets/google/service_account.json` and the app will read via `GOOGLE_APPLICATION_CREDENTIALS`.

````

---

## app/main.py

```python
from flask import Flask, request, jsonify
from .bq_client import get_bq_client, query_table, insert_rows
import os

app = Flask(__name__)

# Read configuration from environment
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")
BQ_DATASET = os.getenv("BQ_DATASET")
BQ_TABLE = os.getenv("BQ_TABLE")

@app.route("/query", methods=["GET"])
def read_rows():
    limit = int(request.args.get("limit", 10))
    client = get_bq_client()
    table_ref = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
    try:
        rows = query_table(client, table_ref, limit)
        return jsonify({"rows": rows})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/insert", methods=["POST"])
def add_row():
    client = get_bq_client()
    table_ref = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
    data = request.get_json()
    if not data or "name" not in data or "age" not in data:
        return jsonify({"error": "Missing 'name' or 'age'"}), 400

    try:
        errors = insert_rows(client, table_ref, [data])
        if errors:
            return jsonify({"status": "error", "errors": errors}), 500
        return jsonify({"status": "success"})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
````

---

## app/bq_client.py

```python
from google.cloud import bigquery
import os

DEFAULT_CREDENTIAL_PATH = "/var/secrets/google/service_account.json"
if os.path.exists(DEFAULT_CREDENTIAL_PATH):
    os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", DEFAULT_CREDENTIAL_PATH)


def get_bq_client():
    project = os.getenv("BQ_PROJECT_ID")
    return bigquery.Client(project=project)


def query_table(client: bigquery.Client, table_ref: str, limit: int = 10):
    sql = f"SELECT * FROM `{table_ref}` LIMIT {int(limit)}"
    query_job = client.query(sql)
    results = query_job.result()
    return [dict(row) for row in results]


def insert_rows(client: bigquery.Client, table_ref: str, rows: list):
    errors = client.insert_rows_json(table_ref, rows)
    return errors
```

---

## app/requirements.txt

```
flask
google-cloud-bigquery
gunicorn
```

---

## Dockerfile

```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY app/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY app /app/app
COPY . /app

ENV PYTHONUNBUFFERED=1

EXPOSE 8080
CMD ["gunicorn", "app.main:app", "--bind", "0.0.0.0:8080"]
```

---

## .gitignore

```
__pycache__/
*.pyc
.env
*.sqlite3
env/
venv/
**/service_account.json
```

---

## k8s/deployment.yaml

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: bq-microservice
spec:
  replicas: 1
  selector:
    matchLabels:
      app: bq-microservice
  template:
    metadata:
      labels:
        app: bq-microservice
    spec:
      containers:
      - name: bq-microservice
        image: gcr.io/YOUR_K8S_PROJECT/bq-microservice:latest
        ports:
        - containerPort: 8080
        env:
        - name: BQ_PROJECT_ID
          value: "YOUR_BIGQUERY_PROJECT_ID"
        - name: BQ_DATASET
          value: "my_dataset"
        - name: BQ_TABLE
          value: "my_table"
        volumeMounts:
        - name: gcp-key
          mountPath: /var/secrets/google
          readOnly: true
      volumes:
      - name: gcp-key
        secret:
          secretName: bq-sa-key
---
apiVersion: v1
kind: Service
metadata:
  name: bq-microservice-service
spec:
  type: LoadBalancer
  selector:
    app: bq-microservice
  ports:
  - port: 80
    targetPort: 8080
```

---

## helm-chart/Chart.yaml

```yaml
apiVersion: v2
name: bq-microservice
description: A Flask microservice that queries/updates BigQuery
type: application
version: 0.1.0
appVersion: "1.0"
```

---

## helm-chart/values.yaml

```yaml
replicaCount: 1
image:
  repository: gcr.io/YOUR_K8S_PROJECT/bq-microservice
  tag: latest
  pullPolicy: IfNotPresent
service:
  type: LoadBalancer
  port: 80
env:
  BQ_PROJECT_ID: "YOUR_BIGQUERY_PROJECT_ID"
  BQ_DATASET: "my_dataset"
  BQ_TABLE: "my_table"
secret:
  enabled: true
  name: bq-sa-key
```

---

## helm-chart/templates/secret.yaml

```yaml
{{- if .Values.secret.enabled }}
apiVersion: v1
kind: Secret
metadata:
  name: {{ .Values.secret.name }}
type: Opaque
data:
  # Create this secret manually or use base64 encoded key.json here
  # service_account.json: {{ .Files.Get "service_account.json" | b64enc }}
{{- end }}
```

---

## helm-chart/templates/deployment.yaml

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: {{ include "bq-microservice.fullname" . }}
spec:
  replicas: {{ .Values.replicaCount }}
  selector:
    matchLabels:
      app: {{ include "bq-microservice.name" . }}
  template:
    metadata:
      labels:
        app: {{ include "bq-microservice.name" . }}
    spec:
      containers:
        - name: {{ include "bq-microservice.name" . }}
          image: "{{ .Values.image.repository }}:{{ .Values.image.tag }}"
          imagePullPolicy: {{ .Values.image.pullPolicy }}
          ports:
            - containerPort: 8080
          env:
            - name: BQ_PROJECT_ID
              value: "{{ .Values.env.BQ_PROJECT_ID }}"
            - name: BQ_DATASET
              value: "{{ .Values.env.BQ_DATASET }}"
            - name: BQ_TABLE
              value: "{{ .Values.env.BQ_TABLE }}"
          volumeMounts:
            - name: gcp-key
              mountPath: /var/secrets/google
              readOnly: true
      volumes:
        - name: gcp-key
          secret:
            secretName: {{ .Values.secret.name }}
```

---

## helm-chart/templates/service.yaml

```yaml
apiVersion: v1
kind: Service
metadata:
  name: {{ include "bq-microservice.fullname" . }}
spec:
  type: {{ .Values.service.type | default "LoadBalancer" }}
  ports:
    - port: {{ .Values.service.port | default 80 }}
      targetPort: 8080
  selector:
    app: {{ include "bq-microservice.name" . }}
```

---

## Next steps

* Optionally integrate **Workload Identity** to eliminate JSON key mounting.
* Add **Cloud Build or GitHub Actions CI/CD** pipeline to build and deploy automatically.

---

*End of Flask starter repo.*

           


curl -X POST http://127.0.0.1:8080/insert \
  -H "Content-Type: application/json" \
  -d '{"name":"Alice","age":28}'

curl "http://127.0.0.1:8080/query?query=SELECT%20*%20FROM%20`project-b.my_dataset.my_table`"

main.py
=======


## app/main.py


```python
from flask import Flask, request, jsonify
from .bq_client import get_bq_client, query_table, insert_rows
import os


app = Flask(__name__)


# Read configuration from environment
BQ_PROJECT_ID = os.getenv("BQ_PROJECT_ID")
BQ_DATASET = os.getenv("BQ_DATASET")
BQ_TABLE = os.getenv("BQ_TABLE")


@app.route("/query", methods=["GET"])
def read_rows():
limit = int(request.args.get("limit", 10))
client = get_bq_client()
table_ref = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
try:
rows = query_table(client, table_ref, limit)
return jsonify({"rows": rows})
except Exception as e:
return jsonify({"error": str(e)}), 500




@app.route("/insert", methods=["POST"])
def add_row():
client = get_bq_client()
table_ref = f"{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
data = request.get_json()
if not data or "name" not in data or "age" not in data:
return jsonify({"error": "Missing 'name' or 'age'"}), 400


try:
errors = insert_rows(client, table_ref, [data])
if errors:
return jsonify({"status": "error", "errors": errors}), 500
return jsonify({"status": "success"})
except Exception as e:
return jsonify({"error": str(e)}), 500



app/bq_client.py

if __name__ == "__main__":
app.run(host="0.0.0.0", port=8080)


from google.cloud import bigquery
import os


DEFAULT_CREDENTIAL_PATH = "/var/secrets/google/service_account.json"
if os.path.exists(DEFAULT_CREDENTIAL_PATH):
os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", DEFAULT_CREDENTIAL_PATH)




def get_bq_client():
project = os.getenv("BQ_PROJECT_ID")
return bigquery.Client(project=project)




def query_table(client: bigquery.Client, table_ref: str, limit: int = 10):
sql = f"SELECT * FROM `{table_ref}` LIMIT {int(limit)}"
query_job = client.query(sql)
results = query_job.result()
return [dict(row) for row in results]




def insert_rows(client: bigquery.Client, table_ref: str, rows: list):
errors = client.insert_rows_json(table_ref, rows)
return errors


