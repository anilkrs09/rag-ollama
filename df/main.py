dataflow-csv-project/
├── jenkins/
│   └── Jenkinsfile                # CI/CD pipeline
├── pipeline/
│   ├── main.py                     # Dataflow Python pipeline
│   ├── requirements.txt
│   ├── Dockerfile
│   └── metadata.json
└── helm/
    └── dataflow-job/
        ├── Chart.yaml
        ├── values.yaml
        └── templates/
            └── job.yaml


import os
import logging
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import storage

class MoveFileFn(beam.DoFn):
    def __init__(self, bucket_name, processed_prefix):
        self.bucket_name = bucket_name
        self.processed_prefix = processed_prefix

    def setup(self):
        self.client = storage.Client()
        self.bucket = self.client.bucket(self.bucket_name)

    def process(self, file_path):
        blob = self.bucket.blob(file_path)
        dest_blob = self.bucket.blob(f"{self.processed_prefix}{os.path.basename(file_path)}")
        self.bucket.copy_blob(blob, self.bucket, dest_blob.name)
        blob.delete()
        yield f"Moved {file_path} to {dest_blob.name}"

def parse_csv(line):
    fields = line.split(',')
    return {
        'id': fields[0],
        'name': fields[1],
        'dept': fields[2],
        'description': fields[3]
    }

def run():
    project = os.environ['PROJECT_ID']
    region = os.environ['REGION']
    input_path = os.environ['INPUT_PATH']
    output_table = os.environ['OUTPUT_TABLE']
    temp_location = os.environ['TEMP_LOCATION']
    staging_location = os.environ['STAGING_LOCATION']
    processed_prefix = os.environ['PROCESSED_PREFIX']

    pipeline_options = PipelineOptions(
        project=project,
        region=region,
        temp_location=temp_location,
        staging_location=staging_location,
        runner='DataflowRunner',
        save_main_session=True
    )

    bucket_name = input_path.split('/')[2]

    with beam.Pipeline(options=pipeline_options) as p:
        files = p | "Match CSV Files" >> beam.io.MatchFiles(input_path)
        matched = files | "Read Matches" >> beam.io.ReadMatches()

        lines = (
            matched
            | "Read CSV Lines" >> beam.FlatMap(
                lambda fm: [line.decode('utf-8').strip() for line in beam.io.filesystems.FileSystems.open(fm.path)]
            )
            | "Filter Header" >> beam.Filter(lambda line: not line.startswith("id"))
            | "Parse CSV" >> beam.Map(parse_csv)
        )

        lines | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
            output_table,
            schema={
                'fields': [
                    {'name': 'id', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'dept', 'type': 'STRING', 'mode': 'NULLABLE'},
                    {'name': 'description', 'type': 'STRING', 'mode': 'NULLABLE'}
                ]
            },
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )

        (
            matched
            | "Extract Paths" >> beam.Map(lambda f: f.path)
            | "Move Files" >> beam.ParDo(MoveFileFn(bucket_name, processed_prefix))
        )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()



{
  "name": "csv-to-bq-flex-template",
  "description": "Read CSV from GCS, write to BigQuery, move processed files using env vars"
}



apiVersion: batch/v1
kind: Job
metadata:
  name: dataflow-csv-job
spec:
  template:
    spec:
      serviceAccountName: dataflow-job-sa
      containers:
        - name: dataflow-launcher
          image: google/cloud-sdk:slim
          env:
            - name: PROJECT_ID
              value: "{{ .Values.projectId }}"
            - name: REGION
              value: "{{ .Values.region }}"
            - name: TEMPLATE_PATH
              value: "{{ .Values.templatePath }}"
            - name: INPUT_PATH
              value: "{{ .Values.inputPath }}"
            - name: OUTPUT_TABLE
              value: "{{ .Values.outputTable }}"
            - name: TEMP_LOCATION
              value: "{{ .Values.tempLocation }}"
            - name: STAGING_LOCATION
              value: "{{ .Values.stagingLocation }}"
            - name: PROCESSED_PREFIX
              value: "{{ .Values.processedPrefix }}"
          command:
            - /bin/sh
            - -c
            - |
              gcloud dataflow flex-template run csv-to-bq-$(date +%s) \
                --project=$PROJECT_ID \
                --region=$REGION \
                --template-file-gcs-location=$TEMPLATE_PATH \
                --parameters INPUT_PATH=$INPUT_PATH,OUTPUT_TABLE=$OUTPUT_TABLE,TEMP_LOCATION=$TEMP_LOCATION,STAGING_LOCATION=$STAGING_LOCATION,PROCESSED_PREFIX=$PROCESSED_PREFIX
      restartPolicy: Never
  backoffLimit: 1


