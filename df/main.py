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


import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.options.pipeline_options import PipelineOptions
import csv
import os
from apache_beam.io import filesystems

# --------------------------
# Custom DoFn: Parse CSV
# --------------------------
class ParseCSV(beam.DoFn):
    def process(self, element):
        """Parses each CSV line into a dict."""
        line = element.decode('utf-8').strip()
        reader = csv.reader([line])
        for row in reader:
            if len(row) == 4:
                yield {
                    "id": int(row[0]),
                    "name": row[1],
                    "dept": row[2],
                    "description": row[3],
                }

# --------------------------
# Custom DoFn: Move processed files
# --------------------------
class MoveToProcessed(beam.DoFn):
    def process(self, element):
        source_path = element.metadata.path
        processed_prefix = os.environ['PROCESSED_PREFIX']
        file_name = os.path.basename(source_path)
        dest_path = os.path.join(processed_prefix, file_name)

        # Copy file then delete original
        filesystems.FileSystems.copy([source_path], [dest_path])
        filesystems.FileSystems.delete([source_path])
        yield f"Moved {file_name} → {dest_path}"

# --------------------------
# Main
# --------------------------
def run():
    project_id = os.environ['PROJECT_ID']
    region = os.environ['REGION']
    input_pattern = os.environ['INPUT_PATH']
    output_table = os.environ['OUTPUT_TABLE']
    temp_location = os.environ['TEMP_LOCATION']
    staging_location = os.environ['STAGING_LOCATION']

    pipeline_options = PipelineOptions(
        project=project_id,
        region=region,
        temp_location=temp_location,
        staging_location=staging_location,
        save_main_session=True,
        streaming=False
    )

    with beam.Pipeline(options=pipeline_options) as p:
        files = (
            p
            | "MatchFiles" >> fileio.MatchFiles(input_pattern)
            | "ReadMatches" >> fileio.ReadMatches()
        )

        (
            files
            | "ReadLines" >> beam.FlatMap(lambda file: file.read_utf8().splitlines())
            | "ParseCSV" >> beam.ParDo(ParseCSV())
            | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
                output_table,
                schema="id:INTEGER,name:STRING,dept:STRING,description:STRING",
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            )
        )

        # Move processed files
        _ = files | "MoveProcessedFiles" >> beam.ParDo(MoveToProcessed())

if __name__ == "__main__":
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


