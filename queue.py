import os
import logging
import queue
import threading
from flask import Blueprint, request, jsonify
from google.cloud import storage

# Blueprint
upload_bp = Blueprint("upload", __name__)

# Global upload queue
upload_queue = queue.Queue()


# ---------- Upload Worker ----------
def upload_worker():
    while True:
        try:
            project_name, bucket_name, filename = upload_queue.get()

            client = storage.Client(project=project_name)
            bucket = client.bucket(bucket_name)
            blob_name = os.path.basename(filename)
            blob = bucket.blob(blob_name)

            blob.upload_from_filename(filename)
            logging.info(f"✅ Uploaded {filename} -> gs://{bucket_name}/{blob_name}")

            # Delete file after upload
            if os.path.exists(filename):
                os.remove(filename)
                logging.info(f"🗑️ Deleted local file {filename}")

            upload_queue.task_done()
        except Exception as e:
            logging.error(f"❌ Upload worker failed: {e}")
            upload_queue.task_done()


# ---------- API Endpoint ----------
@upload_bp.route("/upload", methods=["POST"])
def upload_file():
    try:
        data = request.get_json(force=True)
        project_name = data.get("project_name")
        bucket_name = data.get("bucket_name")
        filename = data.get("filename")

        if not project_name or not bucket_name or not filename:
            return jsonify({"error": "project_name, bucket_name and filename required"}), 400

        if not os.path.exists(filename):
            return jsonify({"error": f"File not found: {filename}"}), 404

        # Enqueue file for async upload
        upload_queue.put((project_name, bucket_name, filename))
        logging.info(f"📥 Queued {filename} for upload to gs://{bucket_name}")

        return jsonify({"status": "queued", "filename": filename}), 200

    except Exception as e:
        logging.error(f"Upload enqueue failed: {e}")
        return jsonify({"error": str(e)}), 500


# ---------- Start Worker in Background ----------
def start_upload_worker():
    worker_thread = threading.Thread(target=upload_worker, daemon=True)
    worker_thread.start()
    logging.info("🚀 Upload worker started")
