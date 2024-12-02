import functions_framework
import os
import csv
import json
from google.cloud import storage, pubsub_v1


storage_client = storage.Client()
publisher = pubsub_v1.PublisherClient()

ENV_VAR_MSG = "Specified environment variable is not set."
PUBSUB_TOPIC = os.getenv('PUBSUB_TOPIC', ENV_VAR_MSG)

@functions_framework.cloud_event
def process_file(cloud_event):
    """Triggered when a file is uploaded to GCS. Publishes each row of the CSV to a Pub/Sub topic."""
    bucket_name = cloud_event.data['bucket']
    file_name = cloud_event.data['name']
    
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    csv_data = blob.download_as_text()
    
    reader = csv.DictReader(csv_data.splitlines())
    for row in reader:
        message = json.dumps(row).encode('utf-8')
        publisher.publish(PUBSUB_TOPIC, message)
    
    return f"File {file_name} processed. Rows published to Pub/Sub."
