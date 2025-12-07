from google.cloud import storage
import os

def upload_to_gcs(bucket_name, blob_name, data, content_type="text/csv"):
    """
    Uploads data to a Google Cloud Storage bucket.
    
    Args:
        bucket_name (str): Name of the GCS bucket.
        blob_name (str): Destination path in the bucket.
        data (str or bytes): Data to upload.
        content_type (str): Content type of the file.
    """
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        
        blob.upload_from_string(data, content_type=content_type)
        print(f"[SUCCESS] Uploaded {blob_name} to gs://{bucket_name}/{blob_name}")
        return True
    except Exception as e:
        print(f"[ERROR] Failed to upload to GCS: {e}")
        return False
