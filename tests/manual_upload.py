import os
import sys
from dotenv import load_dotenv

# Add src to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.gcs_uploader import upload_to_gcs

def manual_uploader():
    load_dotenv()
    
    # Configuration
    target_date = "20251217"
    formatted_date = "2025-12-17"
    results_dir = "results"
    
    # GCS Config
    bucket_name = os.environ.get("BUCKET_NAME")
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
    base_prefix = os.environ.get("GCS_BASE_PREFIX", "shareholder_data")
    
    if not bucket_name:
        print("[ERROR] BUCKET_NAME not set in .env")
        return

    # Find the file
    print(f"Searching for CSVs for date {target_date} in {results_dir}...")
    files = [f for f in os.listdir(results_dir) if f.startswith(target_date) and f.endswith(".csv")]
    
    if not files:
        print("[ERROR] No matching CSV found.")
        return

    for filename in files:
        file_path = os.path.join(results_dir, filename)
        print(f"Found: {filename}")
        
        # Read content
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
            
        # Construct Blob Path
        # shareholder_data/dt=YYYY-MM-DD/filename.csv
        hive_partition = f"dt={formatted_date}"
        blob_name = f"{base_prefix}/{hive_partition}/{filename}"
        
        print(f"Uploading to: gs://{bucket_name}/{blob_name}")
        
        success = upload_to_gcs(
            bucket_name=bucket_name,
            blob_name=blob_name,
            data=content,
            content_type="text/csv",
            project_id=project_id
        )
        
        if success:
            print("[SUCCESS] Manual upload complete.")
        else:
            print("[ERROR] Upload failed.")

if __name__ == "__main__":
    manual_uploader()
