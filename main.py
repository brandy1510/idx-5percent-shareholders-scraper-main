import os
import sys
import functions_framework
from datetime import datetime, timedelta
from src.idx_fetcher import fetch_idx_pdf
from src.pdf_parser import parse_shareholder_pdf
from src.stock_list_scraper import fetch_stock_list

# Try to import GCS uploader
try:
    from src.gcs_uploader import upload_to_gcs
    GCS_AVAILABLE = True
except ImportError:
    GCS_AVAILABLE = False

# Load .env for local testing (silent failure if missing)
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


def get_target_date():
    """
    Determine the target date based on the current day.
    - If Monday (0) -> Get last Friday (-3 days)
    - If Weekday (1-4) -> Get Yesterday (-1 day)
    - If Weekend (5-6) -> Get Yesterday (-1 day) (Default behavior)
    """
    today = datetime.now()
    weekday = today.weekday()

    if weekday == 0:  # Monday -> Data from last Friday
        target_date = today - timedelta(days=3)
        print(f"Today is Monday. Fetching data for last Friday: {target_date.strftime('%Y-%m-%d')}")
    elif weekday == 6:  # Sunday -> Data from last Friday
        target_date = today - timedelta(days=2)
        print(f"Today is Sunday. Fetching data for last Friday: {target_date.strftime('%Y-%m-%d')}")
    else:  # Tue-Sat -> Data from Yesterday (Mon-Fri)
        target_date = today - timedelta(days=1)
        print(f"Fetching data for Yesterday: {target_date.strftime('%Y-%m-%d')}")
    
    return target_date.strftime("%Y%m%d")

def save_or_upload(content, local_filename, gcs_blob_path, bucket_name, project_id, content_type="text/plain"):
    """
    Helper to save content to local file (fallback) or upload to GCS.
    """
    # GCS Upload
    if GCS_AVAILABLE and bucket_name:
        print(f"Uploading to GCS Bucket: {bucket_name}/{gcs_blob_path}")
        return upload_to_gcs(bucket_name, gcs_blob_path, content, content_type=content_type, project_id=project_id)
    
    # Local Fallback
    local_dir = "/tmp" if os.path.exists("/tmp") else "results"
    os.makedirs(local_dir, exist_ok=True)
    local_path = os.path.join(local_dir, local_filename)
    
    with open(local_path, "w", encoding="utf-8") as f:
        f.write(content)
        
    print(f"[INFO] Saved locally to {local_path}")
    return True

def run_etl(force_date=None):
    print("Starting IDX Shareholder ETL (GCF)...")
    
    # Config
    project_id = os.environ.get("GOOGLE_CLOUD_PROJECT")
    bucket_name = os.environ.get("BUCKET_NAME")
    base_prefix = os.environ.get("GCS_BASE_PREFIX", "shareholder_data")
    
    try:
        if force_date:
            target_date_str = force_date
            print(f"Using Forced Target Date: {target_date_str}")
        else:
            target_date_str = get_target_date()
            print(f"Target Date (YYYYMMDD): {target_date_str}")
        
        # Format date for Hive Partition (dt=YYYY-MM-DD)
        date_obj = datetime.strptime(target_date_str, "%Y%m%d")
        partition_date = date_obj.strftime("%Y-%m-%d")
        hive_partition = f"dt={partition_date}"
        
        # 1. Fetch PDF
        print("Fetching PDF from IDX...")
        fetch_result = fetch_idx_pdf(exact_date=target_date_str)
        
        pdf_content = fetch_result["pdf_content"]
        original_filename = fetch_result["fileName"]
        base_filename = os.path.splitext(original_filename)[0]
        
        print(f"PDF fetched into memory. Size: {pdf_content.getbuffer().nbytes} bytes")
        
        # 2. Parse PDF
        print("Parsing PDF...")
        full_df = parse_shareholder_pdf(pdf_content)
        
        if full_df.empty:
            return "No data found."

        # 3. Save / Upload PDF Data
        csv_full = base_filename + "_full.csv"
        full_csv_str = full_df.to_csv(index=False)
        
        # Path: stock_market/data_kepentingan/dt=YYYY-MM-DD/filename_full.csv
        blob_name_full = f"{base_prefix}/{hive_partition}/{csv_full}"
        
        save_or_upload(
            full_csv_str, 
            csv_full, 
            blob_name_full, 
            bucket_name, 
            project_id, 
            content_type="text/csv"
        )

        # 4. Fetch and Upload Stock List (Daftar Saham)
        print("Fetching Stock List (Daftar Saham)...")
        # Key is loaded internally by fetch_stock_list from env
        
        stock_list_data = fetch_stock_list()
        
        if stock_list_data:
            import json
            # Convert to NDJSON (Newline Delimited JSON)
            # Dump each dict to a string, then join with newlines
            stock_json_str = "\n".join([json.dumps(record, ensure_ascii=False) for record in stock_list_data])
            
            # Use config for prefix
            stock_data_prefix = os.environ.get("STOCK_DATA_PREFIX", "stock_market/data_emiten")
            
            # Static filename to overwrite each run
            stock_filename = "idx_stock_list.json"
            stock_blob_path = f"{stock_data_prefix}/{stock_filename}"
            
            save_or_upload(
                stock_json_str,
                stock_filename,
                stock_blob_path,
                bucket_name, # Use standard bucket_name logic (from env)
                project_id,
                content_type="application/x-ndjson"
            )
        else:
            print("[WARN] Failed to fetch stock list or no data returned.")

        return f"Success. Processed {original_filename}. Rows: {len(full_df)}. Stock List parsed."
        
    except ValueError as ve:
        error_msg = f"No data found: {ve}"
        print(error_msg)
        return error_msg
    except Exception as e:
        error_msg = f"An error occurred: {e}"
        print(error_msg)
        # Re-raise so GCF marks it as failed? Or just return error?
        # Returning error string for simple HTTP response
        raise RuntimeError(error_msg)

@functions_framework.http
def idx_scraper_entry(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    """
    try:
        result = run_etl()
        return result, 200
    except Exception as e:
        return str(e), 500

if __name__ == "__main__":
    # Local debugging
    run_etl()
