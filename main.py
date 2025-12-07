import os
import sys
import functions_framework
from datetime import datetime, timedelta
from idx_fetcher import fetch_idx_pdf
from pdf_parser import parse_shareholder_pdf

# Try to import GCS uploader
try:
    from gcs_uploader import upload_to_gcs
    GCS_AVAILABLE = True
except ImportError:
    GCS_AVAILABLE = False


def get_target_date():
    """
    Determine the target date based on the current day.
    - If Monday (0) -> Get last Friday (-3 days)
    - If Weekday (1-4) -> Get Yesterday (-1 day)
    - If Weekend (5-6) -> Get Yesterday (-1 day) (Default behavior)
    """
    today = datetime.now()
    weekday = today.weekday()

    if weekday == 0:  # Monday
        target_date = today - timedelta(days=3)
        print(f"Today is Monday. Fetching data for last Friday: {target_date.strftime('%Y-%m-%d')}")
    else:
        target_date = today - timedelta(days=1)
        print(f"Fetching data for Yesterday: {target_date.strftime('%Y-%m-%d')}")
    
    return target_date.strftime("%Y%m%d")

def run_etl():
    print("Starting IDX Shareholder ETL (GCF)...")
    
    # Config
    bucket_name = os.environ.get("BUCKET_NAME", "data-dev-01")
    base_prefix = "stock_market/data_kepentingan"
    
    try:
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
        full_df, filtered_df = parse_shareholder_pdf(pdf_content)
        
        if full_df.empty and filtered_df.empty:
            return "No data found."

        # 3. Save / Upload
        csv_full = base_filename + "_full.csv"
        csv_filtered = base_filename + ".csv"
        
        full_csv_str = full_df.to_csv(index=False)
        filtered_csv_str = filtered_df.to_csv(index=False)
        
        if GCS_AVAILABLE:
            print(f"Uploading to GCS Bucket: {bucket_name}")
            
            # Upload Full Data (Raw) matches the main table definition
            # Path: stock_market/data_kepentingan/dt=YYYY-MM-DD/filename_full.csv
            blob_name_full = f"{base_prefix}/{hive_partition}/{csv_full}"
            upload_to_gcs(bucket_name, blob_name_full, full_csv_str)
            
            # Upload Filtered Data (Changes Only) to a variant path
            # Path: stock_market/data_kepentingan_changes/dt=YYYY-MM-DD/filename.csv
            blob_name_filtered = f"stock_market/data_kepentingan_changes/{hive_partition}/{csv_filtered}"
            upload_to_gcs(bucket_name, blob_name_filtered, filtered_csv_str)
            
        else:
            # Fallback for local testing or if GCS unavailable
            # GCF 'tmp' is writable
            local_dir = "/tmp" if os.path.exists("/tmp") else "results"
            os.makedirs(local_dir, exist_ok=True)
            
            with open(os.path.join(local_dir, csv_full), "w", encoding="utf-8") as f:
                f.write(full_csv_str)
            with open(os.path.join(local_dir, csv_filtered), "w", encoding="utf-8") as f:
                f.write(filtered_csv_str)
                
            print(f"[INFO] No bucket configured or GCS unavailable. Saved to {local_dir}")

        return f"Success. Processed {original_filename}. Full: {len(full_df)}, Filtered: {len(filtered_df)}"
        
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
