import argparse
import os
import sys
from datetime import datetime, timedelta

# Add root directory to path so we can import src
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.idx_fetcher import fetch_idx_pdf
from src.pdf_parser import parse_shareholder_pdf
from main import run_etl

def ensure_dirs():
    os.makedirs("downloads", exist_ok=True)
    os.makedirs("results", exist_ok=True)

def test_fetch(date=None):
    """
    Scenario 1: Fetch PDF from IDX and save to downloads/
    """
    print(f"--- [TEST] Fetch Mode (Date: {date}) ---")
    try:
        result = fetch_idx_pdf(exact_date=date)
        filename = result["fileName"]
        content = result["pdf_content"]
        
        save_path = os.path.join("downloads", filename)
        with open(save_path, "wb") as f:
            f.write(content.getvalue())
            
        print(f"[SUCCESS] PDF saved to: {save_path}")
        return save_path
    except Exception as e:
        print(f"[ERROR] Fetch failed: {e}")
        return None

def test_parse(file_path):
    """
    Scenario 2: Parse an existing PDF from local path and save CSV to results/
    """
    print(f"--- [TEST] Parse Mode (File: {file_path}) ---")
    if not os.path.exists(file_path):
        print(f"[ERROR] File not found: {file_path}")
        return

    try:
        with open(file_path, "rb") as f:
            df = parse_shareholder_pdf(f)
            
        if df.empty:
            print("[WARN] No data parsed (DataFrame empty).")
            return

        base_name = os.path.basename(file_path).replace(".pdf", "")
        csv_name = f"{base_name}_full.csv"
        save_path = os.path.join("results", csv_name)
        
        df.to_csv(save_path, index=False)
        print(f"[SUCCESS] CSV saved to: {save_path}")
        print(f"Total Rows: {len(df)}")
        
    except Exception as e:
        print(f"[ERROR] Parse failed: {e}")

import concurrent.futures

def process_date(date_str):
    """
    Helper function to process a single date for backfill.
    """
    print(f"\n[BACKFILL] Processing Date: {date_str} ...")
    try:
        # User requested: local save + GCS upload.
        # run_etl will call save_or_upload, which now does BOTH local (results/) and GCS.
        # But we also want PDFs in src/downloads.
        result = run_etl(
            force_date=date_str, 
            local_save_dir="src/downloads", 
            use_scraperapi=False # User requested: "not using the scraperapi when we run local testing" -> assume applies here too?
            # Or maybe they want real run? "store result locally then the final result push to Google Cloud Storage"
            # If pushing to GCS, maybe we want reliable data?
            # User said "store result locally then the final result push to Google Cloud Storage"
            # User previously said: "not using the scraperapi when we run local testing"
            # I'll stick to use_scraperapi=False for now to save credits as per previous pattern, unless it fails.
        )
        print(f"> {date_str}: {result}")
        return f"{date_str}: Success"
    except Exception as e:
        print(f"> {date_str}: Failed or No Data: {e}")
        return f"{date_str}: Failed"

def run_backfill(start_date, end_date, max_workers=5):
    """
    Scenario 3: Backfill data from start_date to end_date.
    Uses multi-threading to process days in parallel.
    """
    print(f"--- [TEST] Backfill Mode ({start_date} to {end_date}, Workers: {max_workers}) ---")
    
    try:
        # Supported formats: YYYYMMDD or YYYY-MM-DD
        if "-" in start_date:
            start = datetime.strptime(start_date, "%Y-%m-%d")
        else:
            start = datetime.strptime(start_date, "%Y%m%d")
            
        if "-" in end_date:
            end = datetime.strptime(end_date, "%Y-%m-%d")
        else:
            end = datetime.strptime(end_date, "%Y%m%d")
            
    except ValueError:
        print("[ERROR] Invalid date format. Please use YYYYMMDD or YYYY-MM-DD.")
        return

    # Generate list of dates
    date_list = []
    current = start
    while current <= end:
        date_list.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)
        
    print(f"[INFO] Queuing {len(date_list)} dates with {max_workers} threads...")

    # Run with ThreadPoolExecutor
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(process_date, date_list)

def list_downloaded_pdfs():
    pdfs = [f for f in os.listdir("downloads") if f.lower().endswith(".pdf")]
    return sorted(pdfs, reverse=True)

def main():
    ensure_dirs()
    
    parser = argparse.ArgumentParser(description="IDX Scraper Test Runner")
    parser.add_argument("mode", choices=["fetch", "parse", "full", "backfill"], help="Test mode")
    parser.add_argument("--date", help="YYYYMMDD for fetch mode (optional)")
    parser.add_argument("--file", help="Path to PDF file for parse mode (optional)")
    parser.add_argument("--start_date", help="YYYYMMDD Start date for backfill")
    parser.add_argument("--end_date", help="YYYYMMDD End date for backfill")
    
    parser.add_argument("--threads", type=int, default=5, help="Number of threads for backfill")
    
    args = parser.parse_args()

    if args.mode == "fetch":
        test_fetch(args.date)
        
    elif args.mode == "parse":
        target_file = args.file
        
        # If no file specified, pick the latest from downloads/
        if not target_file:
            pdfs = list_downloaded_pdfs()
            if pdfs:
                target_file = os.path.join("downloads", pdfs[0])
                print(f"[INFO] No file specified. Using latest download: {target_file}")
            else:
                print("[ERROR] No PDFs found in downloads/. Please run fetch mode first or specify --file.")
                return
                
        test_parse(target_file)
        
    elif args.mode == "full":
        print("--- [TEST] Full End-to-End Flow ---")
        run_etl(local_save_dir="src/downloads", use_scraperapi=False)
        
    elif args.mode == "backfill":
        if not args.start_date or not args.end_date:
            print("[ERROR] --start_date and --end_date are required for backfill mode.")
            return
        run_backfill(args.start_date, args.end_date, args.threads)

if __name__ == "__main__":
    main()
