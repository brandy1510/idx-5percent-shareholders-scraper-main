import argparse
import os
import sys
from datetime import datetime, timedelta

# Add root directory to path so we can import src
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.idx_fetcher import fetch_idx_pdf
from src.pdf_parser import parse_shareholder_pdf
from main import run_etl

def ensure_dirs():
    os.makedirs("downloads", exist_ok=True)
    os.makedirs("results", exist_ok=True)

def test_fetch(date=None, use_scraperapi=False):
    """
    Scenario 1: Fetch PDF from IDX and save to downloads/
    """
    mode_str = "ScraperAPI" if use_scraperapi else "Local Network"
    print(f"--- [TEST] Fetch Mode (Date: {date}, Mode: {mode_str}) ---")
    try:
        result = fetch_idx_pdf(exact_date=date, local_save_path="downloads", use_scraperapi=use_scraperapi)
        # fetch_idx_pdf returns a list now
        if not result:
            print("[WARN] No results found.")
            return

        for res in result:
            filename = res["fileName"]
            # It's already saved if local_save_path is passed, but let's confirm
            # fetch_idx_pdf saves it.
            # actually fetch_idx_pdf saves it if local_save_path is provided.
            # We don't need to write it again if fetch_idx_pdf did it.
            # But let's check how fetch_idx_pdf behaves. 
            # It writes to `metrics/downloads` equivalent? No, it takes `local_save_path`.
            # If we passed "downloads", it saved there.
            print(f"[SUCCESS] Processed: {filename}")
            
    except Exception as e:
        print(f"[ERROR] Fetch failed: {e}")

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

def process_date(date_str, fetch_pdfs=True, fetch_stocks=True, use_scraperapi=False):
    """
    Helper function to process a single date for backfill.
    """
    mode_str = "ScraperAPI" if use_scraperapi else "Local Network"
    print(f"\n[BACKFILL] Processing Date: {date_str} (PDFs: {fetch_pdfs}, Stocks: {fetch_stocks}, Mode: {mode_str}) ...")
    try:
        result = run_etl(
            force_date=date_str, 
            local_save_dir="src/downloads", 
            use_scraperapi=use_scraperapi,
            fetch_pdfs=fetch_pdfs,
            fetch_stocks=fetch_stocks
        )
        print(f"> {date_str}: {result}")
        return f"{date_str}: Success"
    except Exception as e:
        print(f"> {date_str}: Failed or No Data: {e}")
        return f"{date_str}: Failed"

def run_backfill(start_date, end_date, max_workers=5, step="all", use_scraperapi=False):
    """
    Scenario 3: Backfill data from start_date to end_date.
    Uses multi-threading to process days in parallel.
    Step: 'all', 'pdf', 'stock'
    """
    mode_str = "ScraperAPI" if use_scraperapi else "Local Network"
    print(f"--- [TEST] Backfill Mode ({start_date} to {end_date}, Workers: {max_workers}, Step: {step}, Mode: {mode_str}) ---")
    
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

    # Create a partial function or lambda to pass extra args to process_date
    from functools import partial
    
    fetch_pdfs = True
    fetch_stocks = True
    
    if step == "pdf":
        fetch_stocks = False
    elif step == "stock":
        fetch_pdfs = False
        
    process_func = partial(process_date, fetch_pdfs=fetch_pdfs, fetch_stocks=fetch_stocks, use_scraperapi=use_scraperapi)

    # Run with ThreadPoolExecutor
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(process_func, date_list)

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
    parser.add_argument("--step", choices=["all", "pdf", "stock"], default="all", help="Which step to run: all, pdf (only PDFs), or stock (only Stock List)")
    parser.add_argument("--use-api", action="store_true", help="Use ScraperAPI (charges credits). Default is False (Local Network).")
    
    args = parser.parse_args()

    if args.mode == "fetch":
        test_fetch(args.date, use_scraperapi=args.use_api)
        
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
        print(f"--- [TEST] Full End-to-End Flow (ScraperAPI: {args.use_api}) ---")
        run_etl(local_save_dir="src/downloads", use_scraperapi=args.use_api)
        
    elif args.mode == "backfill":
        if not args.start_date or not args.end_date:
            print("[ERROR] --start_date and --end_date are required for backfill mode.")
            return
        run_backfill(args.start_date, args.end_date, args.threads, step=args.step, use_scraperapi=args.use_api)

if __name__ == "__main__":
    main()
