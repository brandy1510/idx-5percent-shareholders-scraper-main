import os
import sys
from datetime import datetime, timedelta
from idx_fetcher import fetch_idx_pdf
from pdf_parser import parse_shareholder_pdf

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

def main():
    print("Starting IDX Shareholder ETL...")
    
    try:
        target_date_str = get_target_date()
        
        print(f"Target Date (YYYYMMDD): {target_date_str}")
        
        # 1. Fetch PDF
        print("Fetching PDF from IDX...")
        fetch_result = fetch_idx_pdf(exact_date=target_date_str)
        
        pdf_content = fetch_result["pdf_content"]
        original_filename = fetch_result["fileName"]
        csv_filename = os.path.splitext(original_filename)[0] + ".csv"
        
        print(f"PDF fetched into memory. Size: {pdf_content.getbuffer().nbytes} bytes")
        
        # 2. Parse PDF
        print("Parsing PDF...")
        df = parse_shareholder_pdf(pdf_content, csv_filename)
        
        if df.empty:
            print("No significant shareholder changes found.")
        else:
            print(f"Successfully parsed {len(df)} rows.")
            
        print("ETL Process Completed Successfully.")
        
    except ValueError as ve:
        print(f"No data found: {ve}")
    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
