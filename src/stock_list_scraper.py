import os
import json
import time
import urllib.parse
import requests 
from datetime import datetime

def fetch_stock_list(api_key=None):
    """
    Fetches the list of all stocks from IDX using the GetCompanyProfiles endpoint.
    Uses ScraperAPI if SCRAPERAPI_KEY is found in env or passed as arg.
    """
    if not api_key:
        api_key = os.environ.get("SCRAPERAPI_KEY")

    base_url = "https://www.idx.co.id/primary/ListedCompany/GetCompanyProfiles"
    
    # Common params
    idx_params = {
        "indexFrom": 0,
        "pageSize": 100,
        "start": 0,
        "length": 100,
        "kodeEmiten": "",
        "emitenType": "s",
        "sort": "KodeEmiten",
        "order": "asc"
    }

    headers = {
        "Referer": "https://www.idx.co.id/data-pasar/data-saham/daftar-saham",
        "Origin": "https://www.idx.co.id",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest",
         "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
    }
    
    print(f"[INFO] Fetching stock list from {base_url}...")
    
    all_profiles = []
    
    while True:
        try:
            # Construct target URL with params for the current page
            # We must manually encode params into the URL string if using ScraperAPI
            # because ScraperAPI needs the full 'url' param.
            
            if api_key:
                # ScraperAPI mode
                query_string = urllib.parse.urlencode(idx_params)
                target_url = f"{base_url}?{query_string}"
                
                payload = {
                    'api_key': api_key,
                    'url': target_url
                }
                
                # print(f"[DEBUG] Requesting via ScraperAPI: {target_url}")
                response = requests.get('https://api.scraperapi.com/', params=payload, timeout=60)
                
            else:
                # Direct mode (likely to fail WAF without curl_cffi or if IP is blocked)
                print("[WARN] No API Key provided, trying direct connection...")
                response = requests.get(
                    base_url, 
                    params=idx_params, 
                    headers=headers, 
                    timeout=30
                )

            response.raise_for_status()
            
            # ScraperAPI sometimes returns 200 with error text if the target failed, 
            # but usually it relays status. 
            # IDX returns JSON.
            
            try:
                data = response.json()
            except json.JSONDecodeError:
                print(f"[ERROR] Failed to decode JSON. Response text: {response.text[:200]}")
                break
            
            # Check if response has the expected structure
            if "data" not in data:
                print(f"[ERROR] Unexpected response format: {data.keys()}")
                break
                
            profiles = data["data"]
            total_records = data.get("recordsTotal", 0)
            
            if not profiles:
                break
                
            all_profiles.extend(profiles)
            print(f"[INFO] Fetched {len(profiles)} records... (Total so far: {len(all_profiles)}/{total_records})")
            
            if len(all_profiles) >= total_records:
                break
                
            # Next page - based on actual received count
            increment = len(profiles)
            idx_params["indexFrom"] += increment
            idx_params["start"] += increment
            
            time.sleep(0.5) # Be polite
            
        except Exception as e:
            print(f"[ERROR] Failed to fetch stock list: {e}")
            break
            
    return all_profiles

def save_to_file(data):
    if not data:
        print("[WARN] No data to save.")
        return

    # Ensure data directory exists
    os.makedirs("data", exist_ok=True)
    
    filename = "data/idx_stock_list.json"
    
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
        
    print(f"[INFO] Data saved to {filename}")

if __name__ == "__main__":
    # Local test
    key = os.environ.get("SCRAPERAPI_KEY")
    
    if not key:
        print("[WARN] SCRAPERAPI_KEY not found in env. Scraper might fail if WAF blocks.")
        
    stock_data = fetch_stock_list(api_key=key)
    save_to_file(stock_data)
