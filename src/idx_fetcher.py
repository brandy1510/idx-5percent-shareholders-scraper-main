import io
import os
import urllib.parse
from datetime import datetime, timedelta
from curl_cffi import requests
# Fallback to standard requests for ScraperAPI to avoid confusion, 
# but curl_cffi acts as drop-in. We'll use curl_cffi's requests for consistency.

BASE_URL = "https://www.idx.co.id/primary/ListedCompany/GetAnnouncement"

def fetch_idx_pdf(exact_date=None, local_save_path=None, use_scraperapi=True):
    """
    Fetch IDX announcements filtered by 'Pemegang Saham di atas 5%' 
    and return the attachment content as BytesIO or file path.
    
    Args:
        exact_date (str): YYYYMMDD date string to filter by announcement date.
        local_save_path (str): Directory path to save PDF locally. If None, uses memory.
        use_scraperapi (bool): Whether to use ScraperAPI or direct connection.
    
    Returns:
        dict: {
            "title": ...,
            "announcementDate": ...,
            "fileDate": ...,  # Extracted from filename
            "attachmentUrl": ...,
            "fileName": ...,
            "pdf_content": BytesIO object or str (path)
        }
    """

    today_str = datetime.today().strftime("%Y%m%d")

    # === Determine search mode ===
    if exact_date is None:
        # Latest mode
        params = {
            "kodeEmiten": "",
            "emitenType": "*",
            "indexFrom": 0,
            "pageSize": 10,
            "dateFrom": "19010101",
            "dateTo": today_str,
            "lang": "id",
            "keyword": "Pemegang Saham di atas 5%"
        }
    else:
        dt_from = datetime.strptime(exact_date, "%Y%m%d")
        dt_to = dt_from # Exact date means single day
        date_to = dt_to.strftime("%Y%m%d")

        params = {
            "kodeEmiten": "",
            "emitenType": "*",
            "indexFrom": 0,
            "pageSize": 10,
            "dateFrom": exact_date,
            "dateTo": date_to,
            "lang": "id",
            "keyword": "Pemegang Saham di atas 5%"
        }

    # === Fetch data ===
    scraperapi_key = os.environ.get("SCRAPERAPI_KEY")
    data = []
    
    if use_scraperapi and scraperapi_key:
        print(f"[INFO] Using ScraperAPI...")
        # Construct full target URL with params manually for the 'url' param
        query_string = urllib.parse.urlencode(params)
        target_url = f"{BASE_URL}?{query_string}"
        
        payload = {
            'api_key': scraperapi_key, 
            'url': target_url
        }
        
        try:
            # We can use curl_cffi requests as standard requests here
            response = requests.get('https://api.scraperapi.com/', params=payload, timeout=60)
            response.raise_for_status()
            data = response.json().get("Replies", [])
        except Exception as e:
            raise RuntimeError(f"ScraperAPI Failed: {e}")
            
    else:
        # Fallback to direct connection / Proxy / TLS Impersonation
        # === Fetch data with curl_cffi (J3 Fingerprint Impersonation) ===
        # Using 'chrome110' impersonation to match recent browser TLS signatures
        proxy_url = os.environ.get("PROXY_URL")
        if proxy_url and not proxy_url.startswith(("http://", "https://")):
            proxy_url = f"http://{proxy_url}"
    
        proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else None
        
        if proxy_url:
            print(f"[INFO] Using Proxy: {proxy_url}")
        
        print(f"[INFO] Fetching from: {BASE_URL} with params: {params} (using curl_cffi)")
        
        try:
            response = requests.get(
                BASE_URL, 
                params=params, 
                proxies=proxies,
                impersonate="chrome110",
                headers={
                    "Referer": "https://www.idx.co.id/primary/ListedCompany/Index",
                    "Origin": "https://www.idx.co.id",
                    "Accept": "application/json, text/javascript, */*; q=0.01",
                    "X-Requested-With": "XMLHttpRequest"
                },
                timeout=30
            )
            response.raise_for_status()
            
            data = response.json().get("Replies", [])
        except Exception as e:
            raise RuntimeError(f"Failed to fetch IDX data (WAF Blocked?): {e}")

    if not data:
        raise ValueError("No announcements found for the given parameters")

    # Sort announcements by date descending (latest first)
    data.sort(key=lambda x: x["pengumuman"].get(
        "TglPengumuman") or "", reverse=True)

    # === Find _lamp attachment ===
    results = []
    
    for item in data:
        pengumuman = item["pengumuman"]
        attachments = item.get("attachments", [])
        tgl_pengumuman = pengumuman.get("TglPengumuman", "")

        for attachment in attachments:
            file_name = attachment.get("OriginalFilename", "")
            if "_lamp" not in file_name.lower():
                continue

            # Check exact date if filtering enabled (still check announcement date for filtering scope)
            if exact_date:
                date_str = datetime.strptime(
                    tgl_pengumuman[:10], "%Y-%m-%d").strftime("%Y%m%d")
                if date_str != exact_date:
                    continue

            # Extract date from filename
            # Standard format assumption: 20251208_DPS5_lamp.pdf -> 20251208
            try:
                file_date = file_name.split('_')[0]
                # Validate it's a date-like string (digits) and length 8
                if not (file_date.isdigit() and len(file_date) == 8):
                    print(f"[WARN] Filename '{file_name}' does not start with YYYYMMDD date. Using fallback.")
                    file_date = datetime.strptime(tgl_pengumuman[:10], "%Y-%m-%d").strftime("%Y%m%d")
            except Exception:
                file_date = datetime.strptime(tgl_pengumuman[:10], "%Y-%m-%d").strftime("%Y%m%d")

            # --- Download logic (In-Memory or Local) ---
            print(f"[INFO] Downloading {file_name} ...")
            pdf_url = attachment["FullSavePath"]
            
            # Download PDF
            if use_scraperapi and scraperapi_key:
                print(f"[INFO] Downloading via ScraperAPI...")
                payload_pdf = {
                    'api_key': scraperapi_key, 
                    'url': pdf_url
                }
                pdf_data = requests.get('https://api.scraperapi.com/', params=payload_pdf, timeout=120)
            else:
                # Direct / Proxy download using the same session/impersonation
                
                # Setup proxy again locally just to be sure if not set above (e.g. ScraperAPI key exists but use_scraperapi=False)
                proxy_url_env = os.environ.get("PROXY_URL")
                proxies_local = None
                if proxy_url_env and not proxy_url_env.startswith(("http://", "https://")):
                    proxy_url_env = f"http://{proxy_url_env}"
                if proxy_url_env:
                    proxies_local = {"http": proxy_url_env, "https": proxy_url_env}
                
                pdf_data = requests.get(pdf_url, proxies=proxies_local, impersonate="chrome110", timeout=60)
            
            pdf_data.raise_for_status()
            
            if local_save_path:
                os.makedirs(local_save_path, exist_ok=True)
                full_path = os.path.join(local_save_path, file_name)
                with open(full_path, "wb") as f:
                    f.write(pdf_data.content)
                print(f"[SUCCESS] Saved locally to {full_path}")
                pdf_content = full_path
            else:
                # Create BytesIO object
                pdf_content = io.BytesIO(pdf_data.content)
                print(f"[SUCCESS] Downloaded to memory.")

            results.append({
                "title": pengumuman.get("JudulPengumuman"),
                "announcementDate": tgl_pengumuman,
                "fileDate": file_date,
                "attachmentUrl": attachment.get("FullSavePath"),
                "fileName": file_name,
                "pdf_content": pdf_content
            })
            
    if results:
        return results

    raise ValueError("No '_lamp' attachment found for the given parameters")
