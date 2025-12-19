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
    # === Fetch data ===
    # Use request_helper to handle API/Direct logic
    from src.request_helper import make_request
    
    headers = {
        "Referer": "https://www.idx.co.id/primary/ListedCompany/Index",
        "Origin": "https://www.idx.co.id",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest"
    }

    try:
        response = make_request(
            target_url=BASE_URL,
            params=params,
            headers=headers,
            use_api=use_scraperapi,
            timeout=60 # Increased from 30 to 60 for slower proxies
        )
        response.raise_for_status()
        try:
            data = response.json().get("Replies", [])
        except Exception as e:
            print(f"[ERROR] JSON Decode Failed. Response Text (first 500 chars): {response.text[:500]}")
            raise e
        
    except Exception as e:
        raise RuntimeError(f"Failed to fetch IDX data: {e}")
            
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
            
            # Use request_helper for PDF download
            try:
                pdf_data = make_request(
                    target_url=pdf_url,
                    use_api=use_scraperapi,
                    timeout=120
                )
                pdf_data.raise_for_status()
            except Exception as e:
                print(f"[ERROR] Failed to download PDF {file_name}: {e}")
                continue
            
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
