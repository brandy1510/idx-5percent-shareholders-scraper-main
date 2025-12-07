import io
from datetime import datetime, timedelta
import cloudscraper

BASE_URL = "https://www.idx.co.id/primary/ListedCompany/GetAnnouncement"

def fetch_idx_pdf(exact_date=None):
    """
    Fetch IDX announcements filtered by 'Pemegang Saham di atas 5%' 
    and return the attachment content as BytesIO.

    Parameters
    ----------
    exact_date : str | None
        If None -> fetch latest with _lamp.
        If given -> fetch only _lamp file that matches the exact date (YYYYMMDD).

    Returns
    -------
    dict
        {
            'title', 'announcementDate', 'attachmentUrl',
            'fileName', 'pdf_content' (BytesIO)
        }

    Raises
    ------
    ValueError
        If no data found or no '_lamp' file found.
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
        dt_to = dt_from + timedelta(days=7)  # add 7 days
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

    # === Fetch data with cloudscraper ===
    # Use a specific browser signature to bypass WAF blocking in Datacenters (GCF)
    scraper = cloudscraper.create_scraper(
        browser={
            'browser': 'chrome',
            'platform': 'windows',
            'desktop': True
        }
    )
    # Add standard headers to mimic a real user usage
    scraper.headers.update({
        "Referer": "https://www.idx.co.id/primary/ListedCompany/Index",
        "Origin": "https://www.idx.co.id",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "X-Requested-With": "XMLHttpRequest"
    })
    
    print(f"[INFO] Fetching from: {BASE_URL} with params: {params}")
    response = scraper.get(BASE_URL, params=params)
    response.raise_for_status()

    data = response.json().get("Replies", [])
    if not data:
        raise ValueError("No announcements found for the given parameters")

    # Sort announcements by date descending (latest first)
    data.sort(key=lambda x: x["pengumuman"].get(
        "TglPengumuman") or "", reverse=True)

    # === Find _lamp attachment ===
    for item in data:
        pengumuman = item["pengumuman"]
        attachments = item.get("attachments", [])
        tgl_pengumuman = pengumuman.get("TglPengumuman", "")

        for attachment in attachments:
            file_name = attachment.get("OriginalFilename", "")
            if "_lamp" not in file_name.lower():
                continue

            # --- exact mode must match date exactly ---
            if exact_date:
                date_str = datetime.strptime(
                    tgl_pengumuman[:10], "%Y-%m-%d").strftime("%Y%m%d")
                if date_str != exact_date:
                    continue
            else:
                date_str = datetime.strptime(
                    tgl_pengumuman[:10], "%Y-%m-%d").strftime("%Y%m%d")

            # --- Download logic (In-Memory) ---
            print(f"[INFO] Downloading {file_name} ...")
            pdf_url = attachment["FullSavePath"]
            pdf_data = scraper.get(pdf_url)
            pdf_data.raise_for_status()
            
            # Create BytesIO object
            pdf_buffer = io.BytesIO(pdf_data.content)
            print(f"[SUCCESS] Downloaded to memory.")

            return {
                "title": pengumuman.get("JudulPengumuman"),
                "announcementDate": tgl_pengumuman,
                "attachmentUrl": attachment.get("FullSavePath"),
                "fileName": file_name,
                "pdf_content": pdf_buffer
            }

    raise ValueError("No '_lamp' attachment found for the given parameters")
