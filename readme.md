# IDX 5% Shareholder Scraper (GCF-Ready)

A Google Cloud Function (GCF) ETL pipeline that fetches "5% Shareholder" disclosures from the Indonesia Stock Exchange (IDX), parses the PDF, and uploads the structured data to Google Cloud Storage (GCS) with Hive partitioning.

## Features

-   **Serverless**: Designed for Google Cloud Functions (HTTP Trigger).
-   **Automated Scheduling**: Logic handles weekends (Saturday/Sunday -> fetches Friday's data) and Weekdays.
-   **Data Cleaning**:
    -   Automatically fills blank rows (grouping by Shareholder/Issuer).
    -   Drops empty and repeating header rows.
-   **GCS Integration**:
    -   Uploads to a Hive-partitioned path: `gs://<bucket>/<prefix>/dt=YYYY-MM-DD/file_full.csv`.
-   **Backfill Capability**: Built-in multi-threaded backfill tool for historical data.

## Project Structure

```
.
├── main.py          <-- GCF Entry point
├── requirements.txt <-- Dependencies
├── src/             <-- Modular Logic (Fetcher, Parser, Uploader)
│   ├── idx_fetcher.py
│   ├── pdf_parser.py
│   └── gcs_uploader.py
├── test_runner.py   <-- Local Testing & Backfill CLI
└── results/         <-- Local output (gitignored)
```

## Configuration

Set the following Environment Variables in GCF or your local `.env`:

-   `BUCKET_NAME`: Target GCS bucket (e.g., `my-data-bucket`)
-   `GOOGLE_CLOUD_PROJECT`: Google Cloud Project ID (e.g., `my-project-id`)
-   `GCS_BASE_PREFIX`: Path prefix in bucket (e.g., `folder/subfolder`)
-   `PROXY_URL` (Optional): HTTP/HTTPS proxy URL (e.g., `http://user:pass@host:port`) to bypass IP blocking.
-   `SCRAPERAPI_KEY` (Optional): API Key for ScraperAPI (replaces Proxy/TLS logic if set).

## Deployment to Google Cloud Functions

Deploy using the following settings:
-   **Runtime**: Python 3.13 (or compatible)
-   **Entry Point**: `idx_scraper_entry`
-   **Memory**: **1 GB** (Minimum) or **2 GB** (Recommended)
    -   *Why?* PDF parsing (`pdfplumber`) is memory-intensive for files with many pages. 512MB is insufficient.
-   **CPU**: **1 vCPU** (Sufficient)
-   **Timeout**: 300s (5 minutes)
-   **Source**: Upload this directory.

## Local Testing & Development

Use the provided `test_runner.py` for all testing scenarios.

### 1. Download PDF Only
Fetches the PDF for a specific date (YYYYMMDD) into `downloads/`.
```bash
python test_runner.py fetch --date 20251205
```

### 2. Parse PDF Only
Parses a local PDF and outputs CSV to `results/`.
```bash
python test_runner.py parse --file downloads/myfile.pdf
```

### 3. Full End-to-End Test
Runs the GCF logic locally (Fetch -> Parse -> Upload).

**Note**: By default, this saves to `results/` if no bucket is configured.
To test GCS upload locally, set the environment variables before running:
```bash
# Windows PowerShell
$env:BUCKET_NAME="my-data-bucket"
$env:GOOGLE_CLOUD_PROJECT="my-project-id"
python test_runner.py full

# Linux/Mac
export BUCKET_NAME="my-data-bucket"
export GOOGLE_CLOUD_PROJECT="my-project-id"
python test_runner.py full
```

**Alternative (.env)**
You can also create a `.env` file in the root directory (already gitignored):
```
BUCKET_NAME=my-data-bucket
GOOGLE_CLOUD_PROJECT=my-project-id
GCS_BASE_PREFIX=stock_market/data_kepentingan
```
The script will automatically load these if present.

### 4. Backfill / Historical Re-Scrape
Runs a multi-threaded backfill for a date range.
```bash
# Max 3 threads
python test_runner.py backfill --start_date 20250101 --end_date 20250131
```
**Tip**: Since this runs in memory, check your **Google Cloud Storage Bucket** (`stock_market/data_kepentingan/`) to watch the files arrive in real-time.

## Credits

Credits to Hengky Adinata and Remora Trader.
Refactored for Cloud Native / ETL by Altrabyte.