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

-   `BUCKET_NAME`: Target GCS bucket (Default: `data-dev-01`)
-   `GOOGLE_CLOUD_PROJECT`: Google Cloud Project ID (Default: `altrabyte-dev-data-01`)

## Deployment to Google Cloud Functions

Deploy using the following settings:
-   **Runtime**: Python 3.13 (or compatible)
-   **Entry Point**: `idx_scraper_entry`
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
```bash
python test_runner.py full
```

### 4. Backfill / Historical Re-Scrape
Runs a multi-threaded backfill for a date range.
```bash
# Max 3 threads
python test_runner.py backfill --start_date 20250101 --end_date 20250131
```

## Credits

Credits to Hengky Adinata and Remora Trader.
Refactored for Cloud Native / ETL by Altrabyte.
