# IDX 5% Shareholders Scraper

This project scrapes "5% Shareholder Announcements" from the Indonesia Stock Exchange (IDX), parses the PDF attachments, and uploads the data to Google Cloud Storage (GCS) in CSV format. It also fetches the full Stock List as NDJSON.

## Setup

1.  **Install Dependencies**:
    ```bash
    pip install -r requirements.txt
    ```
2.  **Environment Variables**:
    Create a `.env` file in the root directory:
    ```ini
    GOOGLE_CLOUD_PROJECT=your-project-id
    BUCKET_NAME=your-gcs-bucket-name
    SCRAPERAPI_KEY=your-scraperapi-key
    # Optional
    PROXY_URL=http://user:pass@host:port
    ```

## Local Development & Testing (`tests/test_runner.py`)

The `tests/test_runner.py` script is the primary tool for executing the scraper locally for development, debugging, and backfilling.

### **Important: ScraperAPI Cost Control**
By default, **ALL** test modes run using your **local network** (or configured `PROXY_URL`) to save costs. 
To use ScraperAPI (and consume credits), you must explicitly add the `--use-api` flag.

### Usage Examples

#### 1. Backfill Data (Historical Run)
Run a multi-threaded backfill for a date range.
```bash
# Default: Local network, download PDFs + fetch Stock List
python tests/test_runner.py backfill --start_date 20250101 --end_date 20250131 --threads 5

# Use ScraperAPI for reliability (Uses Credits!)
python tests/test_runner.py backfill --start_date 20250101 --end_date 20250131 --use-api
```

#### 2. Granular Steps (`--step`)
Control exactly what gets fetched.
```bash
# Only download and parse PDFs (Skip Stock List)
python tests/test_runner.py backfill --start_date 20250101 --end_date 20250105 --step pdf

# Only fetch the Stock List (Skip PDFs)
python tests/test_runner.py backfill --start_date 20250101 --end_date 20250105 --step stock
```

#### 3. Single Day Test (`fetch`)
Download PDFs for a specific single date to `src/downloads` for inspection.
```bash
python tests/test_runner.py fetch --date 20251214
```

#### 4. Parse Local PDF (`parse`)
Test the PDF parser on a file already in `src/downloads`.
```bash
python tests/test_runner.py parse --file src/downloads/20251214_filename.pdf
```

#### 5. Full End-to-End Test (`full`)
Simulates a single "Cloud Function" execution locally.
```bash
python tests/test_runner.py full
```

## Deployment

The project is designed to be deployed as a Google Cloud Function (2nd Gen).
- **Entry Point**: `idx_scraper_entry`
- **Runtime**: Python 3.10+
- **Trigger**: HTTP (e.g., triggered by Cloud Scheduler)

Ensure secrets (`SCRAPERAPI_KEY`) are mounted as environment variables in the Cloud Function configuration.