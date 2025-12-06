import os
import numpy as np
import pdfplumber
import pandas as pd
import re

RESULT_DIR = "results"
os.makedirs(RESULT_DIR, exist_ok=True)


def parse_shareholder_pdf(pdf_file, output_filename: str, log_callback=None) -> pd.DataFrame:
    """
    Parse a shareholder ownership PDF from IDX (Pemegang Saham di atas 5%) and
    return a cleaned DataFrame containing relevant information.

    Args:
        pdf_file: File-like object (BytesIO) or path to PDF
        output_filename (str): Name of the output CSV file (including .csv extension)

    Returns:
        pd.DataFrame: Filtered DataFrame of affected emitens
    """

    csv_path = os.path.join(RESULT_DIR, output_filename)

    # --- Parse PDF ---
    all_rows = []

    with pdfplumber.open(pdf_file) as pdf:
        total_pages = len(pdf.pages)

        for idx, page in enumerate(pdf.pages[1:], start=2):  # skip first page
            if log_callback:
                log_callback(f"Processing page {idx-1} of {total_pages-1}...")
            print(f"Processing page {idx-1} of {total_pages-1}...")

            table = page.extract_table()
            if not table:
                print(f"No table found on page {idx}, skipping.")
                continue

            header = table[0] if len(table) > 0 else []
            data = table[2:] if len(table) > 2 else []

            final_header = []
            i = 0
            while i < len(header):
                h = (header[i] or "").strip()
                if "kepemilikan per" in h.lower():
                    # extract date like 29-OCT-2025
                    m = re.search(r"(\d{1,2}-[A-Z]{3}-\d{4})", h)
                    date_str = m.group(1) if m else ""
                    final_header.extend([
                        f"Kepemilikan Per {date_str} - Jumlah Saham",
                        f"Kepemilikan Per {date_str} - Saham Gabungan Per Investor",
                        f"Kepemilikan Per {date_str} - Persentase Kepemilikan Per Investor (%)"
                    ])
                    i += 3  # skip 3 columns
                else:
                    if h:
                        final_header.append(h)
                    i += 1

            all_rows.extend(data)

    df = pd.DataFrame(all_rows, columns=final_header)
    print(f"[INFO] Total rows extracted from PDF: {len(df)}")

    # Drop unnecessary columns
    for col in ["Alamat", "Alamat (Lanjutan)", "Domisili"]:
        if col in df.columns:
            df = df.drop(columns=[col])

    # Clean string data
    for col in df.columns:
        df[col] = (
            df[col]
            .astype(str)
            .str.strip()
            .replace(["None", "none", ""], np.nan)
            .str.replace(",", "", regex=False)
            .str.replace("%", "", regex=False)
        )

    # Convert numeric columns by index
    if not df.empty:
        # last 7
        numeric_indices = list(range(len(df.columns) - 7, len(df.columns)))
        for idx in numeric_indices:
            col = df.columns[idx]
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    perc_cols = [
        c for c in df.columns if "Persentase Kepemilikan Per Investor" in c]

    # Assume last two percentage columns are the "before" and "current" dates
    prev_col, curr_col = perc_cols[-2], perc_cols[-1]

    # Convert to numeric (should already be numeric, but just in case)
    df[prev_col] = pd.to_numeric(df[prev_col], errors="coerce").fillna(0)
    df[curr_col] = pd.to_numeric(df[curr_col], errors="coerce").fillna(0)

    # Save Full CSV (Raw Data)
    full_csv_path = csv_path.replace(".csv", "_full.csv")
    df.to_csv(full_csv_path, index=False)
    print(f"[SUCCESS] Saved FULL data to {full_csv_path} ({len(df)} rows)")

    # Compare percentages
    affected_emiten = df.loc[df[prev_col] !=
                             df[curr_col], "Kode Efek"].unique()

    filtered_df = df[df["Kode Efek"].isin(affected_emiten)]

    # Save CSV for next time
    filtered_df.to_csv(csv_path, index=False)
    print(f"[SUCCESS] Parsed and saved FILTERED CSV to {csv_path}")
    print(f"Done. Found {len(filtered_df)} affected rows.")

    return filtered_df
