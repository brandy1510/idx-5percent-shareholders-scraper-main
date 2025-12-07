import os
import numpy as np
import pdfplumber
import pandas as pd
import re

RESULT_DIR = "results"
os.makedirs(RESULT_DIR, exist_ok=True)


def parse_shareholder_pdf(pdf_file, log_callback=None) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Parse a shareholder ownership PDF from IDX (Pemegang Saham di atas 5%) and
    return a cleaned DataFrame containing relevant information.

    Args:
        pdf_file: File-like object (BytesIO) or path to PDF

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]: (full_df, filtered_df)
    """

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

    # Create DataFrame
    df = pd.DataFrame(all_rows, columns=final_header)
    
    # --- Data Cleaning & Filling ---
    # 2. Drop Header rows (recurring on every page)
    # The first column is "No". If it contains "No", it's a header.
    if "No" in df.columns:
        df = df[df["No"] != "No"]
    
    # Columns to forward fill
    cols_to_fill = ["No", "Nama Emiten", "Nama Pemegang Saham", "Kebangsaan"]
    
    if "No" in df.columns:
        df["No"] = df["No"].ffill()
    
    for col in cols_to_fill:
        if col in df.columns and col != "No":
             df[col] = df[col].ffill()
             
    # Ensure percentages are numeric
    # prev_col = [c for c in headers if "Persentase" in c and "26-NOV" in c] # Dynamic? 
    # Actually the code below relies on exact column names found dynamically
    # But since we return the full DF, we don't strictly need the percentage comparison logic
    # UNLESS we used it to calculate "Perubahan" column? 
    # The "Perubahan" column is in the headers list? Yes (Step 354 shows "Perubahan")
    
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

    # Convert numeric columns by index
    if not df.empty:
        # last 7
        numeric_indices = list(range(len(df.columns) - 7, len(df.columns)))
    cols_to_drop = [c for c in df.columns if "Unnamed" in c]
    df.drop(columns=cols_to_drop, inplace=True, errors='ignore')

    # Identify percentage columns dynamically (dates change)
    pct_cols = [c for c in df.columns if "Persentase" in c]
    
    # ... [percentage calculations omitted if not strictly needed for dropna determination, 
    # but user wants to fix blank rows in output]
    
    print(f"[INFO] Parsed {len(df)} total rows.")
    
    # --- Data Cleaning & Filling (Moved to end) ---
    # Replace empty strings and None with np.nan
    df.replace(["", None, "None"], np.nan, inplace=True)
    
    # 1. Drop completely empty rows
    df.dropna(how="all", inplace=True)
    
    # 2. Drop Header rows (recurring on every page)
    # The first column is "No". If it contains "No", it's a header.
    if "No" in df.columns:
        df = df[df["No"] != "No"]
    
    # Columns to forward fill
    cols_to_fill = ["No", "Nama Emiten", "Nama Pemegang Saham", "Kebangsaan"]
    
    if "No" in df.columns:
        df["No"] = df["No"].ffill()
    
    for col in cols_to_fill:
        if col in df.columns and col != "No":
             df[col] = df[col].ffill()

    return df
