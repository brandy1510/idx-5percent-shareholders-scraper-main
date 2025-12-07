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
    # Replace empty strings and None with np.nan for correct filling
    df.replace(["", None, "None"], np.nan, inplace=True)
    
    # Columns to forward fill
    # "No" identifies the Shareholder group. "Nama Emiten" identifies the Company.
    # "Nama Pemegang Saham" and "Kebangsaan" often missing in sub-rows.
    # "Kode Efek" is reliable (User input), but other columns rely on "No" grouping
    # Since "No" is only present on the first row of a group, we ffill it first.
    cols_to_fill = ["No", "Nama Emiten", "Nama Pemegang Saham", "Kebangsaan"]
    
    # We forward fill. Grouping by 'Kode Efek' is safer to ensure we don't bleed across issuers,
    # though strictly speaking the list is sequential.
    # User suggestion: "column kode efek -> will always availble... use this as a group by key"
    
    # However, 'No' resets for each Issuer? No, 'No' usually increments strictly 1..N in the doc?
    # Inspecting CSV: No 1 (AADI), No 2 (AADI), No 3 (AADI), No 4 (ABBA).
    # So 'No' increments across issuers in the same file? Or resets?
    # Actually, usually it resets per Issuer in some reports, or is global?
    # In the sample: 1, 2, 3 for AADI. 4 for ABBA. It seems continuous!
    # So Global ffill is actually safe for 'No'.
    
    # Let's simple ffill "No" first.
    if "No" in df.columns:
        df["No"] = df["No"].ffill()
    
    # Now for other columns, we can ffill grouped by 'No' to be very safe, 
    # OR just global ffill assuming standard PDF table structure.
    # Safe bet: Global ffill for Emiten/Name is fine because 'Kode Efek' changes when these change.
    
    for col in cols_to_fill:
        if col in df.columns and col != "No":
             df[col] = df[col].ffill()
             
    # Ensure percentages are numeric
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

    # Compare percentages
    affected_emiten = df.loc[df[prev_col] !=
                             df[curr_col], "Kode Efek"].unique()

    filtered_df = df[df["Kode Efek"].isin(affected_emiten)]

    print(f"[INFO] Parsed {len(df)} total rows, found {len(filtered_df)} affected rows.")

    return df, filtered_df
