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
                # Clean Header Cell:
                # Issue: Sometimes previous content (like page numbers or previous row) 
                # gets merged into the header (e.g., "1766\nNo").
                # Solution: Split by newline and take the last non-empty part?
                # Or specifically look for known header keywords?
                # "No", "Nama Emiten" are usually at the bottom.
                
                raw_h = str(header[i] or "").strip()
                
                # Simple heuristic: Take the last line if multiline
                if "\n" in raw_h:
                    parts = raw_h.split("\n")
                    h = parts[-1].strip()
                    # Fallback: if last part is empty or too short?
                    if not h and len(parts) > 1:
                        h = parts[-2].strip()
                else:
                    h = raw_h

                # Check for Date Pattern (DD-MMM-YYYY)
                # Note: The garbled date header usually contains the date in the string somewhere
                # "Kepe9m... 15-DEC-2025 ..." -> regex finds 15-DEC-2025
                m = re.search(r"(\d{1,2}-[A-Z]{3}-\d{4})", h.upper())
                
                # If regex failed on clean 'h', try 'raw_h' just in case the date was in top line?
                # Unlikely, usually date is in the main header text.
                if not m:
                     m = re.search(r"(\d{1,2}-[A-Z]{3}-\d{4})", raw_h.upper())

                if m:
                    # Found a date-based column triplet (Kepemilikan Per X)
                    date_str = m.group(1)
                    final_header.extend([
                        f"Kepemilikan Per {date_str} - Jumlah Saham",
                        f"Kepemilikan Per {date_str} - Saham Gabungan Per Investor",
                        f"Kepemilikan Per {date_str} - Persentase Kepemilikan Per Investor (%)"
                    ])
                    
                    # Logic to determine how many input columns to consume:
                    # If this was a merged cell, we consume 1.
                    # If this was standard (Text, None, None), we consume 3.
                    skip = 1
                    # Check next cell
                    if i + 1 < len(header) and not (header[i+1] or "").strip():
                        skip += 1
                        # Check next-next cell
                        if i + 2 < len(header) and not (header[i+2] or "").strip():
                            skip += 1
                    
                    i += skip
                else:
                    if h:
                        final_header.append(h)
                    else:
                        # Empty header cell not associated with date expansion?
                        # Usually we skip, or append "Unnamed"?
                        # Based on original logic: "if h: final_header.append(h)"
                        # So we skip empty non-date headers. (Assuming they are merged parts of previous?)
                        pass 
                    i += 1

            all_rows.extend(data)

    # Create DataFrame
    try:
        df = pd.DataFrame(all_rows, columns=final_header)
    except ValueError as e:
        if all_rows:
            print(f"[ERROR] DataFrame Creation Failed. Header Len: {len(final_header)}, First Row Len: {len(all_rows[0])}")
            # print(f"Header: {final_header}")
            # print(f"First Row: {all_rows[0]}")
        raise e
    
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
