import pdfplumber
import sys

PDF_PATH = "downloads/20251128_Semua Emiten Saham_Pengumuman Bursa_31988315_lamp1.pdf"

import pdfplumber
import sys

PDF_PATH = "downloads/20251128_Semua Emiten Saham_Pengumuman Bursa_31988315_lamp1.pdf"

def debug_pdf():
    print(f"Inspecting: {PDF_PATH}")
    try:
        with pdfplumber.open(PDF_PATH) as pdf:
            print(f"Total Pages: {len(pdf.pages)}")
            
            # Scan all pages
            print("\n--- Scanning All Pages ---")
            for i, page in enumerate(pdf.pages):
                table = page.extract_table()
                if not table:
                    print(f"Page {i}: No table found.")
                    continue
                
                first_row = table[0]
                # Check if it looks like a header
                if "Kode Efek" in str(first_row):
                    print(f"Page {i}: Header FOUND. Rows: {len(table)}")
                else:
                    print(f"Page {i}: Header MISSING? First row: {first_row[:3]}...")
                    # If header is missing, current logic (table[2:]) will skip data!

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    debug_pdf()
