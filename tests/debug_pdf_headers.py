import pdfplumber
import sys

def inspect_pdf(pdf_path):
    print(f"--- Inspecting: {pdf_path} ---")
    with pdfplumber.open(pdf_path) as pdf:
        # Check all pages
        for i, page in enumerate(pdf.pages): 
            try:
                table = page.extract_table()
                if table:
                    # Check first few rows for artifacts
                    for row_idx, row in enumerate(table[:3]):
                        row_str = str(row)
                        if "1766" in row_str or "WIDODO MAKMUR" in row_str:
                            print(f"\n[Page {i+1} Row {row_idx}] Found artifact:")
                            print(row)
            except Exception as e:
                pass

if __name__ == "__main__":
    inspect_pdf("src/downloads/20251217_Semua Emiten Saham_Pengumuman Bursa_32013675_lamp1.pdf")
