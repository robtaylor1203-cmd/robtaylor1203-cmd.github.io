import fitz  # This is the PyMuPDF library
import json
import os
import datetime

# The location where our scraper saved the downloaded PDF
INPUT_DIR = "source_reports/kolkata_synopsis_reports"
    
def parse_synopsis_pdf():
    try:
        # This code finds the most recently downloaded PDF in the folder
        pdf_files = [f for f in os.listdir(INPUT_DIR) if f.lower().endswith('.pdf')]
        if not pdf_files:
            print(f"Error: No PDF files found in '{INPUT_DIR}'.")
            return
        
        latest_file = max([os.path.join(INPUT_DIR, f) for f in pdf_files], key=os.path.getctime)
        print(f"Found latest Synopsis PDF: {latest_file}")

    except FileNotFoundError:
        print(f"Error: The directory '{INPUT_DIR}' does not exist.")
        return

    # --- This is the core PyMuPDF logic ---
    print("Opening PDF and extracting text...")
    doc = fitz.open(latest_file)
    print(f"File has {doc.page_count} pages.")

    full_text = ""
    for page_num in range(doc.page_count):
        page = doc.load_page(page_num)
        full_text += page.get_text()
    
    doc.close()
    # --- End of core logic ---

    print("\n--- Successfully extracted all raw text from the PDF ---")
    print(full_text)
    print("----------------------------------------------------------")
    
    # --- NEXT STEPS ---
    print("\nOur next task will be to write Python code to search through this raw text,")
    print("find the specific data we want, and save it as a clean JSON file.")

if __name__ == "__main__":
    parse_synopsis_pdf()
