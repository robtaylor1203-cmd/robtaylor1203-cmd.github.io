import os
import json
import datetime
import re
import pytesseract
from PIL import Image
from playwright.sync_api import sync_playwright

# --- Configuration ---
TARGET_URL = "https://web.forbestea.com/statistics/sri-lankan-statistics/64-sri-lanka-tea-exports/1302-sri-lanka-tea"
FINAL_OUTPUT_DIR = "source_reports/forbes_tea_exports"

def parse_exports_from_ocr_definitive(ocr_text):
    """
    Parses raw OCR text using a robust counting method. It assumes each
    keyword appears 4 times, once for each table in order.
    """
    lines = ocr_text.strip().split('\n')
    
    # --- Data structures to hold the final parsed data ---
    all_tables_data = {
        "MONTHLY_QUANTITY": [],
        "MONTHLY_VALUE": [],
        "CUMULATIVE_QUANTITY": [],
        "CUMULATIVE_VALUE": []
    }
    table_order = ["MONTHLY_QUANTITY", "MONTHLY_VALUE", "CUMULATIVE_QUANTITY", "CUMULATIVE_VALUE"]
    
    headers = {
        "MONTHLY_QUANTITY": ['DESCRIPTION', 'JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'],
        "MONTHLY_VALUE": ['DESCRIPTION', 'JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'],
        "CUMULATIVE_QUANTITY": ['DESCRIPTION', 'JAN/FEB', 'JAN/MAR', 'JAN/APR', 'JAN/MAY', 'JAN/JUN', 'JAN/JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'],
        "CUMULATIVE_VALUE": ['DESCRIPTION', 'JAN/FEB', 'JAN/MAR', 'JAN/APR', 'JAN/MAY', 'JAN/JUN', 'JAN/JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
    }
    
    row_keywords = {
        "Bulk": "Tea In Bulk",
        "Packets": "Tea In Packets",
        "Bags": "Tea In Bags",
        "Instant": "Instant Tea",
        "Green": "Green Tea",
        "TOTAL": "TOTAL"
    }
    
    # --- Counting Logic ---
    # This dictionary will track how many times we've seen each keyword
    seen_counts = {desc: 0 for desc in row_keywords.values()}

    for line in lines:
        line = line.strip()
        if not line:
            continue

        for keyword, clean_description in row_keywords.items():
            if re.search(r'\b' + re.escape(keyword) + r'\b', line, re.IGNORECASE):
                # Increment the count for this description
                seen_counts[clean_description] += 1
                
                # Determine which table this row belongs to based on the count
                table_index = seen_counts[clean_description] - 1
                if table_index < len(table_order):
                    table_name = table_order[table_index]
                    
                    # Extract numbers and build the data row
                    numbers = re.findall(r'[\d,]+', line)
                    if numbers:
                        values = [clean_description] + numbers
                        current_headers = headers.get(table_name, [])
                        all_tables_data[table_name].append(dict(zip(current_headers, values)))
                
                # Break after finding the first keyword on a line to avoid double counting
                break
                
    return all_tables_data

def scrape_forbes_exports_ocr():
    print(f"--- Starting Definitive OCR Scraper for Forbes Tea Exports ---")
    os.makedirs(FINAL_OUTPUT_DIR, exist_ok=True)
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        try:
            print(f"[1/4] Navigating to page: {TARGET_URL}")
            page.goto(TARGET_URL, wait_until="networkidle", timeout=60000)

            print("[2/4] Taking full-page screenshot...")
            screenshot_path = os.path.join(FINAL_OUTPUT_DIR, "temp_exports_screenshot.png")
            page.screenshot(path=screenshot_path, full_page=True)
            print(f"Full-page screenshot saved to {screenshot_path}")

            print("[3/4] Performing OCR on screenshot and parsing text...")
            ocr_text = pytesseract.image_to_string(Image.open(screenshot_path))
            
            structured_data = parse_exports_from_ocr_definitive(ocr_text)

            if not structured_data:
                 raise Exception("Parsing logic failed to find any data tables.")

            print(f"Successfully parsed data for {len(structured_data)} tables.")

            print("[4/4] Saving extracted data to JSON file...")
            output_data = {
                "report_title": "Forbes Tea - Sri Lanka Tea Exports",
                "source_url": TARGET_URL,
                "retrieval_date": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "export_data_by_table": structured_data
            }

            date_str = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d')
            output_filename = f"ForbesTea_Exports_Definitive_{date_str}.json"
            output_path = os.path.join(FINAL_OUTPUT_DIR, output_filename)

            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=4, ensure_ascii=False)

            print(f"\n--- PROCESS COMPLETE ---")
            print(f"Successfully saved parsed data to: {output_path}")

        except Exception as e:
            print(f"!!! An unexpected error occurred: {e}")
        finally:
            browser.close()

if __name__ == "__main__":
    scrape_forbes_exports_ocr()
