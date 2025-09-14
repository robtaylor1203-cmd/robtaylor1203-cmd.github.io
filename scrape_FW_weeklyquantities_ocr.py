import json
import datetime
import re
import pytesseract
from PIL import Image
from playwright.sync_api import sync_playwright
from pathlib import Path
# Import the updated utility
from pipeline_utils import generate_manifest

# --- Standardized Configuration ---
REPO_ROOT = Path(__file__).resolve().parent
LOCATION = "colombo"
OUTPUT_DIR = REPO_ROOT / "source_reports" / LOCATION
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# URL from proven script
TARGET_URL = "https://web.forbestea.com/statistics/sri-lankan-statistics/90-weekly-tea-auction-quantities-averages/1299-sri-lanka-weekly-tea-auction-quantities-averages"

# Temp directory for screenshot
TEMP_IMG_DIR = REPO_ROOT / "temp_downloads"
TEMP_IMG_DIR.mkdir(parents=True, exist_ok=True)
# --------------------------------------------------------------

def clean_ocr_value(value_str):
    """
    Cleans common OCR errors from a string that should be a number. (Restored Logic)
    """
    if isinstance(value_str, str):
        return re.sub(r'[^\d,.]', '', value_str)
    return value_str

def parse_quantities_from_ocr_final(ocr_text):
    """
    Parses the raw OCR text. (Restored Logic)
    """
    lines = ocr_text.strip().split('\n')
    structured_data = []
    headers = [
        'DATE', 'QTY_HIGH', 'QTY_MEDIUM', 'QTY_LOW', 'QTY_TOTAL', 
        'AVG_HIGH', 'AVG_MEDIUM', 'AVG_LOW', 'AVG_TOTAL'
    ]
    # Regex pattern restored from proven script
    row_pattern = re.compile(r'^\s*([\d]{1,2}-[A-Za-z]{3}[-.]?[\d]{4})\s+(.*)')

    for line in lines:
        match = row_pattern.search(line)
        if match:
            date_str, rest_of_line = match.groups()
            
            # --- FINAL POLISH: Standardize the date format (Restored) ---
            try:
                cleaned_date_str = date_str.replace('.', '-')
                     
                # Handle potential long month names if short names fail
                try:
                    date_obj = datetime.datetime.strptime(cleaned_date_str, "%d-%b-%Y")
                except ValueError:
                     date_obj = datetime.datetime.strptime(cleaned_date_str, "%d-%B-%Y")

                # Convert to the universal YYYY-MM-DD format
                iso_date = date_obj.strftime("%Y-%m-%d")
            except ValueError:
                # If date parsing fails, keep the original messy string
                iso_date = date_str

            number_values = rest_of_line.split()
            
            if len(number_values) >= 8:
                all_values = [iso_date] + number_values[:8]
                cleaned_values = [clean_ocr_value(v) for v in all_values]
                structured_data.append(dict(zip(headers, cleaned_values)))
                
    return structured_data

def scrape_forbes_quantities_ocr():
    """
    Automates scraping the long quantities table. (Restored Logic)
    """
    print(f"--- Starting Definitive OCR Scraper for Forbes Quantities ---")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        try:
            print(f"[1/4] Navigating to page: {TARGET_URL}")
            page.goto(TARGET_URL, wait_until="networkidle", timeout=60000)

            print("[2/4] Taking full-page screenshot...")
            screenshot_path = TEMP_IMG_DIR / "temp_quantities_screenshot.png"
            # Full page screenshot logic restored
            page.screenshot(path=screenshot_path, full_page=True)
            print(f"Full-page screenshot saved to {screenshot_path}")

            print("[3/4] Performing OCR and parsing with definitive logic...")
            ocr_text = pytesseract.image_to_string(Image.open(screenshot_path))
            
            if not ocr_text.strip():
                raise Exception("OCR failed to extract any text from the screenshot.")

            structured_data = parse_quantities_from_ocr_final(ocr_text)

            if not structured_data:
                 raise Exception("OCR text was extracted, but the definitive parser failed to find any data rows.")

            print(f"Successfully parsed {len(structured_data)} rows of data.")

            print("[4/4] Saving extracted data to JSON file...")
            output_data = {
                "report_title": "Forbes Tea - Sri Lanka Weekly Tea Auction Quantities/Averages",
                "source_url": TARGET_URL,
                "retrieval_date": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "auction_data": structured_data
            }

            # --- Standardized Saving & Auto-Manifest Mechanism ---
            # Since this covers many dates, we use the scrape date (YYYY_MM_DD) for the filename
            # And we use the current YYYY_MM for the manifest period.
            date_str_file = datetime.date.today().strftime('%Y_%m_%d')
            date_str_period = datetime.date.today().strftime('%Y_%m')
            
            file_prefix = "FW_weekly_quantities_ocr_parsed"
            
            output_filename = f"{file_prefix}_{date_str_file}.json"
            output_path = OUTPUT_DIR / output_filename

            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=4, ensure_ascii=False)

            print(f"\n--- PROCESS COMPLETE ---")
            print(f"Successfully saved parsed data to: {output_path}")
            
            # Generate the manifest automatically (Colombo uses LKR)
            # Note: We pass REPO_ROOT and use the period format YYYY_MM
            generate_manifest(REPO_ROOT, LOCATION, date_str_period, currency="LKR", report_type="Monthly/Weekly Statistics")

        except Exception as e:
            print(f"!!! An unexpected error occurred: {e}")
        finally:
            browser.close()

if __name__ == "__main__":
    scrape_forbes_quantities_ocr()
