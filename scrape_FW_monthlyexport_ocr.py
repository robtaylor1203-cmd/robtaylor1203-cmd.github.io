import json
import datetime
import re
import pytesseract
from PIL import Image
from playwright.sync_api import sync_playwright
from pathlib import Path
import io
import cv2
import numpy as np

# --- Standardized Configuration (Adapted for Proven Script) ---
REPO_ROOT = Path(__file__).resolve().parent
LOCATION = "colombo"
OUTPUT_DIR = REPO_ROOT / "source_reports" / LOCATION
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

TARGET_URL = "https://web.forbestea.com/statistics/sri-lankan-statistics/64-sri-lanka-tea-exports/1302-sri-lanka-tea-exports"

# Temp directory (used if fallback needed)
TEMP_IMG_DIR = REPO_ROOT / "temp_downloads"
TEMP_IMG_DIR.mkdir(parents=True, exist_ok=True)
# --------------------------------------------------------------

def preprocess_image_for_ocr(image_bytes):
    """Applies OpenCV preprocessing to enhance OCR accuracy."""
    try:
        # Convert bytes to numpy array
        nparr = np.frombuffer(image_bytes, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

        # 1. Convert to Grayscale
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

        # 2. Thresholding (Binarization) - Otsu's method for sharp black/white
        _, binary = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
        
        # Convert back to PIL Image format for Pytesseract
        processed_image = Image.fromarray(binary)
        return processed_image

    except Exception as e:
        print(f"Warning: Error during image preprocessing: {e}. Falling back to original.")
        return Image.open(io.BytesIO(image_bytes))


def parse_exports_from_ocr_definitive(ocr_text):
    """
    Parses raw OCR text using a robust counting method. (Restored Logic)
    """
    lines = ocr_text.strip().split('\n')
    
    # --- Data structures (Restored) ---
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
    
    # --- Counting Logic (Restored) ---
    seen_counts = {desc: 0 for desc in row_keywords.values()}

    for line in lines:
        line = line.strip()
        if not line:
            continue

        for keyword, clean_description in row_keywords.items():
            if re.search(r'\b' + re.escape(keyword) + r'\b', line, re.IGNORECASE):
                seen_counts[clean_description] += 1
                
                table_index = seen_counts[clean_description] - 1
                if table_index < len(table_order):
                    table_name = table_order[table_index]
                    
                    # Extract numbers and build the data row
                    numbers = re.findall(r'[\d,]+', line)
                    if numbers:
                        values = [clean_description] + numbers
                        current_headers = headers.get(table_name, [])
                        # Handle potential mismatch in column counts
                        if len(values) > len(current_headers):
                             values = values[:len(current_headers)]
                        all_tables_data[table_name].append(dict(zip(current_headers, values)))
                
                break # Break after finding the first keyword on a line
                
    return all_tables_data

def scrape_forbes_exports_ocr():
    print(f"--- Starting Definitive OCR Scraper for Forbes Tea Exports (Enhanced) ---")
    
    # (FIX): Determine the report period using the current system date
    # Due to persistent OCR errors (e.g., 1881), we rely on the current system date as requested.
    report_period = datetime.date.today().strftime("%Y_%m")
    print(f"Using Current System Date for Report Period: {report_period}")
    
    with sync_playwright() as p:
        # Using Chromium with High DPI for better screenshot quality
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(device_scale_factor=2) 
        
        try:
            print(f"[1/5] Navigating to page: {TARGET_URL}")
            page.goto(TARGET_URL, wait_until="networkidle", timeout=90000)

            print("[2/5] Taking full-page screenshot (High DPI)...")
            # Take screenshot bytes in memory
            screenshot_bytes = page.screenshot(full_page=True)
            
            print("[3/5] Preprocessing image (OpenCV)...")
            processed_image = preprocess_image_for_ocr(screenshot_bytes)

            print("[4/5] Performing OCR on processed image and parsing text...")
            # Use specific Tesseract config for better results on tables
            custom_config = r'--oem 3 --psm 6 -l eng'
            ocr_text = pytesseract.image_to_string(processed_image, config=custom_config)
            
            structured_data = parse_exports_from_ocr_definitive(ocr_text)

            # Check if any data was actually parsed into the tables
            if not any(structured_data.values()):
                 raise Exception("Parsing logic failed to find any data rows in the tables.")

            print(f"Successfully parsed data.")

            print("[5/5] Saving extracted data to JSON file...")
            output_data = {
                "report_title": "Forbes Tea - Sri Lanka Tea Exports",
                "period": report_period,
                "source_url": TARGET_URL,
                "retrieval_date": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "export_data_by_table": structured_data
            }

            # --- Standardized Saving Mechanism ---
            # We use the YYYY_MM format
            file_prefix = "FW_monthly_export_ocr_parsed"
            output_filename = f"{file_prefix}_{report_period}.json"
            output_path = OUTPUT_DIR / output_filename

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
