import os
import json
import datetime
import re
import pytesseract
from PIL import Image
from playwright.sync_api import sync_playwright

# --- Configuration ---
TARGET_URL = "https://web.forbestea.com/statistics/sri-lankan-statistics/65-sri-lanka-tea-production/1301-sri-lanka-tea-production"
FINAL_OUTPUT_DIR = "source_reports/forbes_tea_production"

def clean_ocr_value(value_str):
    """
    Cleans common OCR errors from a string that should be a number.
    Removes all characters that are not digits or commas.
    """
    if isinstance(value_str, str):
        return re.sub(r'[^\d,]', '', value_str)
    return value_str

def parse_text_from_ocr_revised(ocr_text):
    """
    Parses the raw text from OCR using a more resilient, two-stage approach.
    It assumes data before "CUMULATIVE" is monthly, and data after is cumulative.
    """
    lines = ocr_text.strip().split('\n')
    
    monthly_headers = ['DESCRIPTION', 'JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC', 'TOTAL']
    cumulative_headers = ['DESCRIPTION', 'JAN/FEB', 'JAN/MAR', 'JAN/APR', 'JAN/MAY', 'JAN/JUN', 'JAN/JUL', 'JAN/AUG', 'JAN/SEP', 'JAN/OCT', 'JAN/NOV', 'JAN/DEC']

    monthly_data = []
    cumulative_data = []
    
    in_cumulative_section = False
    row_keywords = ["High", "Medium", "Low", "Green", "TOTAL"]

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # Switch to the cumulative section when this keyword is found
        if "CUMULATIVE" in line:
            in_cumulative_section = True
            continue

        # Check if the line looks like a data row
        first_word = line.split()[0] if line.split() else ''
        if any(keyword in first_word for keyword in row_keywords):
            
            if "Green" in line and "Tea" in line:
                description = "Green Tea"
                raw_values = line.split()[2:]
            else:
                description = first_word
                raw_values = line.split()[1:]
            
            # Clean each value before creating the list
            cleaned_values = [description] + [clean_ocr_value(v) for v in raw_values]

            if not in_cumulative_section:
                monthly_data.append(dict(zip(monthly_headers, cleaned_values)))
            else:
                cumulative_data.append(dict(zip(cumulative_headers, cleaned_values)))
                
    return monthly_data, cumulative_data

def scrape_forbes_production_ocr():
    """
    Automates the entire process: fetches the page with Playwright,
    screenshots the content area, and uses OCR to extract data.
    """
    print(f"--- Starting OCR Scraper for Forbes Tea Production ---")
    os.makedirs(FINAL_OUTPUT_DIR, exist_ok=True)
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        try:
            print(f"[1/4] Navigating to page: {TARGET_URL}")
            page.goto(TARGET_URL, wait_until="networkidle", timeout=60000)

            print("[2/4] Taking screenshot of the data area...")
            content_area = page.locator('div.item-page')
            content_area.wait_for(state="visible", timeout=30000)
            
            screenshot_path = os.path.join(FINAL_OUTPUT_DIR, "temp_production_screenshot.png")
            content_area.screenshot(path=screenshot_path)
            print(f"Screenshot of content area saved to {screenshot_path}")

            print("[3/4] Performing OCR on screenshot and parsing text...")
            ocr_text = pytesseract.image_to_string(Image.open(screenshot_path))
            
            if not ocr_text.strip():
                raise Exception("OCR failed to extract any text from the screenshot.")

            monthly_data, cumulative_data = parse_text_from_ocr_revised(ocr_text)

            if not monthly_data or not cumulative_data:
                 raise Exception("Parsing logic failed to find data for one or both tables.")

            print(f"Successfully parsed {len(monthly_data)} rows of monthly data.")
            print(f"Successfully parsed {len(cumulative_data)} rows of cumulative data.")

            print("[4/4] Saving extracted data to JSON file...")
            output_data = {
                "report_title": "Forbes Tea - Sri Lanka Tea Production (In Kilos)",
                "source_url": TARGET_URL,
                "retrieval_date": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "monthly_production": monthly_data,
                "cumulative_production": cumulative_data
            }

            date_str = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d')
            output_filename = f"ForbesTea_SL-Production-OCR_{date_str}.json"
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
    scrape_forbes_production_ocr()
