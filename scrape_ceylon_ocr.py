import os
import json
import datetime
import pytesseract
from PIL import Image
from playwright.sync_api import sync_playwright

# --- Configuration ---
TARGET_URL = "https://ceylonteabrokers.com/weekly-sales-averages/"
FINAL_OUTPUT_DIR = "source_reports/ceylon_tea_brokers_averages"

def scrape_weekly_sales_averages_ocr():
    print(f"--- Starting OCR Scraper for Weekly Averages ---")
    os.makedirs(FINAL_OUTPUT_DIR, exist_ok=True)
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        try:
            print(f"[1/3] Navigating to page: {TARGET_URL}")
            page.goto(TARGET_URL, wait_until="load", timeout=60000)

            print("Locating the data iframe...")
            # This is the CORRECT iframe locator that was discovered
            iframe_element = page.locator('iframe[src*="/widgets/sales/"]')
            iframe_element.scroll_into_view_if_needed()
            page.wait_for_timeout(3000) # Wait for content to render

            print("[2/3] Taking screenshot of the data table...")
            screenshot_path = os.path.join(FINAL_OUTPUT_DIR, "temp_table_screenshot.png")
            iframe_element.screenshot(path=screenshot_path)
            print(f"Screenshot saved to {screenshot_path}")

            print("Performing OCR on the screenshot...")
            # Use Pytesseract to read the text from the image
            ocr_text = pytesseract.image_to_string(Image.open(screenshot_path))

            if not ocr_text.strip():
                raise Exception("OCR failed to extract any text from the screenshot.")

            print("OCR extraction successful.")

            print("[3/3] Saving extracted OCR data to JSON file...")
            output_data = {
                "report_title": "Ceylon Tea Brokers - Weekly Sales Averages (OCR)",
                "source_url": TARGET_URL,
                "retrieval_date": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "raw_ocr_text": ocr_text
            }
            date_str = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d')
            output_filename = f"CeylonTeaBrokers_WeeklyAverages_OCR_{date_str}.json"
            output_path = os.path.join(FINAL_OUTPUT_DIR, output_filename)

            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=4, ensure_ascii=False)

            print(f"\n--- PROCESS COMPLETE ---")
            print(f"Successfully saved OCR data to: {output_path}")

        except Exception as e:
            print(f"!!! An unexpected error occurred: {e}")
        finally:
            browser.close()

if __name__ == "__main__":
    scrape_weekly_sales_averages_ocr()
