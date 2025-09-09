from playwright.sync_api import sync_playwright
import datetime
import os
import json
from urllib.parse import urljoin
import requests
import pytesseract
from PIL import Image
import io # Used to handle image data in memory

# --- Configuration ---
LANDING_PAGE_URL = "https://www.atbltd.com/Docs/current_market_report"
BASE_URL = "https://www.atbltd.com/Docs/"
OUTPUT_DIR = "source_reports/mombasa_atbltd_reports"

def process_atbltd_report():
    """
    End-to-end script that navigates the ATB Ltd. site, downloads the two report images,
    parses them with OCR, and saves the combined text.
    """
    print(f"--- Starting Advanced Scraper for {LANDING_PAGE_URL} ---")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
    }
    
    full_report_text = ""

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        try:
            print("Navigating to page and waiting for it to settle...")
            page.goto(LANDING_PAGE_URL, wait_until="networkidle", timeout=60000)

            # --- PAGE 1 PROCESSING ---
            print("\n[PART 1/3] Processing Page 1...")
            page.wait_for_selector("img", timeout=30000)
            image_tag_1 = page.locator("img").first
            relative_url_1 = image_tag_1.get_attribute('src')
            full_image_url_1 = urljoin(BASE_URL, relative_url_1)
            
            print(f"Found Page 1 image link: {full_image_url_1}")
            print("Downloading and parsing Page 1 image with OCR...")
            response_1 = requests.get(full_image_url_1, headers=headers)
            response_1.raise_for_status()
            
            page1_text = pytesseract.image_to_string(Image.open(io.BytesIO(response_1.content)))
            full_report_text += page1_text + "\n\n--- End of Page 1 ---\n\n"
            print("Successfully parsed Page 1.")

            # --- PAGE 2 PROCESSING ---
            print("\n[PART 2/3] Processing Page 2...")
            next_button_selector = "input[value='Next']"
            next_button = page.locator(next_button_selector)

            if next_button.is_visible():
                print("Found 'Next' button. Clicking it...")
                next_button.click()
                page.wait_for_timeout(5000) # Wait for the new image to load

                image_tag_2 = page.locator("img").first
                relative_url_2 = image_tag_2.get_attribute('src')
                
                if relative_url_2 != relative_url_1:
                    full_image_url_2 = urljoin(BASE_URL, relative_url_2)
                    print(f"Found Page 2 image link: {full_image_url_2}")
                    print("Downloading and parsing Page 2 image with OCR...")
                    response_2 = requests.get(full_image_url_2, headers=headers)
                    response_2.raise_for_status()

                    page2_text = pytesseract.image_to_string(Image.open(io.BytesIO(response_2.content)))
                    full_report_text += page2_text + "\n\n--- End of Page 2 ---\n\n"
                    print("Successfully parsed Page 2.")
                else:
                    print("Page 2 image is the same as Page 1. Assuming end of report.")
            else:
                print("Could not find a 'Next' button. Report appears to be only one page.")

        except Exception as e:
            print(f"!!! An error occurred during scraping: {e}")
            page.screenshot(path='error_screenshot_atbltd.png')
            print("Saved an error screenshot to error_screenshot_atbltd.png")
        finally:
            browser.close()
            print("Browser closed.")

    # --- PART 3: SAVING THE FINAL JSON ---
    if full_report_text:
        print("\n[PART 3/3] Saving the combined extracted text...")
        output_data = {
            "report_title": f"ATB Ltd Market Report - {datetime.datetime.now().strftime('%Y-%m-%d')}",
            "raw_text": full_report_text
        }
        
        date_str = datetime.datetime.now().strftime('%Y-%m-%d')
        output_filename = f"ATBLtd_MarketReport_{date_str}.json"
        output_path = os.path.join(OUTPUT_DIR, output_filename)

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=4, ensure_ascii=False)
        
        print(f"\n--- PROCESS COMPLETE ---")
        print(f"Successfully saved combined data to: {output_path}")
    else:
        print("\n--- PROCESS COMPLETE ---")
        print("No text was extracted from the report images.")

if __name__ == "__main__":
    process_atbltd_report()
