from playwright.sync_api import sync_playwright
import json
import datetime
import time
from bs4 import BeautifulSoup
from pathlib import Path
import re

# --- Standardized Configuration ---
REPO_ROOT = Path(__file__).resolve().parent
LOCATION = "kolkata"
OUTPUT_DIR = REPO_ROOT / "source_reports" / LOCATION
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

TARGET_URL = "https://jthomasindia.com/market_report.php"
# ----------------------------------

def scrape_full_market_report_stealth():
    print(f"Starting STEALTH Playwright scraper for {TARGET_URL} (Sequential Interaction)...")
    
    all_reports_data = {}
    sale_number = None 

    # Maximized timeouts and stabilization waits
    NAVIGATION_TIMEOUT = 180000 # 3 minutes
    WAIT_FOR_UPDATE_TIMEOUT = 180000 # 3 minutes
    STABILIZATION_PAUSE_LONG = 15 # 15s
    STABILIZATION_PAUSE_MEDIUM = 10 # 10s

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()

        try:
            # --- Initial Setup Phase: Determine Categories and Sale Number (Strict Sequence) ---
            print("--- Initial Setup Phase: Determining Categories and Sale Number ---")
            page = context.new_page()
            page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            page.goto(TARGET_URL, wait_until="networkidle", timeout=NAVIGATION_TIMEOUT)
            
            # 1. Select Centre
            page.select_option("#cbocenter", label="KOLKATA")
            print(f"Kolkata selected. Pausing for {STABILIZATION_PAUSE_LONG}s stabilization...")
            time.sleep(STABILIZATION_PAUSE_LONG) 
            
            # 2. Wait for Categories and identify them
            print(f"Waiting for Categories to load (up to {WAIT_FOR_UPDATE_TIMEOUT/1000}s)...")
            page.wait_for_function("document.querySelectorAll('#cbocat option').length > 1", timeout=WAIT_FOR_UPDATE_TIMEOUT)
            print("Categories loaded.")
            
            categories = page.locator("#cbocat option").all()
            categories_to_scrape = [(opt.get_attribute("value"), opt.inner_text()) for opt in categories if opt.get_attribute("value")]

            if not categories_to_scrape:
                raise ValueError("Could not find any categories to scrape.")

            # 3. Select the FIRST category to trigger Sale Number loading (CRITICAL FIX)
            first_category_value = categories_to_scrape[0][0]
            page.select_option("#cbocat", value=first_category_value)
            print(f"First category selected. Pausing for {STABILIZATION_PAUSE_MEDIUM}s stabilization...")
            time.sleep(STABILIZATION_PAUSE_MEDIUM)

            # 4. Wait for Sale Numbers
            print(f"Waiting for Sale Numbers to load (up to {WAIT_FOR_UPDATE_TIMEOUT/1000}s)...")
            page.wait_for_function("document.querySelectorAll('#cbosale option').length > 1", timeout=WAIT_FOR_UPDATE_TIMEOUT)
            print("Sale Numbers loaded.")
            
            # 5. Capture the Sale Number
            try:
                latest_sale_label = page.eval_on_selector('#cbosale', 
                    "(select) => select.options[1].text")
                if latest_sale_label:
                   match = re.search(r'(\d{1,2})', latest_sale_label)
                   if match:
                       sale_number = match.group(1)
            except Exception as e:
                print(f"Warning: Could not determine sale number: {e}")

            page.close() # Close the initial page
            
            print(f"Found categories to scrape: {[name for val, name in categories_to_scrape]}")
            print(f"Targeting Sale Number: {sale_number}")

            if not sale_number:
                raise ValueError("Cannot proceed without a Sale Number.")

            # --- Iterative Scraping Phase (Proven Stealth Logic) ---
            print("\n--- Iterative Scraping Phase ---")
            for category_value, category_name in categories_to_scrape:
                print(f"\nProcessing category: {category_name} on a fresh page...")
                page = context.new_page()
                page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

                page.goto(TARGET_URL, wait_until="networkidle", timeout=NAVIGATION_TIMEOUT)

                print("Performing selections (Centre -> Category -> Sale)...")
                # 1. Select Centre
                page.select_option("#cbocenter", label="KOLKATA")
                time.sleep(STABILIZATION_PAUSE_MEDIUM)
                
                # 2. Select Category
                page.wait_for_function("document.querySelectorAll('#cbocat option').length > 1", timeout=WAIT_FOR_UPDATE_TIMEOUT)
                page.select_option("#cbocat", value=category_value)
                time.sleep(STABILIZATION_PAUSE_MEDIUM)
                
                # 3. Select Sale
                page.wait_for_function("document.querySelectorAll('#cbosale option').length > 1", timeout=WAIT_FOR_UPDATE_TIMEOUT)
                page.select_option("#cbosale", index=1)
                
                # 4. Refresh and Extract
                page.click("#refresh")

                print("Waiting for report data to load...")
                page.wait_for_function("document.querySelector('#divmarketreport').innerText.length > 100", timeout=WAIT_FOR_UPDATE_TIMEOUT)

                report_div_html = page.locator("#divmarketreport").inner_html()
                soup = BeautifulSoup(report_div_html, 'lxml')
                report_text = soup.get_text(separator='\n', strip=True)

                all_reports_data[category_name] = report_text
                print(f"Successfully extracted text for {category_name}.")

                page.close()
                time.sleep(2)
                
        except Exception as e:
            print(f"!!! An error occurred: {e}")
            if 'page' in locals() and not page.is_closed():
                page.screenshot(path='error_screenshot_marketreport.png')
                print("Saved an error screenshot to error_screenshot_marketreport.png")
        finally:
            browser.close()
            print("Browser closed.")

    # --- Standardized Saving Mechanism ---
    if all_reports_data and sale_number:
        file_prefix = "JT_market_report_stealth"
        sale_suffix = str(sale_number).zfill(2)
        
        filename = f"{file_prefix}_S{sale_suffix}.json"
        output_path = OUTPUT_DIR / filename

        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(all_reports_data, f, indent=4, ensure_ascii=False)
            print(f"\n--- SCRAPING COMPLETE ---")
            print(f"Successfully saved combined report to {output_path}")
        except Exception as e:
            print(f"Error saving file: {e}")
    else:
        print("Scraping finished without extracting data or determining sale number. No file saved.")

if __name__ == "__main__":
    scrape_full_market_report_stealth()
