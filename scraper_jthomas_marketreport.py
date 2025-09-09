from playwright.sync_api import sync_playwright
import json
import os
import datetime
import time
from bs4 import BeautifulSoup

TARGET_URL = "https://jthomasindia.com/market_report.php"
OUTPUT_DIR = "source_reports/kolkata_market_reports"

def scrape_full_market_report_stealth():
    print(f"Starting STEALTH Playwright scraper for {TARGET_URL}...")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    all_reports_data = {}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        # Create a single browser "context" (like a profile) for the whole session
        context = browser.new_context()

        try:
            # We first need to get the list of categories to scrape
            print("Fetching list of categories...")
            page = context.new_page()
            page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            page.goto(TARGET_URL, wait_until="networkidle", timeout=60000)
            page.select_option("#cbocenter", label="KOLKATA")
            page.wait_for_function("document.querySelectorAll('#cbocat option').length > 1")
            categories = page.locator("#cbocat option").all()
            categories_to_scrape = [(opt.get_attribute("value"), opt.inner_text()) for opt in categories if opt.get_attribute("value")]
            page.close() # Close the initial page
            
            print(f"Found categories to scrape: {[name for val, name in categories_to_scrape]}")

            # Now, loop and use a FRESH PAGE for each category
            for category_value, category_name in categories_to_scrape:
                print(f"\n--- Processing category: {category_name} on a fresh page ---")
                page = context.new_page() # Create a new, clean page
                page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
                
                print("Navigating to page...")
                page.goto(TARGET_URL, wait_until="networkidle", timeout=60000)

                print("Performing selections...")
                page.select_option("#cbocenter", label="KOLKATA")
                page.wait_for_function("document.querySelectorAll('#cbocat option').length > 1")
                page.select_option("#cbocat", value=category_value)
                page.wait_for_function("document.querySelectorAll('#cbosale option').length > 1")
                page.select_option("#cbosale", index=1)
                page.click("#refresh")
                
                print("Waiting for report data to load...")
                page.wait_for_function("document.querySelector('#divmarketreport').innerText.length > 50", timeout=30000)
                
                report_div_html = page.locator("#divmarketreport").inner_html()
                soup = BeautifulSoup(report_div_html, 'lxml')
                report_text = soup.get_text(separator='\n', strip=True)
                
                all_reports_data[category_name] = report_text
                print(f"Successfully extracted text for {category_name}.")
                
                page.close() # Close the tab for this category to free up resources
                time.sleep(2) # Pause before starting the next one

        except Exception as e:
            print(f"!!! An error occurred: {e}")
            if 'page' in locals() and not page.is_closed():
                page.screenshot(path='error_screenshot.png')
                print("Saved an error screenshot to error_screenshot.png")
        finally:
            browser.close()
            print("Browser closed.")

    # Save the final combined report
    date_str = datetime.datetime.now().strftime('%Y-%m-%d')
    output_filename = f"JThomas_MarketReport_AllCategories_{date_str}.json"
    output_path = os.path.join(OUTPUT_DIR, output_filename)

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(all_reports_data, f, indent=4, ensure_ascii=False)
    
    print(f"\n--- SCRAPING COMPLETE ---")
    if all_reports_data:
        print(f"The combined report has been saved to: {output_path}")
    else:
        print("No data was collected.")

if __name__ == "__main__":
    scrape_full_market_report_stealth()
