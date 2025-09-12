from playwright.sync_api import sync_playwright
import json
import os
import datetime
import time
from bs4 import BeautifulSoup
from pathlib import Path

# --- Configuration (Directory Updated for consistency) ---
TARGET_URL = "https://jthomasindia.com/market_report.php"
OUTPUT_DIR = Path(__file__).parent.parent.parent / "source_reports" / "kolkata"

# THIS IS YOUR UNCHANGED, WORKING SCRAPING FUNCTION
def scrape_full_market_report_stealth():
    print(f"Starting STEALTH Playwright scraper for {TARGET_URL}...")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    all_reports_data = {}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()

        try:
            print("Fetching list of categories...")
            page = context.new_page()
            # The init script was broken in your pasted code, this corrects it
            page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            page.goto(TARGET_URL, wait_until="networkidle", timeout=60000)
            page.select_option("#cbocenter", label="KOLKATA")
            page.wait_for_function("document.querySelectorAll('#cbocat option').length > 1")
            
            # Your original logic had a typo here, this corrects it to be functional
            categories = page.locator("#cbocat option").all()[1:]
            categories_to_scrape = [(opt.get_attribute("value"), opt.inner_text()) for opt in categories]
            page.close() 
            
            print(f"Found categories to scrape: {[name for val, name in categories_to_scrape]}")

            for category_value, category_name in categories_to_scrape:
                print(f"\n--- Processing category: {category_name} on a fresh page ---")
                page = context.new_page() 
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
                # Your original logic had a typo here, this corrects it
                page.wait_for_function("document.querySelector('#divmarketreport').innerText.length > 50")

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
                page.screenshot(path='error_screenshot.png')
                print("Saved an error screenshot to error_screenshot.png")
        finally:
            browser.close()
            print("Browser closed.")
    
    # The function now returns the data instead of saving it
    return all_reports_data

# --- NEW FUNCTION: Calculates Sale Number Based on Date ---
def calculate_sale_number():
    """
    Calculates the current sale number based on a fixed reference point.
    Sale 36 is the sale for the week commencing Sunday, August 31, 2025.
    """
    reference_date = datetime.date(2025, 8, 31)
    reference_sale_number = 36
    
    today = datetime.date.today()
    
    # Calculate the start of the week for the reference date (a Sunday)
    reference_week_start = reference_date - datetime.timedelta(days=reference_date.weekday() + 1 if reference_date.weekday() != 6 else 0)
    
    # Calculate the start of the week for today's date
    today_week_start = today - datetime.timedelta(days=today.weekday() + 1 if today.weekday() != 6 else 0)
    
    # Calculate the number of weeks that have passed
    weeks_passed = (today_week_start - reference_week_start).days // 7
    
    current_sale_number = reference_sale_number + weeks_passed
    print(f"Based on date logic, calculated current Sale Number is: {current_sale_number}")
    return current_sale_number

# --- MAIN EXECUTION BLOCK ---
if __name__ == "__main__":
    # 1. Run your trusted scraping function
    scraped_data = scrape_full_market_report_stealth()

    # 2. If data was scraped, proceed to save it
    if scraped_data:
        # 3. Calculate the sale number using our new logic
        sale_number = calculate_sale_number()

        # 4. Format the filename using the calculated sale number
        today_str = datetime.datetime.now().strftime('%Y%m%d')
        sale_suffix = f"S{sale_number}_{today_str}"
        output_filename = f"market_report_iterative_{sale_suffix}.json"
        output_path = OUTPUT_DIR / output_filename

        # 5. Save the file
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(scraped_data, f, indent=4, ensure_ascii=False)
        
        print(f"\n--- PROCESS COMPLETE ---")
        print(f"The combined report has been saved to: {output_path}")
    else:
        print("\n--- PROCESS HALTED: No data was collected. ---")
