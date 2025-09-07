from playwright.sync_api import sync_playwright
import datetime
import os
import json
from urllib.parse import urljoin

LANDING_PAGE_URL = "https://web.forbestea.com/market-reports"
BASE_URL = "https://web.forbestea.com"
OUTPUT_DIR = "source_reports/colombo_raw_data"

def scrape_forbes_latest_report_playwright():
    print(f"Starting Playwright scraper for Forbes Tea at {LANDING_PAGE_URL}...")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        try:
            print("Navigating to page and waiting for it to settle...")
            page.goto(LANDING_PAGE_URL, wait_until="networkidle", timeout=60000)

            print("Finding and switching to the report selection iframe...")
            frame_locator = page.frame_locator('iframe[src="https://web.forbestea.com/report-server.php"]')
            
            # --- STEP 1: Select the Year (inside the iframe) ---
            year_selector = "#year"
            print("Finding the year dropdown inside the frame...")
            year_dropdown = frame_locator.locator(year_selector)
            year_dropdown.wait_for() 
            
            year_options = year_dropdown.locator("option").all()
            latest_year_value = year_options[-1].get_attribute("value")
            
            print(f"Selecting the latest year: {latest_year_value}...")
            year_dropdown.select_option(value=latest_year_value)
            
            # --- STEP 2: Select the Sale Number (inside the iframe) ---
            sale_selector = "#catid"
            print("Waiting for the sale number dropdown to appear (up to 60 seconds)...")
            sale_dropdown = frame_locator.locator(sale_selector)
            
            # --- THE FIX IS HERE ---
            # We give it a longer timeout to wait for the slow server response.
            sale_dropdown.wait_for(timeout=60000)

            sale_options = sale_dropdown.locator("option").all()
            latest_sale_value = sale_options[-1].get_attribute("value")

            print(f"Selecting the latest sale: {latest_sale_value}...")
            sale_dropdown.select_option(value=latest_sale_value)

            # --- STEP 3: Click Submit and catch the new page ---
            submit_button_selector = "button[type='submit']"
            print("Waiting for the 'Submit' button to appear...")
            submit_button = frame_locator.locator(submit_button_selector)
            submit_button.wait_for()

            print("Clicking 'Submit' and waiting for new page to open...")
            with page.context.expect_page() as new_page_info:
                submit_button.click()
            
            report_page = new_page_info.value
            report_page.wait_for_load_state("networkidle")
            print(f"Switched to new report page: {report_page.title()}")

            # --- STEP 4: Scrape the text from the new report page ---
            print("Scraping text content from the final report page...")
            content_selector = "div.itemView" 
            report_content_locator = report_page.locator(content_selector)
            report_text = report_content_locator.inner_text()
            
            # --- STEP 5: Save the extracted text as a JSON file ---
            output_data = { "report_title": report_page.title(), "retrieved_url": report_page.url, "raw_text": report_text }
            date_str = datetime.datetime.now().strftime('%Y-%m-%d')
            filename = f"forbes_report_{date_str}.json"
            output_path = os.path.join(OUTPUT_DIR, filename)
            
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=4, ensure_ascii=False)

            print(f"\nSuccess! Report text saved to: {output_path}")

        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            browser.close()

if __name__ == "__main__":
    scrape_forbes_latest_report_playwright()
