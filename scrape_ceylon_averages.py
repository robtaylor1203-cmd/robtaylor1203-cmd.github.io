import os
import json
import datetime
from playwright.sync_api import sync_playwright

# --- Configuration ---
TARGET_URL = "https://ceylonteabrokers.com/weekly-sales-averages/"
FINAL_OUTPUT_DIR = "source_reports/ceylon_tea_brokers_averages"

def scrape_weekly_sales_averages_final():
    """
    Scrapes weekly sales averages by first scrolling the main page to trigger
    the dynamic creation of the data iframe, then extracting the data.
    """
    print(f"--- Starting Final Scraper for Weekly Averages ---")
    os.makedirs(FINAL_OUTPUT_DIR, exist_ok=True)
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        try:
            # --- PART 1: Navigate and SCROLL to create the iframe ---
            print(f"[1/3] Navigating to page: {TARGET_URL}")
            page.goto(TARGET_URL, wait_until="load", timeout=60000)

            # --- KEY STEP: Scroll down to trigger the iframe creation ---
            print("Scrolling page to trigger lazy-loaded content...")
            # We scroll the main page window down to ensure the script runs
            page.evaluate("window.scrollBy(0, 1000)") # Scroll down by 1000 pixels
            # Wait for the JavaScript to execute and create the iframe
            page.wait_for_timeout(3000)

            print("Locating the dynamically loaded data iframe...")
            # Now the iframe should exist on the page
            iframe_content = page.frame_locator('iframe[src*="/widgets/sales/"]')
            
            # Wait for the content inside the frame to be ready
            iframe_content.locator("div.row").first.wait_for(timeout=30000)
            
            # --- PART 2: Extract data from within the now-loaded iframe ---
            print("[2/3] Reconstructing table from divs inside the iframe...")
            
            data_rows = iframe_content.locator("div.row").all()[1:] # Ignore header row
            
            if not data_rows:
                raise Exception("Could not find any data rows inside the iframe.")

            extracted_data = []
            for row_element in data_rows:
                cells = row_element.locator("div.cell").all()
                row_data = [cell.inner_text() for cell in cells]
                extracted_data.append(row_data)

            print(f"Successfully extracted {len(extracted_data)} rows of data.")

            headers = ["Category", "2025", "2024", "2023", "2022", "2021", "2020", "2019"]
            structured_data = [dict(zip(headers, row)) for row in extracted_data]

            # --- PART 3: Save the data ---
            print("[3/3] Saving extracted data to JSON file...")
            output_data = {
                "report_title": "Ceylon Tea Brokers - Weekly Sales Averages",
                "source_url": TARGET_URL,
                "retrieval_date": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "weekly_averages": structured_data
            }

            date_str = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d')
            output_filename = f"CeylonTeaBrokers_WeeklyAverages_{date_str}.json"
            output_path = os.path.join(FINAL_OUTPUT_DIR, output_filename)

            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=4, ensure_ascii=False)

            print(f"\n--- PROCESS COMPLETE ---")
            print(f"Successfully saved parsed data to: {output_path}")

        except Exception as e:
            print(f"!!! An unexpected error occurred: {e}")
            page.screenshot(path='error_screenshot_averages.png')
            print("Saved an error screenshot to error_screenshot_averages.png")
        finally:
            browser.close()

if __name__ == "__main__":
    scrape_weekly_sales_averages_final()
