from playwright.sync_api import sync_playwright
import datetime
import os
import json
import time
from bs4 import BeautifulSoup
from urllib.parse import urljoin

# --- Configuration ---
LANDING_PAGE_URL = "https://jthomasindia.com/stats/BOL.php"
BASE_URL = "https://jthomasindia.com/stats/"
OUTPUT_DIR = "source_reports/kolkata_batting_order_reports"

def scrape_jthomas_batting_order():
    """
    Final version: Navigates, expands the menu, clicks each sub-link,
    and scrapes all text from the main content area of each sub-page.
    """
    print(f"--- Starting J. Thomas Batting Order Scraper for {LANDING_PAGE_URL} ---")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    all_reports_data = {}

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        try:
            print("Navigating to page and waiting for it to settle...")
            page.goto(LANDING_PAGE_URL, wait_until="networkidle", timeout=60000)

            # Step 1: Expand the "N. India" section
            n_india_expander_selector = "div[data-target='#demo_NI']"
            print("Finding and clicking the 'N. India' expander...")
            page.click(n_india_expander_selector)
            page.wait_for_timeout(2000)

            # Step 2: Get all the links to scrape
            links_to_scrape_locators = page.locator("#demo_NI a").all()
            
            links_info = []
            for link_locator in links_to_scrape_locators:
                links_info.append({
                    "text": link_locator.inner_text().strip(),
                    "href": link_locator.get_attribute("href")
                })

            print(f"Found {len(links_info)} sub-reports to process.")

            # Step 3: Loop through each link and scrape the page text
            for link_info in links_info:
                report_name = link_info['text']
                print(f"\n--- Processing: {report_name} ---")

                report_url = urljoin(BASE_URL, link_info['href'])
                page.goto(report_url, wait_until="networkidle")
                
                # --- THE FIX ---
                # Instead of looking for a table, we'll find the main content container.
                # A robust selector for this page is the div with the class 'wpb_wrapper'.
                content_container_selector = "div.wpb_wrapper"
                print("Waiting for main content to load...")
                page.locator(content_container_selector).first.wait_for(timeout=30000)
                
                # --- Step 4: Extract all text from that container ---
                content_html = page.locator(content_container_selector).first.inner_html()
                soup = BeautifulSoup(content_html, 'lxml')
                # Use get_text() to extract all text from all elements inside the container
                report_text = soup.get_text(separator='\n', strip=True)
                
                all_reports_data[report_name] = report_text
                print(f"Successfully scraped text for '{report_name}'.")
                time.sleep(2)

        except Exception as e:
            print(f"!!! An error occurred during scraping: {e}")
            page.screenshot(path='error_screenshot_bol.png')
            print("Saved an error screenshot to error_screenshot_bol.png")
        finally:
            browser.close()
            print("Browser closed.")

    # --- Step 5: Save the final combined JSON file ---
    date_str = datetime.datetime.now().strftime('%Y-%m-%d')
    output_filename = f"JThomas_BattingOrder_N_India_{date_str}.json"
    output_path = os.path.join(OUTPUT_DIR, output_filename)

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(all_reports_data, f, indent=4, ensure_ascii=False)
    
    print(f"\n--- PROCESS COMPLETE ---")
    print(f"Successfully saved combined data to: {output_path}")

if __name__ == "__main__":
    scrape_jthomas_batting_order()
