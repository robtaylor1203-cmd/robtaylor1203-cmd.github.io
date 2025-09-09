import os
import json
import datetime
import time
import re
from playwright.sync_api import sync_playwright, TimeoutError

# --- Configuration ---
LANDING_PAGE_URL = "https://ceylonteabrokers.com/market-reports/"
FINAL_OUTPUT_DIR = "source_reports/ceylon_tea_brokers_reports"

def scrape_latest_ceylon_tea_brokers_report():
    """
    Scrapes the LATEST Ceylon Tea Brokers report with a final, patient
    waiting strategy for slow-loading BI reports.
    """
    print(f"--- Starting Ceylon Tea Brokers Scraper (Final Patient Logic) ---")
    os.makedirs(FINAL_OUTPUT_DIR, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            viewport={'width': 1920, 'height': 1080}
        )
        page = context.new_page()
        page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        try:
            # Steps 1-5 remain the same
            print(f"[1/7] Navigating to {LANDING_PAGE_URL}...")
            page.goto(LANDING_PAGE_URL, wait_until="domcontentloaded", timeout=60000)

            print("[2/7] Checking for and accepting cookie consent banner...")
            try:
                page.get_by_role("button", name="Accept all").click(timeout=5000)
                print("Cookie consent banner accepted.")
            except TimeoutError:
                print("Cookie consent banner not found or already accepted, proceeding...")

            print("[3/7] Finding and clicking the FIRST main report link...")
            report_image_link = page.get_by_role("link", name=re.compile(r"Market Report – \d+", re.IGNORECASE)).first
            report_title = report_image_link.inner_text(timeout=60000)
            print(f"Found latest report link: '{report_title}'")
            report_image_link.click()
            page.wait_for_load_state("networkidle", timeout=60000)
            print(f"Successfully navigated to report page: {page.url}")

            print("[4/7] Clicking 'View Market Report' and waiting for new Sway tab...")
            with page.context.expect_page(timeout=60000) as new_page_info:
                page.get_by_role("link", name="View Market Report", exact=True).click()
            sway_page = new_page_info.value
            print(f"Switched to new Sway tab: {sway_page.url}")
            sway_page.wait_for_load_state("networkidle", timeout=90000)

            print("[5/7] Scrolling Sway presentation to find all content...")
            last_height = -1
            while True:
                sway_page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                sway_page.wait_for_timeout(2000)
                new_height = sway_page.evaluate("document.body.scrollHeight")
                if new_height == last_height:
                    print("Reached the end of the report.")
                    break
                last_height = new_height

            print("[6/7] Clicking all 'View' buttons to load BI reports...")
            view_buttons = sway_page.get_by_role("button", name="View")
            view_button_count = view_buttons.count()

            if view_button_count > 0:
                print(f"Found {view_button_count} 'View' button(s). Clicking them all...")
                for i in range(view_button_count):
                    view_buttons.first.click()
                    print(f"Clicked 'View' button #{i+1}")
                    # Brief pause to let the loading process begin
                    sway_page.wait_for_timeout(1000)
            else:
                print("No 'View' buttons found on the page.")

            print("[7/7] Extracting main text and embedded BI data...")
            body_element = sway_page.locator("body")
            full_text = body_element.inner_text()
            
            embedded_bi_data = []
            
            # --- FINAL STRATEGY: Patiently wait for the iframes to appear AFTER clicking ---
            try:
                if view_button_count > 0:
                    print("Patiently waiting for BI reports to load after clicks (up to 60s)...")
                    # This will wait until the FIRST iframe appears, with a long timeout
                    first_iframe = sway_page.locator('iframe[src*="powerbi.com"]').first
                    first_iframe.wait_for(timeout=60000)
                    print("BI reports have loaded.")
                
                iframe_elements = sway_page.locator('iframe[src*="powerbi.com"]')
                iframe_count = iframe_elements.count()

                if iframe_count > 0:
                    print(f"Found {iframe_count} loaded BI report(s). Extracting data...")
                    for i in range(iframe_count):
                        try:
                            frame = iframe_elements.nth(i).content_frame()
                            frame.locator("div.visual-container").first.wait_for(timeout=30000)
                            bi_text = frame.locator("body").inner_text()
                            embedded_bi_data.append(bi_text)
                            print(f"Successfully extracted data from BI report #{i+1}")
                        except TimeoutError:
                            print(f"Warning: Timed out while waiting for content inside Power BI report #{i+1}.")
                            embedded_bi_data.append("Error: Timed out waiting for BI content to load.")
                else:
                    print("No loaded Power BI reports found after waiting.")
            except TimeoutError:
                print("Process timed out: No BI reports appeared after clicking 'View' buttons.")

            output_data = {
                "report_title": report_title,
                "source_url": sway_page.url,
                "retrieval_date": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "raw_text": full_text,
                "embedded_bi_data": embedded_bi_data
            }

            date_str = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d')
            safe_title = "".join(c for c in report_title if c.isalnum() or c in (' ', '–', '&')).rstrip()
            clean_title = safe_title.replace(" ", "_").replace("–", "-").replace("&", "and")
            output_filename = f"CeylonTeaBrokers_{clean_title}_{date_str}.json"
            output_path = os.path.join(FINAL_OUTPUT_DIR, output_filename)

            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=4, ensure_ascii=False)
            
            print(f"\n--- PROCESS COMPLETE ---")
            print(f"Successfully saved enhanced data to: {output_path}")

        except Exception as e:
            print(f"!!! An unexpected error occurred: {e}")
            if not page.is_closed():
                page.screenshot(path='error_screenshot_ceylon.png')
                print("Saved an error screenshot to error_screenshot_ceylon.png")
        finally:
            print("Closing browser...")
            browser.close()

if __name__ == "__main__":
    scrape_latest_ceylon_tea_brokers_report()
