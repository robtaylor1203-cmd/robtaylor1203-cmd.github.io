import json
import datetime
import time
import re
from playwright.sync_api import sync_playwright, TimeoutError
from pathlib import Path

# --- Standardized Configuration (Adapted for Proven Script) ---
REPO_ROOT = Path(__file__).resolve().parent
LOCATION = "colombo"
OUTPUT_DIR = REPO_ROOT / "source_reports" / LOCATION
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

LANDING_PAGE_URL = "https://ceylonteabrokers.com/market-reports/"
# --------------------------------------------------------------

def determine_sale_number(report_title):
    """Helper function to determine sale number accurately."""
    # Strategy 1: Look for explicit "Sale No." or "Week"
    match = re.search(r'(?:Sale No\.|Week)[\s]*(\d{1,2})', report_title, re.IGNORECASE)
    if match:
        return match.group(1).zfill(2)

    # Strategy 2: Calculate ISO Week Number from the date in the title
    date_match = re.search(r'(\d{1,2})(?:st|nd|rd|th)?(?:\s*&\s*\d{1,2}(?:st|nd|rd|th)?)?\s+([A-Za-z]+)\s+(\d{4})', report_title)
    if date_match:
        day, month, year = date_match.groups()
        try:
            date_str = f"{day} {month} {year}"
            report_date = datetime.datetime.strptime(date_str, "%d %B %Y").date()
            iso_week = report_date.isocalendar()[1]
            print(f"Calculated ISO Week Number: {iso_week}")
            return str(iso_week).zfill(2)
        except ValueError:
            print(f"Warning: Could not parse date from title: {report_title}")
            return None
            
    return None


def scrape_latest_ceylon_tea_brokers_report():
    print(f"--- Starting Ceylon Tea Brokers Scraper (Final Patient Logic) ---")
    
    sale_week_number = None
    # (FIX): Increased BI Wait time significantly and added stabilization pause
    BI_WAIT_TIMEOUT = 240000 # 240 seconds (4 minutes)
    BI_STABILIZATION_PAUSE = 10 # 10 seconds

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        # Context configuration restored from proven script
        context = browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            viewport={'width': 1920, 'height': 1080}
        )
        page = context.new_page()
        # Stealth measure restored
        page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        try:
            # Steps 1-3
            print(f"[1/7] Navigating to {LANDING_PAGE_URL}...")
            page.goto(LANDING_PAGE_URL, wait_until="domcontentloaded", timeout=90000)

            print("[2/7] Checking for and accepting cookie consent banner...")
            try:
                page.get_by_role("button", name="Accept all").click(timeout=5000)
                print("Cookie consent banner accepted.")
            except TimeoutError:
                print("Cookie consent banner not found or already accepted, proceeding...")

            print("[3/7] Finding and clicking the FIRST main report link...")
            
            # Using the exact proven regex selector logic
            report_image_link = page.get_by_role("link", name=re.compile(r"Market Report – \d+", re.IGNORECASE)).first
            
            # Wait explicitly for the element before accessing text
            report_image_link.wait_for(timeout=60000)
            
            report_title = report_image_link.inner_text()
            print(f"Found latest report link: '{report_title}'")
            
            # Determine Sale Number
            sale_week_number = determine_sale_number(report_title)
            
            if not sale_week_number:
                 raise ValueError(f"Could not determine Sale/Week number from title: {report_title}")

            print(f"Determined Sale/Week Number: {sale_week_number}")

            report_image_link.click()
            page.wait_for_load_state("networkidle", timeout=90000)
            print(f"Successfully navigated to report page: {page.url}")

            # Steps 4-7 (Increased timeouts)
            print("[4/7] Clicking 'View Market Report' and waiting for new Sway tab...")
            with page.context.expect_page(timeout=90000) as new_page_info:
                page.locator("a:has-text('View Market Report')").first.click()

            sway_page = new_page_info.value
            print(f"Switched to new Sway tab: {sway_page.url}")
            sway_page.wait_for_load_state("networkidle", timeout=180000)

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
                    sway_page.wait_for_timeout(1500)
                
                # (FIX): Add stabilization pause AFTER clicking, before waiting for iframes
                print(f"Clicks complete. Pausing for {BI_STABILIZATION_PAUSE}s stabilization...")
                time.sleep(BI_STABILIZATION_PAUSE)

            else:
                print("No 'View' buttons found on the page.")

            print("[7/7] Extracting main text and embedded BI data...")
            body_element = sway_page.locator("body")
            full_text = body_element.inner_text()
            
            embedded_bi_data = []
            
            # --- Patiently wait for the iframes (Increased Timeout) ---
            try:
                if view_button_count > 0:
                    print(f"Patiently waiting for BI reports to load (up to {BI_WAIT_TIMEOUT/1000}s)...")
                    first_iframe = sway_page.locator('iframe[src*="powerbi.com"]').first
                    first_iframe.wait_for(timeout=BI_WAIT_TIMEOUT)
                    print("BI reports have loaded.")
                
                iframe_elements = sway_page.locator('iframe[src*="powerbi.com"]')
                iframe_count = iframe_elements.count()

                if iframe_count > 0:
                    print(f"Found {iframe_count} loaded BI report(s). Extracting data...")
                    for i in range(iframe_count):
                        try:
                            frame = iframe_elements.nth(i).content_frame()
                            frame.locator("div.visual-container").first.wait_for(timeout=90000)
                            bi_text = frame.locator("body").inner_text()
                            embedded_bi_data.append(bi_text)
                            print(f"Successfully extracted data from BI report #{i+1}")
                        except TimeoutError:
                            print(f"Warning: Timed out while waiting for content inside Power BI report #{i+1}.")
                            embedded_bi_data.append("Error: Timed out waiting for BI content to load.")
                else:
                    if view_button_count > 0:
                        print("No loaded Power BI reports found after waiting.")
            except TimeoutError:
                print("Process timed out: No BI reports appeared after clicking 'View' buttons.")

            output_data = {
                "report_title": report_title,
                "sale_week_number": sale_week_number,
                "source_url": sway_page.url,
                "retrieval_date": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "raw_text": full_text,
                "embedded_bi_data": embedded_bi_data
            }

            # --- Standardized Saving Mechanism ---
            file_prefix = "CTB_report_sway_parsed"
            sale_suffix = str(sale_week_number).zfill(2)
            output_filename = f"{file_prefix}_S{sale_suffix}.json"
            output_path = OUTPUT_DIR / output_filename

            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=4, ensure_ascii=False)
            
            print(f"\n--- PROCESS COMPLETE ---")
            print(f"Successfully saved enhanced data to: {output_path}")

        except Exception as e:
            print(f"!!! An unexpected error occurred: {e}")
            if 'page' in locals() and not page.is_closed():
                page.screenshot(path='error_screenshot_ceylon.png')
                print("Saved an error screenshot to error_screenshot_ceylon.png")
        finally:
            print("Closing browser...")
            browser.close()

if __name__ == "__main__":
    scrape_latest_ceylon_tea_brokers_report()
