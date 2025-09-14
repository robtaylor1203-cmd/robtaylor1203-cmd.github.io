import json
from playwright.sync_api import sync_playwright
import datetime
from pathlib import Path
from bs4 import BeautifulSoup

# Define directories
REPO_BASE = Path(__file__).resolve().parent.parent.parent
OUTPUT_DIR = REPO_BASE / "source_reports" / "kolkata"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

URL = "https://jthomasindia.com/market_report.php"

def scrape_jthomas_commentary_playwright():
    print(f"Starting Playwright scraper for J Thomas Commentary at {URL}...")
    
    commentary_data = []
    latest_sale_no = "Unknown"
    
    # Correct, case-sensitive selectors for this page
    CENTRE_SELECTOR = "#CboCentre"
    LEAF_SELECTOR = "#CboLeaf"
    SALE_SELECTOR = "#CboSale"
    REFRESH_BUTTON = "#filter"
    KOLKATA_VALUE = "5"
    # The div where the final report content is loaded
    RESULTS_CONTAINER = "#showdata"

    with sync_playwright() as p:
        user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=user_agent)
        page = context.new_page()

        try:
            print("Navigating to page and waiting for it to settle...")
            page.goto(URL, wait_until="networkidle", timeout=120000) # 2 minute timeout for initial load

            # --- 1. Select Centre ---
            print("Selecting 'Kolkata'...")
            page.select_option(CENTRE_SELECTOR, value=KOLKATA_VALUE)
            print("Waiting for page to update after centre selection...")
            page.wait_for_timeout(5000) # Wait for AJAX to populate other dropdowns

            # --- 2. Get Dynamic Parameters ---
            # Get the latest sale value
            latest_sale_value = page.eval_on_selector(SALE_SELECTOR, "(select) => select.options[1].value")
            latest_sale_no = latest_sale_value.split('/')[0]
            print(f"Latest Sale No identified: {latest_sale_no}")

            # Get all leaf types to loop through
            leaf_types = page.eval_on_selector(LEAF_SELECTOR, """(select) => {
                return Array.from(select.options)
                    .filter(opt => opt.value && opt.value !== '0')
                    .map(opt => ({value: opt.value, text: opt.innerText.trim()}));
            }""")
            print(f"Found Leaf Types: {[lt['text'] for lt in leaf_types]}")

            # --- 3. Loop, Click, and Scrape ---
            for leaf_type in leaf_types:
                print(f"\nProcessing Leaf Type: {leaf_type['text']}...")

                # Select the current leaf type
                page.select_option(LEAF_SELECTOR, value=leaf_type['value'])
                page.wait_for_timeout(2000)

                # Select the latest sale number
                page.select_option(SALE_SELECTOR, value=latest_sale_value)
                page.wait_for_timeout(2000)

                # Click the refresh/filter button
                print("Clicking 'Refresh' to load commentary...")
                page.click(REFRESH_BUTTON)

                # Wait for the results container to be populated with a table
                print("Waiting for commentary to appear...")
                page.wait_for_selector(f"{RESULTS_CONTAINER} table", timeout=60000)

                # Extract the text from the results container
                commentary_html = page.inner_html(RESULTS_CONTAINER)
                soup = BeautifulSoup(commentary_html, 'html.parser')
                commentary_text = soup.get_text(separator='\n', strip=True)

                if commentary_text:
                    commentary_data.append({
                        "type": f"Commentary - {leaf_type['text']}",
                        "comment": commentary_text
                    })
                    print("Successfully extracted commentary.")
                else:
                    print("Warning: Content loaded but no text found.")

        except Exception as e:
            print(f"\nERROR: An error occurred during scraping. Details: {e}")
            page.screenshot(path="error_screenshot.png")
            print("Saved an error screenshot to error_screenshot.png")

        finally:
            browser.close()
            print("Browser closed.")

    # --- 4. Save Data ---
    if commentary_data:
        sale_suffix = f"S{str(latest_sale_no).zfill(2)}"
        output_filename = f"market_commentary_{sale_suffix}.json"
        save_path = OUTPUT_DIR / output_filename

        with open(save_path, 'w', encoding='utf-8') as f:
            json.dump(commentary_data, f, indent=4, ensure_ascii=False)
        
        print(f"\nSUCCESS! Scraped {len(commentary_data)} commentary sections.")
        print(f"Data saved to {save_path}")
    else:
        print("\nFAILURE: No commentary data was collected.")

if __name__ == "__main__":
    scrape_jthomas_commentary_playwright()
