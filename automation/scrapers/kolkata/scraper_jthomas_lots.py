import json
from playwright.sync_api import sync_playwright
import datetime
from pathlib import Path

# Define the base directory and output directory
REPO_BASE = Path(__file__).resolve().parent.parent.parent
OUTPUT_DIR = REPO_BASE / "raw_downloads" / "kolkata"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

URL = "https://jthomasindia.com/auction_prices.php"

def scrape_jthomas_lots():
    print(f"Starting J Thomas Lot scraper (Reverting to Proven Logic with Stabilization Wait)...")
    
    scraped_data = []
    latest_sale_no = "Unknown"
    
    # Set Maximum Timeout: 10 minutes (600 seconds)
    # This is necessary due to the slow server response and massive rendering task.
    MAX_TIMEOUT = 600000 

    with sync_playwright() as p:
        user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
        
        # We use headless=True (background mode)
        browser = p.chromium.launch(headless=True, timeout=MAX_TIMEOUT)
        context = browser.new_context(user_agent=user_agent)
        page = context.new_page()
        page.set_default_timeout(MAX_TIMEOUT)

        try:
            # --- Navigation (Using logic from successful run) ---
            print("Navigating to page and waiting for it to settle (networkidle)...")
            page.goto(URL, wait_until="networkidle", timeout=120000) # 2 min nav timeout

            print("Page is fully loaded. Selecting 'Kolkata'...")
            centre_dropdown_selector = "#cbocentre"
            kolkata_option_value = "5"
            page.select_option(centre_dropdown_selector, value=kolkata_option_value)
            
            # --- Fixed Wait (Crucial step from successful run) ---
            # We use fixed waits as they were proven to work with this site's timing.
            print("Auction centre selected. Waiting for page to update (Fixed 5s wait)...")
            page.wait_for_timeout(5000) # 5 seconds pause

            # --- Sale Selection ---
            print("Selecting the most recent sale number...")
            sale_dropdown_selector = "#cbosale"
            
            # Determine the sale number for the filename
            try:
                # Ensure the option is attached before reading it (handles hidden elements)
                # We must ensure the dropdown has populated after the wait.
                page.wait_for_selector(f"{sale_dropdown_selector} option:nth-child(2)", state="attached", timeout=10000)
                latest_sale_value = page.eval_on_selector(sale_dropdown_selector, 
                    "(select) => select.options[1].value")
                if latest_sale_value:
                    # Handle cases like "36/2025"
                    latest_sale_no = latest_sale_value.split('/')[0]
            except Exception:
                print("Warning: Could not determine sale number dynamically, defaulting to 'Unknown'. Proceeding.")

            # Select the second option by index
            page.select_option(sale_dropdown_selector, index=1)
            
            # --- Fixed Wait (From successful run) ---
            print("Sale selected. Waiting (Fixed 3s wait)...")
            page.wait_for_timeout(3000) # 3 seconds pause

            # --- Trigger Load ---
            print(f"Clicking 'SHOW PRICES' button...")
            show_prices_button_selector = "#filter"
            page.click(show_prices_button_selector)

            # --- Wait for Table Structure ---
            print(f"Waiting for the table structure to appear (Maximum wait: 10 minutes)...")
            # The selector proven to work
            table_selector = "#showdata table"
            # Wait for the table container to appear
            page.wait_for_selector(table_selector, timeout=MAX_TIMEOUT)
            
            # !!! CRITICAL STABILIZATION PAUSE !!!
            # The website renders the table structure first, then populates 11k+ rows.
            # We must wait a significant amount of time for the rendering to complete BEFORE locating rows.
            # This addresses the "0 rows found" issue.
            STABILIZATION_WAIT = 90000 # 90 seconds
            print(f"Table structure visible. Waiting significantly ({STABILIZATION_WAIT/1000}s) for data rendering to stabilize...")
            page.wait_for_timeout(STABILIZATION_WAIT) 

            # --- Data Extraction (From successful run) ---
            print("Finding data rows...")
            # Locate rows starting from the second one
            rows = page.locator(f"{table_selector} tr:nth-child(n+2)").all()
            
            print(f"Found {len(rows)} data rows. Extracting data (this will take time)...")

            if not rows:
                print("Warning: No data rows were found. The stabilization wait might need increasing.")
            else:
                # Iterate and extract data (This is slow but proven to work)
                for i, row in enumerate(rows):
                    cells = row.locator("td").all_text_contents()
                    if len(cells) >= 6:
                         # Ensure Lot No (the first cell) is not empty
                        lot_no = cells[0].strip()
                        if lot_no:
                            scraped_data.append({
                                "lot_no": lot_no,
                                "garden": cells[1].strip(),
                                "grade": cells[2].strip(),
                                "invoice": cells[3].strip(),
                                "packages": cells[4].strip(),
                                "price_inr": cells[5].strip()
                            })
                    # Progress indicator
                    if (i + 1) % 500 == 0:
                        print(f"  Processed {i + 1} rows...")

        except Exception as e:
            print(f"\nERROR: Failed during scraping J Thomas Lots. Details: {e}")
            error_screenshot = REPO_BASE / "error_screenshot_jthomas_lots.png"
            try:
                page.screenshot(path=error_screenshot)
                print(f"Saved error screenshot to {error_screenshot}")
            except Exception:
                 print("Could not save screenshot.")
            return

        finally:
            browser.close()
            print("Browser closed.")

    # --- Save Data ---
    if scraped_data:
        # Generate filename: raw_lots_S37_YYYYMMDD.json
        # Note: The environment clock is currently Sept 8, 2025
        today = datetime.date.today().strftime("%Y%m%d")
        # Ensure sale_no is treated as a string before zfill
        sale_suffix = f"S{str(latest_sale_no).zfill(2)}"
        output_filename = f"raw_lots_{sale_suffix}_{today}.json"
        save_path = OUTPUT_DIR / output_filename

        with open(save_path, 'w', encoding='utf-8') as f:
            json.dump(scraped_data, f, indent=4, ensure_ascii=False)
        
        print(f"\nSUCCESS! Scraped {len(scraped_data)} rows.")
        print(f"Raw data saved to {save_path}")
        print(f"NEXT STEP: Run the processor script.")

# Standard Python execution block
if __name__ == "__main__":
    # REMINDER: Keep Chromebook awake during this long process! (Settings -> Device -> Power)
    print("\nREMINDER: Ensure Chromebook sleep settings are disabled (Keep Display On). This process may take 10-15 minutes.\n")
    scrape_jthomas_lots()
