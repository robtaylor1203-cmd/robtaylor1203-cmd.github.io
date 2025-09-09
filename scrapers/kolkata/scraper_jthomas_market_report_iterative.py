from playwright.sync_api import sync_playwright
import json
import os
import datetime
import time
from bs4 import BeautifulSoup
from pathlib import Path # Import Path for robust path handling (Workflow Adaptation)

# --- Configuration (Workflow Adaptation) ---
# Determine project base dynamically
REPO_BASE = Path(__file__).resolve().parent.parent.parent
TARGET_URL = "https://jthomasindia.com/market_report.php"
# Adapted Output Directory
OUTPUT_DIR = REPO_BASE / "source_reports" / "kolkata"

def scrape_full_market_report_stealth():
    print(f"Starting STEALTH Playwright scraper for {TARGET_URL}...")
    # Ensure directory exists using Path object
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Original storage format (used internally)
    all_reports_data = {}
    # Variables for standardized naming (Workflow Adaptation)
    latest_sale_no = "Unknown"

    # !!! Selectors EXACTLY as provided in the successful script (Functionality Preserved) !!!
    CENTRE_SELECTOR = "#cbocenter"
    CAT_SELECTOR = "#cbocat"
    SALE_SELECTOR = "#cbosale"
    REFRESH_BUTTON = "#refresh"
    REPORT_DIV = "#divmarketreport"

    with sync_playwright() as p:
        # Set timeout based on original script context (60s for navigation, default for actions)
        browser = p.chromium.launch(headless=True)
        # Create a single browser "context" (like a profile) for the whole session
        context = browser.new_context()

        try:
            # We first need to get the list of categories to scrape
            print("Fetching list of categories...")
            page = context.new_page()
            # Anti-bot evasion (Reconstructed from provided snippet)
            page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            page.goto(TARGET_URL, wait_until="networkidle", timeout=60000)
            
            # Interaction logic EXACTLY as provided
            page.select_option(CENTRE_SELECTOR, label="KOLKATA")
            
            # Wait function EXACTLY as provided
            # Added explicit timeout for robustness (60s)
            page.wait_for_function(f"document.querySelectorAll('{CAT_SELECTOR} option').length > 1", timeout=60000)
            
            # Capture Sale Number (Workflow Adaptation - using original selectors)
            try:
                # Ensure Sale dropdown also populated
                page.wait_for_function(f"document.querySelectorAll('{SALE_SELECTOR} option').length > 1", timeout=30000)
                latest_sale_value = page.eval_on_selector(SALE_SELECTOR, "(select) => select.options[1].value")
                if latest_sale_value:
                    latest_sale_no = latest_sale_value.split('/')[0]
                print(f"Latest Sale identified: {latest_sale_no}")
            except Exception as e:
                print(f"Warning: Could not determine sale number: {e}")

            categories = page.locator(f"{CAT_SELECTOR} option").all()
            # List comprehension (Reconstructed from provided snippet)
            categories_to_scrape = []
            for opt in categories:
                value = opt.get_attribute("value")
                text = opt.inner_text().strip()
                # Filter logic (Reconstructed from provided snippet)
                if value and value != '0':
                     categories_to_scrape.append((value, text))

            page.close() # Close the initial page
            
            print(f"Found categories to scrape: {[name for val, name in categories_to_scrape]}")

            # Now, loop and use a FRESH PAGE for each category (Functionality Preserved)
            for category_value, category_name in categories_to_scrape:
                print(f"\n--- Processing category: {category_name} on a fresh page ---")
                page = context.new_page() # Create a new, clean page
                # Anti-bot evasion (Reconstructed from provided snippet)
                page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

                print("Navigating to page...")
                page.goto(TARGET_URL, wait_until="networkidle", timeout=60000)

                print("Performing selections...")
                # Interaction logic EXACTLY as provided
                page.select_option(CENTRE_SELECTOR, label="KOLKATA")
                page.wait_for_function(f"document.querySelectorAll('{CAT_SELECTOR} option').length > 1", timeout=60000)
                
                page.select_option(CAT_SELECTOR, value=category_value)
                page.wait_for_function(f"document.querySelectorAll('{SALE_SELECTOR} option').length > 1", timeout=60000)
                
                page.select_option(SALE_SELECTOR, index=1)
                
                page.click(REFRESH_BUTTON)

                print("Waiting for report data to load...")
                # Wait function EXACTLY as provided (Reconstructed)
                # Wait until the report div has content (length > 100 characters as a safety margin)
                page.wait_for_function(f"document.querySelector('{REPORT_DIV}').innerText.length > 100", timeout=60000)

                report_div_html = page.locator(REPORT_DIV).inner_html()
                # Use lxml as specified in the original context
                soup = BeautifulSoup(report_div_html, 'lxml')
                report_text = soup.get_text(separator='\n', strip=True)

                # Store in the original dictionary format
                all_reports_data[category_name] = report_text
                print(f"Successfully extracted text for {category_name}.")

                page.close() # Close the tab for this category to free up resources
                time.sleep(2) # Pause before starting the next one
                
        except Exception as e:
            print(f"!!! An error occurred: {e}")
            # Save screenshot to the repository base (Workflow Adaptation)
            if 'page' in locals() and not page.is_closed():
                page.screenshot(path=str(REPO_BASE / 'error_screenshot_market_report_iterative.png'))
                print("Saved an error screenshot.")
        finally:
            browser.close()
            print("Browser closed.")

    # --- Save the final combined report (Workflow Adaptation) ---
    
    # Standardize the output format for the consolidation engine
    # Convert the dictionary {Category: Text} into a list of objects
    standardized_output = []
    if all_reports_data:
        for category_name, report_text in all_reports_data.items():
             # Ensure the text is not empty before adding
             if report_text:
                 standardized_output.append({
                    "type": f"Commentary - {category_name}",
                    "comment": report_text
                })

    if standardized_output:
        # Standardized output filename (e.g., market_commentary_S36.json)
        sale_suffix = f"S{str(latest_sale_no).zfill(2)}"
        # Use standardized name 'market_commentary'
        output_filename = f"market_commentary_{sale_suffix}.json"
        output_path = OUTPUT_DIR / output_filename

        with open(output_path, 'w', encoding='utf-8') as f:
            # Save as the standardized list
            json.dump(standardized_output, f, indent=4, ensure_ascii=False)
        
        print(f"\n--- SCRAPING COMPLETE ---")
        print(f"The combined report has been saved to: {output_path}")
    else:
        print(f"\n--- SCRAPING COMPLETE ---")
        print("No data was collected.")

if __name__ == "__main__":
    scrape_full_market_report_stealth()
