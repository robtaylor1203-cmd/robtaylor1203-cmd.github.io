import json
from playwright.sync_api import sync_playwright
import datetime
from pathlib import Path
import re
from pipeline_utils import generate_manifest

# --- Configuration ---
REPO_ROOT = Path(__file__).resolve().parent
URL = "https://jthomasindia.com/auction_prices.php"

# Timeouts and waits (Restored from Proven Logic)
MAX_TIMEOUT = 600000 # 10 minutes
DISCOVERY_TIMEOUT = 300000 # 5 minutes (increased further)
STABILIZATION_WAIT = 90000 # 90 seconds
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'
# ---------------------

def get_centres(page):
    """Helper to discover available auction centres with enhanced waiting."""
    print("Discovering available auction centres...")
    
    # Wait for the dropdown to exist first
    page.wait_for_selector("#cbocentre", timeout=DISCOVERY_TIMEOUT)
    
    # Add a stabilization wait to let the page settle
    print("Initial page load complete. Waiting 10 seconds for stabilization...")
    page.wait_for_timeout(10000)
    
    # Now wait specifically for real options to appear (not just the placeholder)
    print("Waiting for actual centre options to load...")
    page.wait_for_function("""
        () => {
            const select = document.querySelector('#cbocentre');
            if (!select) return false;
            const options = select.querySelectorAll('option');
            
            // We need more than just the "Select Centre" option
            if (options.length <= 1) return false;
            
            // Check if we have real options (with actual values)
            let realOptions = 0;
            for (let i = 0; i < options.length; i++) {
                const value = options[i].value;
                const text = options[i].textContent;
                if (value && value.trim() !== '' && !text.includes('Select')) {
                    realOptions++;
                }
            }
            return realOptions > 0;
        }
    """, timeout=DISCOVERY_TIMEOUT)
    
    print("Real centre options detected. Extracting centres...")
    centre_options = page.locator("#cbocentre option").all()
    centres = []
    
    for opt in centre_options:
        label = opt.inner_text().strip()
        value = opt.get_attribute("value")
        # Enhanced filtering
        if (value and 
            value.strip() != "" and 
            value != "0" and 
            not ("Select" in label or "Choose" in label or "Loading" in label)):
            folder_name = label.lower().replace(' ', '_')
            centres.append({"label": label, "value": value, "folder_name": folder_name})
    
    print(f"Found centres: {[c['label'] for c in centres]}")
    return centres

def scrape_jthomas_lots_expanded():
    print(f"Starting EXPANDED J Thomas Auction Lots scraper (Proven Logic)...")
    print("\nREMINDER: Ensure Chromebook sleep settings are disabled. This process may take 15+ minutes per centre.\n")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, timeout=MAX_TIMEOUT)
        context = browser.new_context(user_agent=USER_AGENT)
        
        try:
            # Discover centres dynamically with enhanced logic
            page = context.new_page()
            page.set_default_timeout(DISCOVERY_TIMEOUT)
            
            print("Navigating to auction lots page...")
            page.goto(URL, wait_until="networkidle", timeout=DISCOVERY_TIMEOUT)
            
            centres = get_centres(page)
            page.close()

            if not centres:
                print("Warning: Could not find any auction centres. The site may be having issues.")
                # Fallback to known centres if discovery fails
                centres = [
                    {"label": "KOLKATA", "value": "5", "folder_name": "kolkata"},
                    {"label": "GUWAHATI", "value": "4", "folder_name": "guwahati"},
                    {"label": "SILIGURI", "value": "6", "folder_name": "siliguri"}
                ]
                print(f"Using fallback centres: {[c['label'] for c in centres]}")

            # Iterate through each centre
            for centre in centres:
                location_label = centre['label']
                location_value = centre['value']
                location_folder = centre['folder_name']
                print(f"\n{'='*20} Processing Centre: {location_label} (Value: {location_value}) {'='*20}")
                
                process_single_centre_lots(context, location_label, location_value, location_folder)

        except Exception as e:
            print(f"!!! A critical error occurred during the main process: {e}")
        finally:
            browser.close()
            print("Browser closed.")

def process_single_centre_lots(context, location_label, location_value, location_folder):
    """Handles the scraping logic for one specific auction centre."""
    scraped_data = []
    latest_sale_no = None
    page = None

    try:
        page = context.new_page()
        page.set_default_timeout(MAX_TIMEOUT)
        
        # --- Navigation ---
        page.goto(URL, wait_until="networkidle", timeout=180000)

        # --- Centre Selection ---
        print(f"Selecting '{location_label}'...")
        centre_dropdown_selector = "#cbocentre"
        page.select_option(centre_dropdown_selector, value=location_value)

        # --- Fixed Wait ---
        print("Auction centre selected. Waiting for page to update (Fixed 5s wait)...")
        page.wait_for_timeout(5000)

        # --- Sale Selection ---
        sale_dropdown_selector = "#cbosale"

        try:
            page.wait_for_selector(f"{sale_dropdown_selector} option:nth-child(2)", state="attached", timeout=120000)
            
            latest_sale_label = page.eval_on_selector(sale_dropdown_selector, 
                "(select) => select.options[1].text")
            if latest_sale_label:
                match = re.search(r'(\d{1,2})', latest_sale_label)
                if match:
                    latest_sale_no = match.group(1)
        except Exception:
             print(f"Warning: Could not determine sale number or no sales available for {location_label}. Skipping.")
             return

        if latest_sale_no:
            page.select_option(sale_dropdown_selector, index=1)
        else:
            raise ValueError("Cannot proceed without determining the Sale Number.")

        # --- Fixed Wait ---
        print("Sale selected. Waiting (Fixed 3s wait)...")
        page.wait_for_timeout(3000)

        # --- Trigger Load ---
        page.click("#filter")

        # --- Wait for Table Structure ---
        print(f"Waiting for the table structure to appear (Max wait: 10 minutes)...")
        table_selector = "#showdata table"
        page.wait_for_selector(table_selector, timeout=MAX_TIMEOUT)

        # !!! CRITICAL STABILIZATION PAUSE !!!
        print(f"Table structure visible. Waiting significantly ({STABILIZATION_WAIT/1000}s) for data rendering to stabilize...")
        page.wait_for_timeout(STABILIZATION_WAIT) 

        # --- Data Extraction ---
        rows = page.locator(f"{table_selector} tr:nth-child(n+2)").all()
        print(f"Found {len(rows)} data rows. Extracting data (this will take time)...")

        if rows:
            for i, row in enumerate(rows):
                cells = row.locator("td").all_text_contents()
                if len(cells) >= 6:
                    lot_no = cells[0].strip()
                    if lot_no:
                        scraped_data.append({
                            "lot_no": lot_no,
                            "garden": cells[1].strip(),
                            "grade": cells[2].strip(),
                            "invoice": cells[3].strip(),
                            "packages": cells[4].strip(),
                            "price_inr": cells[5].strip(),
                            "centre": location_label,
                            "sale_number": latest_sale_no
                        })
                if (i + 1) % 1000 == 0:
                    print(f"  Processed {i + 1} rows...")

    except Exception as e:
        print(f"\nERROR: Failed during scraping {location_label}. Details: {e}")
        return

    finally:
        if page and not page.is_closed():
            page.close()

    # --- Standardized Saving & Auto-Manifest Mechanism ---
    if scraped_data and latest_sale_no:
        OUTPUT_DIR = REPO_ROOT / "source_reports" / location_folder
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        sale_suffix = str(latest_sale_no).zfill(2)
        filename = f"JT_auction_lots_stealth_S{sale_suffix}.json"
        output_path = OUTPUT_DIR / filename

        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(scraped_data, f, indent=4, ensure_ascii=False)
            print(f"\nSUCCESS! Scraped {len(scraped_data)} rows for {location_label}.")
            print(f"Successfully saved data to {output_path}")
            
            generate_manifest(REPO_ROOT, location_folder, sale_suffix, currency="INR")

        except Exception as e:
            print(f"Error saving file or generating manifest: {e}")
    else:
        print(f"Scraping finished for {location_label} without complete data. No file saved.")

if __name__ == "__main__":
    scrape_jthomas_lots_expanded()
