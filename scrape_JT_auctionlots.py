import json
from playwright.sync_api import sync_playwright
import datetime
from pathlib import Path
import re
from pipeline_utils import generate_manifest

# --- Configuration ---
REPO_ROOT = Path(__file__).resolve().parent
URL = "https://jthomasindia.com/auction_prices.php"

# Increased timeouts to address timeout issues
MAX_TIMEOUT = 3600000  # 60 minutes (increased from 10 minutes)
DISCOVERY_TIMEOUT = 600000  # 10 minutes (increased from 5 minutes)
STABILIZATION_WAIT = 120000  # 2 minutes (increased from 90 seconds)
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'

def get_centres(page):
    """Helper to discover available auction centres with enhanced waiting."""
    print("Discovering available auction centres...")
    
    page.wait_for_selector("#cbocentre", timeout=DISCOVERY_TIMEOUT)
    print("Initial page load complete. Waiting 15 seconds for stabilization...")
    page.wait_for_timeout(15000)
    
    print("Waiting for actual centre options to load...")
    page.wait_for_function("""
        () => {
            const select = document.querySelector('#cbocentre');
            if (!select) return false;
            const options = select.querySelectorAll('option');
            if (options.length <= 1) return false;
            
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
        if (value and 
            value.strip() != "" and 
            value != "0" and 
            not ("Select" in label or "Choose" in label or "Loading" in label)):
            folder_name = label.lower().replace(' ', '_')
            centres.append({"label": label, "value": value, "folder_name": folder_name})
    
    print(f"Found centres: {[c['label'] for c in centres]}")
    return centres

def get_available_sales_for_centre(page, centre_label):
    """Helper to discover all available sales for a specific centre."""
    print(f"Discovering available sales for {centre_label}...")
    
    sale_dropdown_selector = "#cbosale"
    
    try:
        page.wait_for_function(f"""
            () => {{
                const select = document.querySelector('{sale_dropdown_selector}');
                if (!select) return false;
                const options = select.querySelectorAll('option');
                if (options.length <= 1) return false;
                
                let realOptions = 0;
                for (let i = 0; i < options.length; i++) {{
                    const value = options[i].value;
                    const text = options[i].textContent;
                    if (value && value.trim() !== '' && !text.includes('Select') && !text.includes('Choose')) {{
                        realOptions++;
                    }}
                }}
                return realOptions > 0;
            }}
        """, timeout=180000)
        
        print("Sale options loaded. Extracting all sales...")
        sale_options = page.locator(f"{sale_dropdown_selector} option").all()
        sales = []
        
        for i, opt in enumerate(sale_options):
            if i == 0:
                continue
                
            label = opt.inner_text().strip()
            value = opt.get_attribute("value")
            
            if (value and 
                value.strip() != "" and 
                not ("Select" in label or "Choose" in label)):
                
                match = re.search(r'(\d{1,2})', label)
                if match:
                    sale_number = match.group(1)
                    sales.append({
                        "label": label,
                        "value": value, 
                        "sale_number": sale_number
                    })
        
        print(f"Found {len(sales)} sales for {centre_label}: {[(s['label'], s['sale_number']) for s in sales]}")
        return sales
        
    except Exception as e:
        print(f"Warning: Could not get sales for {centre_label}: {e}")
        return []

def scrape_jthomas_lots_all_sales():
    print(f"Starting J Thomas Auction Lots scraper - ALL SALES (Enhanced Version)")
    print("\nIMPORTANT: This process may take 60+ minutes. Ensure system sleep is disabled.\n")

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True, 
            timeout=MAX_TIMEOUT,
            args=['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
        )
        context = browser.new_context(
            user_agent=USER_AGENT,
            viewport={'width': 1920, 'height': 1080}
        )
        
        try:
            page = context.new_page()
            page.set_default_timeout(DISCOVERY_TIMEOUT)
            
            print("Navigating to auction lots page...")
            page.goto(URL, wait_until="networkidle", timeout=DISCOVERY_TIMEOUT)
            
            centres = get_centres(page)
            page.close()

            if not centres:
                print("Warning: Could not find any auction centres. Using fallback.")
                centres = [
                    {"label": "KOLKATA", "value": "5", "folder_name": "kolkata"},
                    {"label": "GUWAHATI", "value": "4", "folder_name": "guwahati"},
                    {"label": "SILIGURI", "value": "6", "folder_name": "siliguri"}
                ]

            for centre in centres:
                location_label = centre['label']
                location_value = centre['value']
                location_folder = centre['folder_name']
                print(f"\n{'='*20} Processing Auction Centre: {location_label} {'='*20}")
                
                centre_sales = get_all_sales_for_centre(context, location_label, location_value, location_folder)
                
                if centre_sales:
                    print(f"Found {len(centre_sales)} sales for {location_label}. Processing all...")
                    for sale in centre_sales:
                        sale_label = sale['label']
                        sale_value = sale['value'] 
                        sale_number = sale['sale_number']
                        print(f"\n--- Processing Sale: {sale_label} (Sale #{sale_number}) for {location_label} ---")
                        process_single_centre_lots(context, location_label, location_value, location_folder, sale_label, sale_value, sale_number)
                else:
                    print(f"No sales found for {location_label}. Skipping.")

        except Exception as e:
            print(f"Critical error occurred: {e}")
        finally:
            browser.close()
            print("Browser closed.")

def get_all_sales_for_centre(context, location_label, location_value, location_folder):
    """Discovers all available sales for a specific centre."""
    page = None
    try:
        page = context.new_page()
        page.set_default_timeout(MAX_TIMEOUT)
        page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        page.goto(URL, wait_until="networkidle", timeout=300000)
        
        print(f"Selecting centre '{location_label}' to discover sales...")
        page.select_option("#cbocentre", value=location_value)
        page.wait_for_timeout(8000)
        
        sales = get_available_sales_for_centre(page, location_label)
        return sales
        
    except Exception as e:
        print(f"Error discovering sales for {location_label}: {e}")
        return []
    finally:
        if page and not page.is_closed():
            page.close()

def process_single_centre_lots(context, location_label, location_value, location_folder, sale_label, sale_value, sale_number):
    """Handles the scraping logic for one specific auction centre and sale."""
    scraped_data = []
    page = None

    try:
        page = context.new_page()
        page.set_default_timeout(MAX_TIMEOUT)
        page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        page.goto(URL, wait_until="networkidle", timeout=300000)

        print(f"Selecting '{location_label}'...")
        page.select_option("#cbocentre", value=location_value)
        print("Centre selected. Waiting for page to update...")
        page.wait_for_timeout(8000)

        print(f"Selecting specific sale: {sale_label} (#{sale_number})")
        page.select_option("#cbosale", value=sale_value)
        print("Sale selected. Waiting...")
        page.wait_for_timeout(5000)

        page.click("#filter")
        print(f"Waiting for the table structure to appear...")
        table_selector = "#showdata table"
        page.wait_for_selector(table_selector, timeout=MAX_TIMEOUT)

        print(f"Table structure visible. Waiting for data rendering to stabilize...")
        page.wait_for_timeout(STABILIZATION_WAIT) 

        rows = page.locator(f"{table_selector} tr:nth-child(n+2)").all()
        print(f"Found {len(rows)} data rows. Extracting data...")

        if rows:
            for i, row in enumerate(rows):
                try:
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
                                "sale_number": sale_number
                            })
                    if (i + 1) % 1000 == 0:
                        print(f"  Processed {i + 1} rows...")
                except Exception as e:
                    print(f"Error processing row {i}: {e}")
                    continue

    except Exception as e:
        print(f"\nERROR: Failed during scraping {location_label} Sale {sale_number}. Details: {e}")
        if page and not page.is_closed():
            try:
                page.screenshot(path=f'error_screenshot_lots_{location_folder}_S{sale_number}.png')
                print(f"Saved error screenshot")
            except:
                pass
        return

    finally:
        if page and not page.is_closed():
            page.close()

    if scraped_data and sale_number:
        OUTPUT_DIR = REPO_ROOT / "source_reports" / location_folder
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        sale_suffix = str(sale_number).zfill(2)
        filename = f"JT_auction_lots_enhanced_S{sale_suffix}.json"
        output_path = OUTPUT_DIR / filename

        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(scraped_data, f, indent=4, ensure_ascii=False)
            print(f"\nSUCCESS! Scraped {len(scraped_data)} rows for {location_label} Sale {sale_number}.")
            print(f"Successfully saved data to {output_path}")
            
            generate_manifest(REPO_ROOT, location_folder, sale_suffix, currency="INR")

        except Exception as e:
            print(f"Error saving file or generating manifest: {e}")
    else:
        print(f"Scraping finished for {location_label} Sale {sale_number} without complete data.")

if __name__ == "__main__":
    scrape_jthomas_lots_all_sales()
