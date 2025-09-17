from playwright.sync_api import sync_playwright
import json
import datetime
import time
from bs4 import BeautifulSoup
from pathlib import Path
import re
from pipeline_utils import generate_manifest

# --- Configuration ---
REPO_ROOT = Path(__file__).resolve().parent
TARGET_URL = "https://jthomasindia.com/market_report.php"

# Increased timeouts to address timeout issues
MAX_TIMEOUT = 3600000  # 60 minutes (increased from 10 minutes)
DISCOVERY_TIMEOUT = 600000  # 10 minutes (increased from 5 minutes)
STABILIZATION_WAIT = 120000  # 2 minutes (increased from 90 seconds)
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'

def get_centres_market_report(page):
    """Helper to discover available market report centres with enhanced waiting."""
    print("Discovering available market report centres...")
    
    page.wait_for_selector("#cbocenter", timeout=DISCOVERY_TIMEOUT)
    print("Initial page load complete. Waiting 15 seconds for stabilization...")
    page.wait_for_timeout(15000)
    
    print("Waiting for actual centre options to load...")
    page.wait_for_function("""
        () => {
            const select = document.querySelector('#cbocenter');
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
    centre_options = page.locator("#cbocenter option").all()
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

def get_categories_and_sales_for_centre(page, centre_label):
    """Helper to discover all categories and sales for a specific centre."""
    print(f"Discovering categories and sales for {centre_label}...")
    
    try:
        page.wait_for_function("document.querySelectorAll('#cbocat option').length > 1", timeout=180000)
        
        category_options = page.locator("#cbocat option").all()
        categories = []
        
        for opt in category_options:
            label = opt.inner_text().strip()
            value = opt.get_attribute("value")
            if (value and 
                value.strip() != "" and 
                not ("Select" in label or "Choose" in label)):
                categories.append({"label": label, "value": value})
        
        if not categories:
            print(f"No categories found for {centre_label}")
            return [], []
        
        first_category = categories[0]
        page.select_option("#cbocat", value=first_category["value"])
        page.wait_for_timeout(8000)
        
        page.wait_for_function("document.querySelectorAll('#cbosale option').length > 1", timeout=180000)
        
        sale_options = page.locator("#cbosale option").all()
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
        
        print(f"Found {len(categories)} categories and {len(sales)} sales for {centre_label}")
        return categories, sales
        
    except Exception as e:
        print(f"Warning: Could not get categories/sales for {centre_label}: {e}")
        return [], []

def scrape_jthomas_market_reports_all_sales():
    print(f"Starting J Thomas Market Reports scraper - ALL SALES (Enhanced Version)")
    print("\nIMPORTANT: This process may take 90+ minutes. Ensure system sleep is disabled.\n")

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
            
            print("Navigating to market reports page...")
            page.goto(TARGET_URL, wait_until="networkidle", timeout=DISCOVERY_TIMEOUT)
            
            centres = get_centres_market_report(page)
            page.close()

            if not centres:
                print("Warning: Could not find any market report centres. Using fallback.")
                centres = [
                    {"label": "KOLKATA", "value": "5", "folder_name": "kolkata"},
                    {"label": "GUWAHATI", "value": "4", "folder_name": "guwahati"},
                    {"label": "SILIGURI", "value": "6", "folder_name": "siliguri"}
                ]

            for centre in centres:
                location_label = centre['label']
                location_value = centre['value']
                location_folder = centre['folder_name']
                print(f"\n{'='*20} Processing Market Report Centre: {location_label} {'='*20}")
                
                centre_data = get_categories_and_sales_for_centre_data(context, location_label, location_value, location_folder)
                
                if centre_data['categories'] and centre_data['sales']:
                    print(f"Found {len(centre_data['categories'])} categories and {len(centre_data['sales'])} sales for {location_label}.")
                    
                    for sale in centre_data['sales']:
                        sale_label = sale['label']
                        sale_value = sale['value'] 
                        sale_number = sale['sale_number']
                        print(f"\n--- Processing Sale: {sale_label} (Sale #{sale_number}) for {location_label} ---")
                        
                        process_single_centre_market_report(context, location_label, location_value, location_folder, 
                                                           centre_data['categories'], sale_label, sale_value, sale_number)
                else:
                    print(f"No categories or sales found for {location_label}. Skipping.")

        except Exception as e:
            print(f"Critical error occurred: {e}")
        finally:
            browser.close()
            print("Browser closed.")

def get_categories_and_sales_for_centre_data(context, location_label, location_value, location_folder):
    """Discovers all categories and sales for a specific centre."""
    page = None
    try:
        page = context.new_page()
        page.set_default_timeout(MAX_TIMEOUT)
        page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        page.goto(TARGET_URL, wait_until="networkidle", timeout=300000)
        
        print(f"Selecting centre '{location_label}' to discover categories and sales...")
        page.select_option("#cbocenter", value=location_value)
        page.wait_for_timeout(15000)
        
        categories, sales = get_categories_and_sales_for_centre(page, location_label)
        return {"categories": categories, "sales": sales}
        
    except Exception as e:
        print(f"Error discovering categories/sales for {location_label}: {e}")
        return {"categories": [], "sales": []}
    finally:
        if page and not page.is_closed():
            page.close()

def process_single_centre_market_report(context, location_label, location_value, location_folder, categories, sale_label, sale_value, sale_number):
    """Handles the market report scraping logic for one specific centre and sale."""
    all_reports_data = {}
    page = None

    try:
        print(f"Processing {len(categories)} categories for {location_label} Sale {sale_number}")
        
        for category in categories:
            category_name = category['label']
            category_value = category['value']
            
            print(f"  Processing category: {category_name}")
            
            page = context.new_page()
            page.set_default_timeout(MAX_TIMEOUT)
            page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            page.goto(TARGET_URL, wait_until="networkidle", timeout=300000)

            print(f"    Selecting centre: {location_label}")
            page.select_option("#cbocenter", value=location_value)
            page.wait_for_timeout(15000)
            
            print(f"    Waiting for categories to load...")
            page.wait_for_function("document.querySelectorAll('#cbocat option').length > 1", timeout=180000)
            
            print(f"    Selecting category: {category_name}")
            page.select_option("#cbocat", value=category_value)
            page.wait_for_timeout(8000)
            
            print(f"    Waiting for sales to load...")
            page.wait_for_function("document.querySelectorAll('#cbosale option').length > 1", timeout=180000)
            
            print(f"    Selecting sale: {sale_label}")
            page.select_option("#cbosale", value=sale_value)
            page.wait_for_timeout(5000)
            
            print(f"    Clicking refresh...")
            page.click("#refresh")
            
            print(f"    Waiting for report data to load...")
            page.wait_for_function("document.querySelector('#divmarketreport').innerText.length > 100", timeout=MAX_TIMEOUT)
            
            page.wait_for_timeout(STABILIZATION_WAIT)
            
            report_div_html = page.locator("#divmarketreport").inner_html()
            soup = BeautifulSoup(report_div_html, 'lxml')
            report_text = soup.get_text(separator='\n', strip=True)
            
            all_reports_data[category_name] = report_text
            print(f"    Successfully extracted text for {category_name} ({len(report_text)} characters)")
            
            page.close()
            page = None
            time.sleep(3)

    except Exception as e:
        print(f"\nERROR: Failed during market report processing for {location_label} Sale {sale_number}. Details: {e}")
        if page and not page.is_closed():
            try:
                page.screenshot(path=f'error_screenshot_market_report_{location_folder}_S{sale_number}.png')
                print(f"Saved error screenshot")
            except:
                pass
        return

    finally:
        if page and not page.is_closed():
            page.close()

    if all_reports_data and sale_number:
        OUTPUT_DIR = REPO_ROOT / "source_reports" / location_folder
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        sale_suffix = str(sale_number).zfill(2)
        filename = f"JT_market_report_enhanced_S{sale_suffix}.json"
        output_path = OUTPUT_DIR / filename

        output_data = {
            "report_title": f"J Thomas Market Report - {location_label} - Sale {sale_number}",
            "centre": location_label,
            "sale_number": sale_number,
            "sale_label": sale_label,
            "extraction_timestamp": datetime.datetime.now().isoformat(),
            "categories_processed": len(all_reports_data),
            "reports_by_category": all_reports_data
        }

        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=4, ensure_ascii=False)
            
            print(f"\nSUCCESS! Market reports processed for {location_label} Sale {sale_number}.")
            print(f"Processed {len(all_reports_data)} categories: {list(all_reports_data.keys())}")
            print(f"Successfully saved to {output_path}")
            
            generate_manifest(REPO_ROOT, location_folder, sale_suffix, currency="INR")

        except Exception as e:
            print(f"Error saving file or generating manifest: {e}")
    else:
        print(f"Market reports processing finished for {location_label} Sale {sale_number} without complete data.")

if __name__ == "__main__":
    scrape_jthomas_market_reports_all_sales()
