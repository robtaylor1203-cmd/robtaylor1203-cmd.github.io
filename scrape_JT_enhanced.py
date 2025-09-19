#!/usr/bin/env python3
"""
Enhanced J Thomas Scraper - Multiple data types with current workflow compatibility
Includes auction lots, market reports, synopsis, and district averages as supporting data
"""

import json
import datetime
import time
import random
import re
from playwright.sync_api import sync_playwright, TimeoutError
from pathlib import Path
from pipeline_utils import generate_manifest

# --- Configuration ---
REPO_ROOT = Path(__file__).resolve().parent
BASE_URLS = {
    'auction_lots': "https://jthomasindia.com/auction_prices.php",
    'market_reports': "https://jthomasindia.com/market_report.php", 
    'synopsis': "https://jthomasindia.com/market_synopsis.php",
    'district_averages': "https://jthomasindia.com/district_average.php"
}

# Enhanced timeouts
MAX_TIMEOUT = 600000
DISCOVERY_TIMEOUT = 300000
STABILIZATION_WAIT = 90000

# Anti-bot measures
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
]

# Location mapping - matches your current structure
LOCATION_MAPPING = {
    'KOLKATA': 'kolkata',
    'GUWAHATI': 'guwahati', 
    'SILIGURI': 'siliguri',
    'COCHIN': 'cochin',
    'COIMBATORE': 'coimbatore',
    'COONOOR': 'coonoor',
    'JALPAIGURI': 'jalpaiguri'
}

def random_delay(min_seconds=3, max_seconds=8):
    """Human-like delays"""
    delay = random.uniform(min_seconds, max_seconds)
    print(f"    Delay: {delay:.1f}s")
    time.sleep(delay)

def create_stealth_context(p):
    """Anti-bot browser setup"""
    user_agent = random.choice(USER_AGENTS)
    
    browser = p.chromium.launch(
        headless=True,
        timeout=MAX_TIMEOUT,
        args=['--no-sandbox', '--disable-blink-features=AutomationControlled']
    )
    
    context = browser.new_context(
        user_agent=user_agent,
        viewport={'width': 1920, 'height': 1080},
        extra_http_headers={
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        }
    )
    
    return browser, context

def discover_centres_and_sales(page, data_type):
    """Enhanced centre and sale discovery with proper waiting"""
    print(f"  Discovering {data_type} centres and sales...")
    
    try:
        # Wait for page to stabilize
        page.wait_for_load_state("networkidle", timeout=DISCOVERY_TIMEOUT)
        random_delay(5, 10)
        
        # Discover centres
        centres = []
        if data_type != 'district_averages':
            centre_selector = "#cbocenter" if data_type == 'market_reports' else "#cbocentre"
            
            # Wait for centre options
            page.wait_for_function(f"""
                () => {{
                    const select = document.querySelector('{centre_selector}');
                    if (!select) return false;
                    const options = select.querySelectorAll('option');
                    return options.length > 1;
                }}
            """, timeout=DISCOVERY_TIMEOUT)
            
            centre_options = page.locator(f"{centre_selector} option").all()
            
            for opt in centre_options:
                label = opt.inner_text().strip()
                value = opt.get_attribute("value")
                
                if (value and value.strip() != "" and value != "0" and 
                    not any(word in label.lower() for word in ['select', 'choose', 'loading'])):
                    centres.append({
                        "label": label,
                        "value": value,
                        "folder_name": LOCATION_MAPPING.get(label.upper(), label.lower().replace(' ', '_'))
                    })
        else:
            # District averages doesn't have centres
            centres.append({"label": "District Averages", "value": "1", "folder_name": "district_averages"})
        
        print(f"    Found centres: {[c['label'] for c in centres]}")
        return centres
        
    except Exception as e:
        print(f"    Error discovering centres: {e}")
        return []

def get_sales_for_centre(page, centre, data_type):
    """Get available sales for a specific centre"""
    print(f"    Getting sales for {centre['label']}...")
    
    try:
        if data_type != 'district_averages':
            # Select centre first
            centre_selector = "#cbocenter" if data_type == 'market_reports' else "#cbocentre"
            page.select_option(centre_selector, value=centre['value'])
            random_delay(3, 8)
        
        # Wait for sales to load
        sale_selector = "#cbosale"
        page.wait_for_function(f"""
            () => {{
                const select = document.querySelector('{sale_selector}');
                if (!select) return false;
                const options = select.querySelectorAll('option');
                let realOptions = 0;
                for (let i = 1; i < options.length; i++) {{
                    const value = options[i].value;
                    if (value && value.trim() !== '') realOptions++;
                }}
                return realOptions > 0;
            }}
        """, timeout=120000)
        
        # Extract sales
        sale_options = page.locator(f"{sale_selector} option").all()
        sales = []
        
        for i, opt in enumerate(sale_options):
            if i == 0:  # Skip placeholder
                continue
                
            label = opt.inner_text().strip()
            value = opt.get_attribute("value")
            
            if value and value.strip() != "":
                match = re.search(r'(\d{1,2})', label)
                if match:
                    sale_number = match.group(1).zfill(2)
                    sales.append({
                        "label": label,
                        "value": value,
                        "sale_number": sale_number
                    })
        
        # Limit to latest 3 sales for efficiency
        sales = sales[:3]
        print(f"      Found {len(sales)} recent sales")
        return sales
        
    except Exception as e:
        print(f"      Error getting sales: {e}")
        return []

def extract_auction_lots_data(page, centre, sale):
    """Extract auction lots data"""
    print(f"      Extracting auction lots...")
    
    try:
        # Select centre and sale
        page.select_option("#cbocentre", value=centre['value'])
        random_delay(3, 6)
        page.select_option("#cbosale", value=sale['value'])
        random_delay(2, 4)
        
        # Trigger load
        page.click("#filter")
        
        # Wait for table
        page.wait_for_selector("#showdata table", timeout=MAX_TIMEOUT)
        random_delay(15, 25)  # Long wait for data to load
        
        # Extract data
        rows = page.locator("#showdata table tr:nth-child(n+2)").all()
        
        auction_data = []
        for row in rows[:2000]:  # Limit rows for performance
            try:
                cells = row.locator("td").all_text_contents()
                if len(cells) >= 6 and cells[0].strip():
                    auction_data.append({
                        "lot_no": cells[0].strip(),
                        "garden": cells[1].strip(),
                        "grade": cells[2].strip(),
                        "invoice": cells[3].strip(),
                        "packages": cells[4].strip(),
                        "price_inr": cells[5].strip()
                    })
            except:
                continue
        
        return {"auction_lots": auction_data}
        
    except Exception as e:
        print(f"        Error extracting auction lots: {e}")
        return {"auction_lots": []}

def extract_market_report_data(page, centre, sale, categories):
    """Extract market report data across categories"""
    print(f"      Extracting market reports...")
    
    all_reports = {}
    
    try:
        for category in categories[:3]:  # Limit categories
            print(f"        Category: {category['label']}")
            
            # Fresh page navigation
            page.goto(BASE_URLS['market_reports'], wait_until="networkidle")
            random_delay(3, 6)
            
            # Sequential selections
            page.select_option("#cbocenter", value=centre['value'])
            random_delay(3, 6)
            page.select_option("#cbocat", value=category['value'])  
            random_delay(3, 6)
            page.select_option("#cbosale", value=sale['value'])
            random_delay(2, 4)
            
            # Get report
            page.click("#refresh")
            page.wait_for_function("document.querySelector('#divmarketreport').innerText.length > 50", timeout=MAX_TIMEOUT)
            random_delay(10, 20)
            
            report_text = page.locator("#divmarketreport").inner_text()
            all_reports[category['label']] = report_text
    
    except Exception as e:
        print(f"        Error extracting market reports: {e}")
    
    return {"reports_by_category": all_reports}

def extract_synopsis_data(page, centre, sale):
    """Extract synopsis data"""
    print(f"      Extracting synopsis...")
    
    try:
        # Select centre and sale
        page.select_option("#cbocenter", value=centre['value'])
        random_delay(3, 6)
        page.select_option("#cbosale", value=sale['value'])
        random_delay(2, 4)
        
        # Handle new page opening
        with page.context.expect_page() as new_page_info:
            page.click("#refresh")
        
        synopsis_page = new_page_info.value
        synopsis_page.wait_for_load_state("networkidle", timeout=MAX_TIMEOUT)
        random_delay(15, 25)
        
        # Extract text content
        synopsis_text = synopsis_page.locator("body").inner_text()
        synopsis_page.close()
        
        return {"synopsis": synopsis_text}
        
    except Exception as e:
        print(f"        Error extracting synopsis: {e}")
        return {"synopsis": ""}

def save_enhanced_report(data, centre_folder, sale_number, year, data_type):
    """Save using current workflow naming"""
    OUTPUT_DIR = REPO_ROOT / "source_reports" / centre_folder
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Current naming convention
    filename_map = {
        'auction_lots': f"JT_auction_lots_enhanced_S{sale_number}.json",
        'market_reports': f"JT_market_report_enhanced_S{sale_number}.json", 
        'synopsis': f"JT_synopsis_enhanced_S{sale_number}.json"
    }
    
    filename = filename_map.get(data_type, f"JT_{data_type}_enhanced_S{sale_number}.json")
    output_path = OUTPUT_DIR / filename
    
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f"        Saved: {filename}")
        
        # Generate manifest
        generate_manifest(REPO_ROOT, centre_folder, f"S{sale_number}", currency="INR")
        
    except Exception as e:
        print(f"        Error saving: {e}")

def save_district_averages_as_supporting_data(data, sale_number, year):
    """Save district averages and copy to Indian auction centres"""
    # Save main file
    OUTPUT_DIR = REPO_ROOT / "source_reports" / "district_averages" 
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    filename = f"JT_district_average_enhanced_S{sale_number}.json"
    output_path = OUTPUT_DIR / filename
    
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        # Copy as supporting data to Indian centres
        indian_centres = ['guwahati', 'kolkata', 'siliguri']
        for centre in indian_centres:
            centre_dir = REPO_ROOT / "source_reports" / centre
            centre_dir.mkdir(parents=True, exist_ok=True)
            
            supporting_filename = f"JT_district_average_supporting_S{sale_number}.json"
            supporting_path = centre_dir / supporting_filename
            
            with open(supporting_path, 'w', encoding='utf-8') as f:
                json.dump({**data, "data_type": "supporting_district_averages"}, f, indent=2, ensure_ascii=False)
        
        print(f"      District averages saved + copied to {len(indian_centres)} centres")
        generate_manifest(REPO_ROOT, "district_averages", f"S{sale_number}", currency="INR")
        
    except Exception as e:
        print(f"      Error saving district averages: {e}")

def scrape_jthomas_enhanced():
    """Main enhanced J Thomas scraper"""
    print("Starting ENHANCED J Thomas Multi-Data Scraper...")
    print("Modes: auction_lots, market_reports, synopsis, district_averages\n")
    
    current_year = datetime.datetime.now().year
    
    with sync_playwright() as p:
        browser, context = create_stealth_context(p)
        
        try:
            # Process each data type
            for data_type, url in BASE_URLS.items():
                print(f"\n{'='*50}")
                print(f"PROCESSING {data_type.upper()}")
                print(f"{'='*50}")
                
                page = context.new_page()
                page.set_default_timeout(MAX_TIMEOUT)
                
                # Anti-detection
                page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")
                
                try:
                    page.goto(url, wait_until="networkidle", timeout=DISCOVERY_TIMEOUT)
                    
                    centres = discover_centres_and_sales(page, data_type)
                    
                    if not centres:
                        print(f"  No centres found for {data_type}")
                        continue
                    
                    # Process each centre
                    for centre in centres:
                        print(f"\n  Processing {centre['label']}...")
                        
                        try:
                            sales = get_sales_for_centre(page, centre, data_type)
                            
                            if not sales:
                                print(f"    No sales found for {centre['label']}")
                                continue
                            
                            # Process latest sale for each centre
                            latest_sale = sales[0]
                            sale_number = latest_sale['sale_number']
                            
                            print(f"    Processing Sale {sale_number}...")
                            
                            # Extract data based on type
                            extracted_data = {}
                            
                            if data_type == 'auction_lots':
                                extracted_data = extract_auction_lots_data(page, centre, latest_sale)
                            elif data_type == 'synopsis':
                                extracted_data = extract_synopsis_data(page, centre, latest_sale)
                            elif data_type == 'district_averages':
                                # District averages has simpler extraction
                                page.select_option("#cbosale", value=latest_sale['value'])
                                random_delay(2, 4)
                                
                                with page.context.expect_page() as new_page_info:
                                    page.click("#refresh")
                                
                                avg_page = new_page_info.value
                                avg_page.wait_for_load_state("networkidle", timeout=MAX_TIMEOUT)
                                random_delay(10, 20)
                                
                                avg_text = avg_page.locator("body").inner_text()
                                avg_page.close()
                                
                                extracted_data = {"district_averages": avg_text}
                            
                            # Create comprehensive output
                            output_data = {
                                "report_title": f"J Thomas {data_type.replace('_', ' ').title()} - {centre['label']} - Sale {sale_number}",
                                "centre": centre['label'],
                                "sale_number": sale_number,
                                "year": current_year,
                                "data_type": data_type,
                                "extraction_timestamp": datetime.datetime.now().isoformat(),
                                **extracted_data
                            }
                            
                            # Save using current workflow
                            if data_type == 'district_averages':
                                save_district_averages_as_supporting_data(output_data, sale_number, current_year)
                            else:
                                save_enhanced_report(output_data, centre['folder_name'], sale_number, current_year, data_type)
                        
                        except Exception as e:
                            print(f"    Error processing {centre['label']}: {e}")
                            continue
                        
                        # Human-like delay between centres
                        random_delay(8, 15)
                
                except Exception as e:
                    print(f"  Error processing {data_type}: {e}")
                finally:
                    page.close()
                
                # Delay between data types
                random_delay(10, 20)
        
        except Exception as e:
            print(f"Critical error: {e}")
        finally:
            browser.close()
    
    print("\n" + "="*50)
    print("Enhanced J Thomas scraping complete!")

if __name__ == "__main__":
    scrape_jthomas_enhanced()
