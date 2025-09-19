#!/usr/bin/env python3
"""
Enhanced Forbes Walker Market Reports Scraper
Anti-bot measures + Dynamic content extraction + Current workflow compatibility
"""

import json
import datetime
import time
import random
import re
from playwright.sync_api import sync_playwright
from pathlib import Path
from pipeline_utils import generate_manifest

# --- Configuration ---
REPO_ROOT = Path(__file__).resolve().parent
BASE_URL = "http://www.forbestea.com:9090/rpts/portal/report.jsp"
LOCATION_FOLDER = "colombo"

# Enhanced timeouts for dynamic content
MAX_TIMEOUT = 600000  # 10 minutes
PAGE_LOAD_TIMEOUT = 180000  # 3 minutes
CONTENT_WAIT = 120000  # 2 minutes
STABILIZATION_WAIT = 30000  # 30 seconds

# Anti-bot user agents rotation
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0'
]

def random_delay(min_seconds=2, max_seconds=8):
    """Human-like random delays"""
    delay = random.uniform(min_seconds, max_seconds)
    print(f"  Human-like delay: {delay:.1f}s")
    time.sleep(delay)

def calculate_current_sale_info():
    """Calculate current year and sale number"""
    today = datetime.date.today()
    current_year = today.year
    
    # Calculate most recent Monday for sale number
    days_since_monday = today.weekday()
    most_recent_monday = today - datetime.timedelta(days=days_since_monday)
    
    # Reference calculation (adjust as needed)
    REFERENCE_DATE = datetime.date(2025, 1, 6)  # First Monday of 2025
    REFERENCE_SALE_NO = 1
    
    weeks_passed = (most_recent_monday - REFERENCE_DATE).days // 7
    current_sale_no = REFERENCE_SALE_NO + weeks_passed
    
    # Ensure reasonable bounds
    if current_sale_no < 1:
        current_sale_no = 1
    elif current_sale_no > 52:
        current_sale_no = 52
    
    print(f"Calculated Current Sale: {current_sale_no} ({current_year})")
    return current_year, current_sale_no

def get_all_available_sales():
    """Generate comprehensive list of sales to try"""
    current_year, current_sale_no = calculate_current_sale_info()
    
    sales_to_try = []
    
    # Current year sales (current and recent past)
    for sale_no in range(max(1, current_sale_no - 10), current_sale_no + 3):
        if 1 <= sale_no <= 52:
            sales_to_try.append({
                "year": current_year,
                "sale_number": sale_no,
                "sale_suffix": str(sale_no).zfill(2)
            })
    
    # Previous year end-of-year sales
    if current_year > 2024:
        prev_year = current_year - 1
        for sale_no in range(48, 53):
            sales_to_try.append({
                "year": prev_year,
                "sale_number": sale_no,
                "sale_suffix": str(sale_no).zfill(2)
            })
    
    # Sort by year and sale number (newest first)
    sales_to_try.sort(key=lambda x: (x["year"], x["sale_number"]), reverse=True)
    
    print(f"Will attempt {len(sales_to_try)} sales (showing first 5):")
    for sale in sales_to_try[:5]:
        print(f"  - Sale {sale['sale_number']} ({sale['year']})")
    
    return sales_to_try

def test_and_extract_sale_data(page, year, sale_number):
    """Test if sale is available and extract comprehensive data"""
    target_url = f"{BASE_URL}?sale_year={year}&sales_no={sale_number}"
    
    try:
        print(f"  Testing Sale {sale_number} ({year})...")
        
        # Navigate with human-like behavior
        page.goto(target_url, wait_until="networkidle", timeout=PAGE_LOAD_TIMEOUT)
        
        # Human-like stabilization wait
        random_delay(3, 7)
        
        # Check if page loaded successfully
        if not page.locator("body").count():
            return False, "No body content"
        
        # Wait for dynamic content to load
        print("  Waiting for content to stabilize...")
        page.wait_for_load_state("domcontentloaded", timeout=CONTENT_WAIT)
        page.wait_for_timeout(STABILIZATION_WAIT)
        
        # Extract comprehensive data
        print("  Extracting market data...")
        
        # Get all text content
        raw_text = page.locator("body").inner_text()
        
        if not raw_text or len(raw_text.strip()) < 100:
            return False, "Minimal content"
        
        # Enhanced data extraction - look for tables and structured data
        market_data = extract_structured_market_data(page, raw_text)
        
        # Check for error indicators
        error_indicators = ["error", "not found", "no data", "unavailable"]
        if any(indicator in raw_text.lower() for indicator in error_indicators):
            return False, "Error indicators found"
        
        return True, {
            "raw_text": raw_text,
            "structured_data": market_data,
            "content_length": len(raw_text),
            "extraction_timestamp": datetime.datetime.now().isoformat()
        }
        
    except Exception as e:
        return False, str(e)

def extract_structured_market_data(page, raw_text):
    """Extract structured market data from the page"""
    market_data = {
        "tables": [],
        "summary_stats": {},
        "lots_data": [],
        "price_data": []
    }
    
    try:
        # Look for tables
        tables = page.locator("table").all()
        print(f"  Found {len(tables)} tables on page")
        
        for i, table in enumerate(tables):
            try:
                table_text = table.inner_text()
                if table_text.strip():
                    # Try to extract table as structured data
                    rows = table.locator("tr").all()
                    table_data = []
                    
                    for row in rows:
                        cells = row.locator("td, th").all_text_contents()
                        if cells and any(cell.strip() for cell in cells):
                            table_data.append(cells)
                    
                    if table_data:
                        market_data["tables"].append({
                            "table_index": i,
                            "rows": table_data,
                            "text_content": table_text
                        })
            except:
                continue
        
        # Extract key metrics from text
        market_data["summary_stats"] = extract_summary_metrics(raw_text)
        
        # Look for lot numbers and prices
        lot_pattern = r'(?:LOT|Lot)\s*[#:]?\s*(\d+)'
        price_pattern = r'Rs\.?\s*(\d+[,\d]*\.?\d*)'
        
        lots = re.findall(lot_pattern, raw_text, re.IGNORECASE)
        prices = re.findall(price_pattern, raw_text, re.IGNORECASE)
        
        if lots:
            market_data["lots_data"] = lots[:50]  # First 50 lots
        if prices:
            market_data["price_data"] = prices[:50]  # First 50 prices
            
    except Exception as e:
        print(f"  Warning: Error extracting structured data: {e}")
    
    return market_data

def extract_summary_metrics(raw_text):
    """Extract key summary metrics from raw text"""
    metrics = {}
    
    try:
        # Total lots
        lots_patterns = [
            r'(\d+,?\d*)\s*lots?\s*offered',
            r'total\s*lots?[:\s]*(\d+,?\d*)',
            r'(\d+,?\d*)\s*lots?\s*catalogued'
        ]
        
        for pattern in lots_patterns:
            match = re.search(pattern, raw_text, re.IGNORECASE)
            if match:
                metrics['total_lots'] = match.group(1).replace(',', '')
                break
        
        # Total quantity
        qty_patterns = [
            r'(\d+,?\d*,?\d*)\s*kgs?\s*offered',
            r'total\s*quantity[:\s]*(\d+,?\d*,?\d*)\s*kgs?',
            r'(\d+,?\d*,?\d*)\s*kgs?\s*catalogued'
        ]
        
        for pattern in qty_patterns:
            match = re.search(pattern, raw_text, re.IGNORECASE)
            if match:
                metrics['total_quantity'] = match.group(1).replace(',', '')
                break
        
        # Average price
        avg_patterns = [
            r'average[:\s]*Rs\.?\s*(\d+,?\d*\.?\d*)',
            r'Rs\.?\s*(\d+,?\d*\.?\d*)\s*average',
            r'overall\s*average[:\s]*Rs\.?\s*(\d+,?\d*\.?\d*)'
        ]
        
        for pattern in avg_patterns:
            match = re.search(pattern, raw_text, re.IGNORECASE)
            if match:
                metrics['average_price'] = match.group(1).replace(',', '')
                break
        
        # Highest price
        high_patterns = [
            r'highest[:\s]*Rs\.?\s*(\d+,?\d*\.?\d*)',
            r'Rs\.?\s*(\d+,?\d*\.?\d*)\s*highest',
            r'top\s*price[:\s]*Rs\.?\s*(\d+,?\d*\.?\d*)'
        ]
        
        for pattern in high_patterns:
            match = re.search(pattern, raw_text, re.IGNORECASE)
            if match:
                metrics['highest_price'] = match.group(1).replace(',', '')
                break
    
    except Exception as e:
        print(f"  Warning: Error extracting metrics: {e}")
    
    return metrics

def create_stealth_browser_context(p):
    """Create anti-bot browser context"""
    user_agent = random.choice(USER_AGENTS)
    
    # Randomized viewport sizes
    viewports = [
        {'width': 1920, 'height': 1080},
        {'width': 1366, 'height': 768},
        {'width': 1536, 'height': 864},
        {'width': 1440, 'height': 900}
    ]
    viewport = random.choice(viewports)
    
    browser = p.chromium.launch(
        headless=True,
        timeout=MAX_TIMEOUT,
        args=[
            '--no-sandbox',
            '--disable-blink-features=AutomationControlled',
            '--disable-web-security',
            '--disable-features=VizDisplayCompositor'
        ]
    )
    
    context = browser.new_context(
        user_agent=user_agent,
        viewport=viewport,
        locale='en-US',
        timezone_id='America/New_York',
        extra_http_headers={
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
    )
    
    return browser, context

def scrape_forbes_walker_enhanced():
    """Main enhanced scraping function"""
    print("Starting ENHANCED Forbes Walker Market Reports scraper...")
    print("Anti-bot measures: ✓ Stealth browser ✓ Random delays ✓ Human-like behavior\n")
    
    with sync_playwright() as p:
        browser, context = create_stealth_browser_context(p)
        
        try:
            page = context.new_page()
            page.set_default_timeout(MAX_TIMEOUT)
            
            # Enhanced anti-detection
            page.add_init_script("""
                // Remove webdriver property
                Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                
                // Mock plugins
                Object.defineProperty(navigator, 'plugins', {
                    get: () => [1, 2, 3, 4, 5].map(() => ({name: 'Chrome PDF Plugin'})),
                });
                
                // Mock languages
                Object.defineProperty(navigator, 'languages', {
                    get: () => ['en-US', 'en']
                });
            """)
            
            # Get all potential sales
            potential_sales = get_all_available_sales()
            
            if not potential_sales:
                print("Error: Could not generate sales list")
                return
            
            print(f"\nTesting {len(potential_sales)} potential sales...")
            
            available_sales = []
            
            # Test each sale with human-like delays
            for i, sale in enumerate(potential_sales):
                year = sale['year']
                sale_number = sale['sale_number']
                
                # Human-like delay between requests
                if i > 0:
                    random_delay(5, 12)
                
                is_available, data_or_error = test_and_extract_sale_data(page, year, sale_number)
                
                if is_available:
                    print(f"  ✓ Sale {sale_number} ({year}) - Available")
                    sale['extracted_data'] = data_or_error
                    available_sales.append(sale)
                else:
                    print(f"  ✗ Sale {sale_number} ({year}) - {data_or_error}")
            
            if not available_sales:
                print("No available Forbes Walker reports found")
                return
            
            print(f"\nProcessing {len(available_sales)} available sales...")
            
            # Process each available sale
            for sale in available_sales:
                year = sale['year']
                sale_number = sale['sale_number']
                sale_suffix = sale['sale_suffix']
                extracted_data = sale['extracted_data']
                
                print(f"\nProcessing Sale {sale_number} ({year})...")
                
                # Create comprehensive output data
                output_data = {
                    "report_title": f"Forbes Walker Market Report - Sale {sale_number} ({year})",
                    "sale_number": sale_number,
                    "year": year,
                    "source_url": f"{BASE_URL}?sale_year={year}&sales_no={sale_number}",
                    "retrieval_date": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    "content_length": extracted_data['content_length'],
                    "raw_text": extracted_data['raw_text'],
                    "structured_data": extracted_data['structured_data'],
                    "extraction_method": "Enhanced Playwright with Anti-bot measures"
                }
                
                # Save using current naming convention
                save_enhanced_report(output_data, sale_suffix, year)
                
                print(f"✓ Sale {sale_number} ({year}) processed successfully")
        
        except Exception as e:
            print(f"Critical error during scraping: {e}")
        finally:
            browser.close()

def save_enhanced_report(output_data, sale_suffix, year):
    """Save report using current workflow naming convention"""
    OUTPUT_DIR = REPO_ROOT / "source_reports" / LOCATION_FOLDER
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    # Use current naming: FW_report_direct_parsed_S37_2025.json
    filename = f"FW_report_direct_parsed_S{sale_suffix}_{year}.json"
    output_path = OUTPUT_DIR / filename
    
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
        
        print(f"  Saved: {output_path}")
        
        # Generate manifest using current system
        generate_manifest(REPO_ROOT, LOCATION_FOLDER, f"{sale_suffix}_{year}", currency="LKR")
        
    except Exception as e:
        print(f"  Error saving {filename}: {e}")

if __name__ == "__main__":
    scrape_forbes_walker_enhanced()
