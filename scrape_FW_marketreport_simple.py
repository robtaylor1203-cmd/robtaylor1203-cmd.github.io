import json
import datetime
import requests
from bs4 import BeautifulSoup
from pathlib import Path
from pipeline_utils import generate_manifest

# --- Configuration ---
REPO_ROOT = Path(__file__).resolve().parent
BASE_URL = "http://www.forbestea.com:9090/rpts/portal/report.jsp"

# Enhanced timeouts and settings
REQUEST_TIMEOUT = 120  # 2 minutes for HTTP requests
RETRY_ATTEMPTS = 5    # Increased retry attempts
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'

# Forbes Tea is single location (Colombo)
LOCATION_FOLDER = "colombo"

# Reference data for calculating the sale number
REFERENCE_DATE = datetime.date(2025, 9, 1)
REFERENCE_SALE_NO = 34

def calculate_current_sale_info():
    """Calculates the current year and sale number based on the current date."""
    today = datetime.date.today()
    current_year = today.year
    
    days_since_monday = today.weekday()
    most_recent_monday = today - datetime.timedelta(days=days_since_monday)
    
    weeks_passed = (most_recent_monday - REFERENCE_DATE).days // 7
    
    if weeks_passed < 0:
        print("Warning: Reference date is in the future.")
        return current_year, REFERENCE_SALE_NO 

    current_sale_no = REFERENCE_SALE_NO + weeks_passed
    print(f"Calculated Current Sale Number: {current_sale_no}")
    
    return current_year, current_sale_no

def get_all_available_sales():
    """Helper to discover all available Forbes Tea sales."""
    print("Discovering all available Forbes Tea sales...")
    
    current_year, current_sale_no = calculate_current_sale_info()
    
    max_lookback = 30  # Look back up to 30 weeks
    min_sale = max(1, current_sale_no - max_lookback)
    max_sale = current_sale_no + 5  # Include future sales in case calculation is off
    
    sales_to_try = []
    
    # Current year sales
    for sale_no in range(min_sale, max_sale + 1):
        sales_to_try.append({
            "year": current_year,
            "sale_number": sale_no,
            "sale_suffix": str(sale_no).zfill(2)
        })
    
    # Previous year sales (last few weeks)
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
    
    print(f"Will attempt to discover {len(sales_to_try)} potential sales")
    return sales_to_try

def test_sale_availability(year, sale_number):
    """Test if a specific sale report is available with enhanced retry logic."""
    target_url = f"{BASE_URL}?sale_year={year}&sales_no={sale_number}"
    
    for attempt in range(RETRY_ATTEMPTS):
        try:
            headers = {
                'User-Agent': USER_AGENT,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1'
            }
            
            response = requests.get(target_url, timeout=REQUEST_TIMEOUT, headers=headers)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            if not soup.body:
                return False, "No body content"
            
            raw_text = soup.body.get_text(separator='\n', strip=True)
            
            if not raw_text.strip():
                return False, "Empty report content"
            
            # Check for error indicators
            if ("error" in raw_text.lower() or 
                "not found" in raw_text.lower() or 
                "404" in raw_text or 
                len(raw_text) < 100):
                return False, "Error or minimal content"
            
            return True, raw_text
            
        except requests.exceptions.Timeout:
            print(f"    Timeout on attempt {attempt + 1}/{RETRY_ATTEMPTS}")
            if attempt < RETRY_ATTEMPTS - 1:
                import time
                time.sleep(2 ** attempt)  # Exponential backoff
                continue
            return False, "Request timeout after retries"
        except requests.exceptions.RequestException as e:
            if attempt < RETRY_ATTEMPTS - 1:
                print(f"    Request error on attempt {attempt + 1}/{RETRY_ATTEMPTS}: {e}")
                import time
                time.sleep(2)
                continue
            return False, str(e)
    
    return False, "Max retries exceeded"

def scrape_forbes_tea_all_sales():
    print(f"Starting Forbes Tea Market Reports scraper - ALL SALES (Enhanced Version)")
    print("\nIMPORTANT: This will discover and process ALL available Forbes Tea reports.\n")

    try:
        potential_sales = get_all_available_sales()
        
        if not potential_sales:
            print("Warning: Could not generate list of potential sales.")
            return

        print(f"\n{'='*20} Testing Sales Availability {'='*20}")
        
        available_sales = []
        for i, sale in enumerate(potential_sales):
            year = sale['year']
            sale_number = sale['sale_number']
            sale_suffix = sale['sale_suffix']
            
            print(f"Testing Sale {sale_number} ({year})... ", end="", flush=True)
            
            is_available, content_or_error = test_sale_availability(year, sale_number)
            
            if is_available:
                print("✓ Available")
                sale['raw_text'] = content_or_error
                available_sales.append(sale)
            else:
                print(f"✗ Not available ({content_or_error})")
            
            # Small delay to be respectful to server
            if i < len(potential_sales) - 1:
                import time
                time.sleep(1)

        if not available_sales:
            print("Warning: No available Forbes Tea reports found.")
            return

        print(f"\n{'='*20} Processing Available Sales {'='*20}")
        print(f"Found {len(available_sales)} available Forbes Tea reports. Processing all...")

        for i, sale in enumerate(available_sales):
            year = sale['year']
            sale_number = sale['sale_number']
            sale_suffix = sale['sale_suffix']
            raw_text = sale['raw_text']
            
            print(f"\n--- Processing Sale {i+1}/{len(available_sales)}: Sale {sale_number} ({year}) ---")
            process_single_forbes_sale(year, sale_number, sale_suffix, raw_text)

    except Exception as e:
        print(f"Critical error occurred: {e}")

def process_single_forbes_sale(year, sale_number, sale_suffix, raw_text):
    """Handles the processing logic for one specific Forbes Tea sale."""
    
    try:
        source_url = f"{BASE_URL}?sale_year={year}&sales_no={sale_number}"
        report_title = f"Forbes Tea Market Report - Sale No {sale_number} ({year})"
        
        output_data = {
            "report_title": report_title,
            "sale_number": sale_number,
            "year": year,
            "source_url": source_url,
            "retrieval_date": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "content_length": len(raw_text),
            "raw_text": raw_text
        }

        print(f"Extracted {len(raw_text)} characters of content for Sale {sale_number} ({year})")

    except Exception as e:
        print(f"\nERROR: Failed during Forbes Tea processing for Sale {sale_number} ({year}). Details: {e}")
        return

    if output_data and raw_text.strip():
        OUTPUT_DIR = REPO_ROOT / "source_reports" / LOCATION_FOLDER
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        filename = f"FW_report_enhanced_S{sale_suffix}_{year}.json"
        output_path = OUTPUT_DIR / filename

        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=4, ensure_ascii=False)
            
            print(f"SUCCESS! Forbes Tea report processed for Sale {sale_number} ({year}).")
            print(f"Successfully saved report data to {output_path}")
            
            generate_manifest(REPO_ROOT, LOCATION_FOLDER, f"{sale_suffix}_{year}", currency="LKR")

        except Exception as e:
            print(f"Error saving file or generating manifest: {e}")
    else:
        print(f"Forbes Tea processing finished for Sale {sale_number} ({year}) without valid data.")

def scrape_forbes_direct_current():
    """Legacy function to scrape only the current sale."""
    print(f"--- Starting Direct Scraper for Current Forbes Tea Report ---")

    try:
        sale_year, sale_no = calculate_current_sale_info()
        target_url = f"{BASE_URL}?sale_year={sale_year}&sales_no={sale_no}"
        print(f"Constructed URL: {target_url}")

        print("Fetching the current report content...")
        
        headers = {
            'User-Agent': USER_AGENT,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
        }
        
        response = requests.get(target_url, timeout=REQUEST_TIMEOUT, headers=headers)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        if not soup.body:
             raise Exception("The response has no body content.")
        
        raw_text = soup.body.get_text(separator='\n', strip=True)

        if not raw_text.strip():
            print("The report page was blank. Assuming report not yet published.")
            return

        sale_suffix = str(sale_no).zfill(2)
        process_single_forbes_sale(sale_year, sale_no, sale_suffix, raw_text)

    except requests.exceptions.RequestException as e:
        print(f"An error occurred while fetching the URL: {e}")
        print("This could be a 404 Not Found error if the report is not yet published or a timeout.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    # Run the comprehensive ALL SALES version
    scrape_forbes_tea_all_sales()
    
    # Uncomment the line below if you want to run only the current sale version instead
    # scrape_forbes_direct_current()
