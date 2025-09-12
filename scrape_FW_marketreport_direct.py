import json
import datetime
import requests
from bs4 import BeautifulSoup
from pathlib import Path

# --- Standardized Configuration (Adapted for Proven Script) ---
REPO_ROOT = Path(__file__).resolve().parent
LOCATION = "colombo"
OUTPUT_DIR = REPO_ROOT / "source_reports" / LOCATION
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Direct URL from proven script
BASE_URL = "http://www.forbestea.com:9090/rpts/portal/report.jsp"

# --- Reference data for calculating the sale number (Restored) ---
REFERENCE_DATE = datetime.date(2025, 9, 1)
REFERENCE_SALE_NO = 34
# ----------------------------------------------------------------

def calculate_current_sale_info():
    """
    Calculates the current year and sale number based on the current date. (Restored Logic)
    """
    today = datetime.date.today()
    current_year = today.year
    
    # Calculate the most recent Monday
    days_since_monday = today.weekday()
    most_recent_monday = today - datetime.timedelta(days=days_since_monday)
    
    weeks_passed = (most_recent_monday - REFERENCE_DATE).days // 7
    
    current_sale_no = REFERENCE_SALE_NO + weeks_passed
    
    print(f"Today's Date: {today}")
    print(f"Most Recent Monday: {most_recent_monday}")
    print(f"Calculated Sale Number: {current_sale_no}")
    
    return current_year, current_sale_no

def scrape_forbes_direct():
    """
    Scrapes the Forbes Tea market report by constructing a direct URL
    based on the calculated current sale number. (Restored Logic)
    """
    print(f"--- Starting Direct Scraper for Forbes Tea Reports ---")

    try:
        # --- PART 1: Calculate URL ---
        print("[1/4] Calculating current sale number and constructing URL...")
        sale_year, sale_no = calculate_current_sale_info()
        
        target_url = f"{BASE_URL}?sale_year={sale_year}&sales_no={sale_no}"
        print(f"Constructed URL: {target_url}")

        # --- PART 2: Fetch Report ---
        print("[2/4] Fetching the report content...")
        # Timeout increased slightly for robustness
        response = requests.get(target_url, timeout=60)
        response.raise_for_status()
        
        # --- PART 3: Extract Text (Restored Logic) ---
        print("[3/4] Extracting text...")
        soup = BeautifulSoup(response.content, 'html.parser')
        
        if not soup.body:
             raise Exception("The response has no body content.")
        
        # Get all text from the body
        raw_text = soup.body.get_text(separator='\n', strip=True)

        if not raw_text.strip():
            raise Exception("The report page was blank.")

        # --- PART 4: Save Data to JSON (Standardized) ---
        print("[4/4] Saving data to JSON file...")
        report_title = f"Forbes Tea Market Report - Sale No {sale_no} ({sale_year})"
        
        output_data = {
            "report_title": report_title,
            "sale_number": sale_no,
            "source_url": target_url,
            "retrieval_date": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "raw_text": raw_text
        }

        # Standardized filename
        file_prefix = "FW_report_direct_parsed"
        sale_suffix = str(sale_no).zfill(2)
        output_filename = f"{file_prefix}_S{sale_suffix}.json"
        output_path = OUTPUT_DIR / output_filename

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=4, ensure_ascii=False)
        
        print(f"\n--- PROCESS COMPLETE ---")
        print(f"Successfully saved parsed data to: {output_path}")

    except requests.exceptions.RequestException as e:
        print(f"!!! An error occurred while fetching the URL: {e}")
        print("!!! This could be a 404 Not Found error if the report for this week is not yet published or a timeout.")
    except Exception as e:
        print(f"!!! An unexpected error occurred: {e}")

if __name__ == "__main__":
    scrape_forbes_direct()
