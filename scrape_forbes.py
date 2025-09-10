import os
import json
import datetime
import requests
from bs4 import BeautifulSoup

# --- Configuration ---
FINAL_OUTPUT_DIR = "source_reports/forbes_tea_reports"
BASE_URL = "http://www.forbestea.com:9090/rpts/portal/report.jsp"

# --- Reference data for calculating the sale number ---
REFERENCE_DATE = datetime.date(2025, 9, 1)
REFERENCE_SALE_NO = 34

def calculate_current_sale_info():
    """
    Calculates the current year and sale number based on the current date.
    """
    today = datetime.date.today()
    current_year = today.year
    
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
    based on the calculated current sale number.
    """
    print(f"--- Starting Direct Scraper for Forbes Tea Reports ---")
    os.makedirs(FINAL_OUTPUT_DIR, exist_ok=True)

    try:
        # --- PART 1: Calculate URL ---
        print("[1/4] Calculating current sale number and constructing URL...")
        sale_year, sale_no = calculate_current_sale_info()
        
        target_url = f"{BASE_URL}?sale_year={sale_year}&sales_no={sale_no}"
        print(f"Constructed URL: {target_url}")

        # --- PART 2: Fetch Report ---
        print("[2/4] Fetching the report content...")
        response = requests.get(target_url, timeout=30)
        response.raise_for_status()
        
        # --- PART 3: Extract Text ---
        print("[3/4] Extracting text...")
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # --- CORRECTED LOGIC: Get all text from the body, not a specific <pre> tag ---
        if not soup.body:
             raise Exception("The response has no body content.")
        
        # This is more robust and captures all text within the page's body
        raw_text = soup.body.get_text(separator='\n', strip=True)

        if not raw_text.strip():
            raise Exception("The report page was blank.")

        # --- PART 4: Save Data to JSON ---
        print("[4/4] Saving data to JSON file...")
        report_title = f"Forbes Tea Market Report - Sale No {sale_no} ({sale_year})"
        
        output_data = {
            "report_title": report_title,
            "source_url": target_url,
            "retrieval_date": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "raw_text": raw_text
        }

        date_str = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d')
        output_filename = f"ForbesTea_Sale-{sale_no}_{date_str}.json"
        output_path = os.path.join(FINAL_OUTPUT_DIR, output_filename)

        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=4, ensure_ascii=False)
        
        print(f"\n--- PROCESS COMPLETE ---")
        print(f"Successfully saved parsed data to: {output_path}")

    except requests.exceptions.RequestException as e:
        print(f"!!! An error occurred while fetching the URL: {e}")
        print("!!! This could be a 404 Not Found error if the report for this week is not yet published.")
    except Exception as e:
        print(f"!!! An unexpected error occurred: {e}")

if __name__ == "__main__":
    scrape_forbes_direct()
