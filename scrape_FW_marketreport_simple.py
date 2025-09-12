import json
import datetime
import requests
from bs4 import BeautifulSoup
from pathlib import Path
import re
from urllib.parse import urljoin

# --- Standardized Configuration ---
REPO_ROOT = Path(__file__).resolve().parent
LOCATION = "colombo"
OUTPUT_DIR = REPO_ROOT / "source_reports" / LOCATION
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

BASE_URL = "http://www.forbestea.com" 
TARGET_URL = "http://www.forbestea.com/market-reports.asp" 
# ----------------------------------

def scrape_forbes_walker_simple():
    print(f"Starting Forbes & Walker (Simple) scraper for {LOCATION}...")
    extracted_data = {}
    sale_number = None

    try:
        # Increased timeout to 60 seconds for the HTTP request
        response = requests.get(TARGET_URL, timeout=60)
        response.raise_for_status() # Check for HTTP errors

        soup = BeautifulSoup(response.content, 'html.parser')

        # Find the main content area where reports are listed
        report_links = soup.find_all('a', href=True)
        
        latest_report_link = None
        latest_report_title = None

        # Iterate through links to find the most relevant market report
        for link in report_links:
            href = link['href']
            title = link.text.strip()
            
            # Look for links that seem to point to the weekly report 
            if ("Weekly Tea Market Report" in title or "Sale No" in title):
                
                # Construct the full URL
                full_url = urljoin(TARGET_URL, href)
                
                # Assuming the first one found is the latest
                latest_report_link = full_url
                latest_report_title = title
                break
        
        if latest_report_link:
            print(f"Found Report: {latest_report_title}")
            print(f"Link: {latest_report_link}")

            # Determine Sale Number from the title
            if latest_report_title:
                match = re.search(r'Sale No[:\s]*(\d{1,2})', latest_report_title, re.IGNORECASE)
                if match:
                    sale_number = match.group(1).zfill(2)

            if not sale_number:
                 raise ValueError(f"Could not determine Sale Number from title: {latest_report_title}")

            extracted_data = {
                "broker": "Forbes & Walker",
                "location": LOCATION,
                "report_title": latest_report_title,
                "sale_number": sale_number,
                "scrape_date": datetime.date.today().isoformat(),
                "source_url": TARGET_URL,
                "report_link": latest_report_link
            }
        else:
            print("Warning: Could not find the latest market report link.")

    except requests.exceptions.Timeout:
        print(f"Timeout Error: The website ({TARGET_URL}) did not respond in time.")
    except requests.exceptions.RequestException as e:
        print(f"HTTP Request failed: {e}")
    except Exception as e:
        print(f"An error occurred during scraping: {e}")

    # --- Standardized Saving Mechanism ---
    if extracted_data and sale_number:
        file_prefix = "FW_report_link_simple"
        filename = f"{file_prefix}_S{sale_number}.json"
        output_path = OUTPUT_DIR / filename

        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(extracted_data, f, ensure_ascii=False, indent=4)
            print(f"Successfully saved data to {output_path}")
        except Exception as e:
            print(f"Error saving file: {e}")
    else:
        print("Scraping finished without extracting data or determining sale number.")

if __name__ == "__main__":
    scrape_forbes_walker_simple()
