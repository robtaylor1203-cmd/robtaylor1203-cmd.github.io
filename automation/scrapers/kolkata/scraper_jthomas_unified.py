import requests
import pypdf
import re
import json
import os
from datetime import datetime
from pathlib import Path
from bs4 import BeautifulSoup

# --- CONFIGURATION ---
BASE_URL_SYNOPSIS = "https://jthomasindia.com/market_synopsis.php"
BASE_URL_DISTRICT = "https://jthomasindia.com/district_average.php"
BASE_URL_ITERATIVE = "https://jthomasindia.com/market_report.php"
CENTRE_ID = "5" # ID for Kolkata

# --- DIRECTORY SETUP ---
SCRIPT_DIR = Path(__file__).parent
REPO_ROOT = SCRIPT_DIR.parent.parent
DOWNLOAD_DIR = REPO_ROOT / "raw_downloads" / "kolkata"
FINAL_OUTPUT_DIR = REPO_ROOT / "source_reports" / "kolkata"

DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
FINAL_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# --- HELPER FUNCTIONS ---
def generate_sale_suffix(sale_no, date_str):
    """Generates a standardized file suffix like S36_20250910."""
    return f"S{sale_no}_{date_str}"

def save_json(data, path):
    """Saves data to a JSON file with pretty printing."""
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    print(f"Successfully saved final parsed data to: {path}")

def parse_pdf_text(pdf_path):
    """Extracts all text from a given PDF file."""
    try:
        with open(pdf_path, 'rb') as f:
            reader = pypdf.PdfReader(f)
            text = "".join(page.extract_text() for page in reader.pages if page.extract_text())
        return text
    except Exception as e:
        print(f"!!! PDF Parsing Error for {pdf_path}: {e}")
        return None

# --- SCRAPER MODULES ---

def scrape_synopsis():
    """
    ULTRA-ROBUST VERSION: Finds the latest sale number by intelligently parsing all available
    Kolkata links, making it immune to text format changes.
    """
    print("\n--- Starting Full J. Thomas Synopsis Process ---")
    print("\n[PART 1/2] Finding and downloading the latest PDF...")
    try:
        session = requests.Session()
        response = session.get(BASE_URL_SYNOPSIS)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        sale_links = {}
        # Find all links on the page
        for link in soup.find_all('a', href=True):
            # We only care about links with 'Kolkata' in the text
            if 'Kolkata' in link.get_text(strip=True, separator=' '):
                # Use regex to find a sale number in the link text (e.g., "Sale No: 36/2025")
                match = re.search(r'Sale No:\s*(\d+)', link.get_text(strip=True, separator=' '))
                if match:
                    sale_number = int(match.group(1))
                    sale_links[sale_number] = link['href']
        
        if not sale_links:
            print("!!! FATAL ERROR: Could not find any valid Kolkata sale number links on the page.")
            return None, None

        # Find the highest sale number from the ones we found
        latest_sale_no = max(sale_links.keys())
        pdf_url = sale_links[latest_sale_no]
        
        print(f"Latest Sale No identified: {latest_sale_no}")
        print(f"Found PDF download link: {pdf_url}")

        pdf_response = session.get(pdf_url)
        pdf_response.raise_for_status()
        
        today_str = datetime.now().strftime('%Y%m%d')
        sale_suffix = generate_sale_suffix(latest_sale_no, today_str)
        downloaded_pdf_path = DOWNLOAD_DIR / f"JThomas_Synopsis_{sale_suffix}.pdf"
        
        with open(downloaded_pdf_path, 'wb') as f:
            f.write(pdf_response.content)
        print(f"Downloaded PDF to: {downloaded_pdf_path}")
        
        print("\n[PART 2/2] Parsing the downloaded PDF to extract text...")
        full_text = parse_pdf_text(downloaded_pdf_path)
        if not full_text:
            return None, None

        output_data = {
            "source": "JThomas", "type": "Market Synopsis", "sale_number": str(latest_sale_no),
            "location": "Kolkata", "content": full_text
        }
        
        output_path = FINAL_OUTPUT_DIR / f"market_synopsis_{sale_suffix}.json"
        save_json(output_data, output_path)
        print("\n--- SYNOPSIS PROCESS COMPLETE ---")
        
        os.remove(downloaded_pdf_path)
        return str(latest_sale_no), today_str

    except requests.RequestException as e:
        print(f"!!! HTTP Error during Synopsis scraping: {e}")
        return None, None
    except Exception as e:
        print(f"!!! An unexpected error occurred in scrape_synopsis: {e}")
        return None, None


def scrape_district_average(sale_no, date_str):
    """Scrapes the District Average PDF for a given sale number."""
    print(f"\n--- Starting J. Thomas District Average Process for Sale {sale_no} ---")
    print("\n[PART 1/2] Finding and downloading the PDF...")
    try:
        session = requests.Session()
        response = session.get(BASE_URL_DISTRICT)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        
        option = soup.find('option', string=re.compile(f"SALE {sale_no}"))
        if not option or not option.has_attr('value'):
             print(f"!!! ERROR: Could not find Sale {sale_no} in the district average dropdown.")
             return

        sale_value = option['value']
        print(f"Found sale value '{sale_value}' for Sale No {sale_no}")

        report_page_url = f"https://jthomasindia.com/district_avg_data.php?sale={sale_value}"
        print(f"Fetching report page: {report_page_url}")
        report_response = session.get(report_page_url)
        report_response.raise_for_status()

        pdf_match = re.search(r'href="(.*?\.pdf)"', report_response.text)
        if not pdf_match:
            print("!!! ERROR: Could not find the embedded PDF link on the report page.")
            return

        pdf_url = pdf_match.group(1)
        print(f"Found PDF download link: {pdf_url}")
        
        pdf_response = session.get(pdf_url)
        pdf_response.raise_for_status()
        
        sale_suffix = generate_sale_suffix(sale_no, date_str)
        downloaded_pdf_path = DOWNLOAD_DIR / f"JThomas_DistrictAverage_{sale_suffix}.pdf"
        
        with open(downloaded_pdf_path, 'wb') as f:
            f.write(pdf_response.content)
        print(f"Downloaded PDF to: {downloaded_pdf_path}")

        print("\n[PART 2/2] Parsing the downloaded PDF to extract text...")
        full_text = parse_pdf_text(downloaded_pdf_path)
        if not full_text:
            return

        output_data = {
            "source": "JThomas", "type": "District Averages", "sale_number": sale_no,
            "location": "Kolkata", "content": full_text
        }
        
        output_path = FINAL_OUTPUT_DIR / f"district_averages_{sale_suffix}.json"
        save_json(output_data, output_path)
        print("\n--- DISTRICT AVERAGE PROCESS COMPLETE ---")
        
        os.remove(downloaded_pdf_path)

    except requests.RequestException as e:
        print(f"!!! HTTP Error during District Average scraping: {e}")
    except Exception as e:
        print(f"!!! An unexpected error occurred in scrape_district_average: {e}")


def scrape_iterative_report(sale_no, date_str):
    """Scrapes the iterative market report commentary for all leaf types."""
    print(f"\n--- Starting J. Thomas Iterative Market Report for Sale {sale_no} ---")
    print("(Direct HTTP Simulation Mode)")
    
    leaf_types = ['CTC', 'DUST', 'ORTHODOX']
    commentaries = []
    
    try:
        session = requests.Session()
        initial_response = session.get(BASE_URL_ITERATIVE)
        initial_response.raise_for_status()
        soup = BeautifulSoup(initial_response.content, 'html.parser')

        option = soup.find('option', string=re.compile(f"SALE NO: {sale_no}"))
        if not option or not option.has_attr('value'):
            print(f"!!! ERROR: Could not find the internal value for Sale {sale_no} for iterative report.")
            return
        
        saleno_value = option['value']
        print(f"Found internal value '{saleno_value}' for Sale {sale_no}.")

        for leaf in leaf_types:
            print(f"\nFetching commentary for '{leaf}' leaf type...")
            payload = {'centcode': CENTRE_ID, 'saleno': saleno_value, 'leaf': leaf}
            response = session.post(BASE_URL_ITERATIVE, data=payload)
            response.raise_for_status()
            
            report_soup = BeautifulSoup(response.content, 'html.parser')
            table = report_soup.find('table', {'width': '750'})
            if table:
                text = table.get_text(separator='\n', strip=True)
                commentaries.append({"leaf_type": leaf, "commentary": text})
                print(f"Successfully extracted commentary for '{leaf}'.")
            else:
                print(f"Warning: Could not find report table for '{leaf}'.")
        
        if not commentaries:
            print("!!! PROCESSING FAILED: No commentaries were extracted.")
            return

        sale_suffix = generate_sale_suffix(sale_no, date_str)
        output_data = {
            "source": "JThomas", "type": "Market Report Commentary", "sale_number": sale_no,
            "location": "Kolkata", "content": commentaries
        }
        
        output_path = FINAL_OUTPUT_DIR / f"market_report_iterative_{sale_suffix}.json"
        save_json(output_data, output_path)
        print("\n--- ITERATIVE REPORT PROCESS COMPLETE ---")
        
    except requests.RequestException as e:
        print(f"!!! HTTP Error during Iterative Report scraping: {e}")
    except Exception as e:
        print(f"!!! An unexpected error occurred in scrape_iterative_report: {e}")


def main():
    """Main function to run all the scraping processes in order."""
    print("--- Starting J. Thomas Unified Scraper for Kolkata ---")
    
    latest_sale_no, today_str = scrape_synopsis()
    
    if latest_sale_no and today_str:
        scrape_district_average(latest_sale_no, today_str)
        scrape_iterative_report(latest_sale_no, today_str)
        print("\n\n--- ALL PROCESSES COMPLETED SUCCESSFULLY ---")
    else:
        print("\n\n--- SCRAPER HALTED: The initial synopsis step failed. ---")

if __name__ == "__main__":
    main()
