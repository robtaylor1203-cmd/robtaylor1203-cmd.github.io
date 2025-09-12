from playwright.sync_api import sync_playwright
import datetime
import json
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import fitz # PyMuPDF
import requests
from pathlib import Path
import re # Import regex library

# --- Standardized Configuration (Adapted for Proven Script) ---
REPO_ROOT = Path(__file__).resolve().parent
LOCATION = "kolkata"
OUTPUT_DIR = REPO_ROOT / "source_reports" / LOCATION
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Standardized temporary directory for PDF downloads
TEMP_PDF_DIR = REPO_ROOT / "temp_downloads"
TEMP_PDF_DIR.mkdir(parents=True, exist_ok=True)

LANDING_PAGE_URL = "https://jthomasindia.com/district_average.php"
BASE_URL = "https://jthomasindia.com/"
# --------------------------------------------------------------

def process_jthomas_district_average():
    print(f"--- Starting Full J. Thomas District Average Process for {LANDING_PAGE_URL} ---")
    
    downloaded_pdf_path = None
    sale_number = None # Initialize sale number

    # --- PART 1: SCRAPING (Using Playwright - Restored Logic) ---
    print("\n[PART 1/2] Finding and downloading the latest PDF...")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        # Stealth script restored
        page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        try:
            page.goto(LANDING_PAGE_URL, wait_until="networkidle", timeout=60000)

            # --- Simplified Interaction (Restored Logic) ---
            print("Selecting the latest sale from the dropdown...")
            sale_dropdown_selector = "#cbosale"
            
            # (FIX): Capture the Sale Number using the TEXT (Label) not the Value (ID)
            try:
                page.wait_for_selector(f"{sale_dropdown_selector} option:nth-child(2)", state="attached", timeout=10000)
                
                # Use the text/label of the option instead of the value attribute
                latest_sale_label = page.eval_on_selector(sale_dropdown_selector, 
                    "(select) => select.options[1].text")
                
                if latest_sale_label:
                    # Extract the number from the text (e.g., "36 (2025-26)")
                    match = re.search(r'(\d+)', latest_sale_label.strip())
                    if match:
                        sale_number = match.group(1)
                        print(f"Determined Sale Number: {sale_number}")
                    else:
                        print(f"Warning: Could not extract number from label: {latest_sale_label}")
                
            except Exception as e:
                print(f"Warning: Could not determine sale number dynamically: {e}")

            if not sale_number:
                 raise ValueError("Cannot proceed without determining the Sale Number.")

            page.select_option(sale_dropdown_selector, index=1) 
            page.wait_for_timeout(2000) # Fixed wait restored

            print("Clicking 'Refresh' and waiting for new page to open...")
            with page.context.expect_page() as new_page_info:
                page.click("#refresh")

            report_page = new_page_info.value
            report_page.wait_for_load_state("networkidle")
            print(f"Switched to new report page: {report_page.url}")

            print("Searching for the embedded PDF link on the new page...")
            report_page_html = report_page.content()
            soup = BeautifulSoup(report_page_html, 'lxml')

            pdf_tag = soup.find('embed') or soup.find('iframe')
            if not pdf_tag: raise Exception("Could not find <embed> or <iframe> tag on the report page.")

            pdf_relative_url = pdf_tag.get('src')
            if not pdf_relative_url: raise Exception("Found tag but it has no 'src' link.")

            pdf_full_url = urljoin(BASE_URL, pdf_relative_url)
            print(f"Found PDF download link: {pdf_full_url}")

            # Download using Requests
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
            response_pdf = requests.get(pdf_full_url, headers=headers, timeout=90)
            response_pdf.raise_for_status()

            pdf_filename = Path(pdf_full_url).name or f"district_avg_S{sale_number}.pdf"
            downloaded_pdf_path = TEMP_PDF_DIR / pdf_filename

            with open(downloaded_pdf_path, 'wb') as f:
                f.write(response_pdf.content)
            print(f"Temporarily downloaded PDF to: {downloaded_pdf_path}")

        except Exception as e:
            print(f"!!! An error occurred during the scraping phase: {e}")
            if 'page' in locals() and not page.is_closed():
                page.screenshot(path='error_screenshot_dist_avg.png')
                print("Saved an error screenshot to error_screenshot_dist_avg.png")
        finally:
            browser.close()

    # --- PART 2: PARSING (Using PyMuPDF) ---
    if downloaded_pdf_path and sale_number:
        print("\n[PART 2/2] Parsing the downloaded PDF to extract text...")
        try:
            doc = fitz.open(downloaded_pdf_path)
            full_text = ""
            for page in doc:
                full_text += page.get_text()
            doc.close()

            output_data = {
                "report_title": f"J Thomas District Average - Sale {sale_number}",
                "source_pdf": downloaded_pdf_path.name,
                "raw_text": full_text
            }

            # --- Standardized Saving Mechanism ---
            file_prefix = "JT_district_average_parsed"
            # Standardize the sale number format
            sale_suffix = str(sale_number).zfill(2)
            
            filename = f"{file_prefix}_S{sale_suffix}.json"
            output_path = OUTPUT_DIR / filename

            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=4, ensure_ascii=False)

            print(f"\n--- PROCESS COMPLETE ---")
            print(f"Successfully saved final parsed data to: {output_path}")

        except Exception as e:
            print(f"!!! An error occurred during the parsing phase: {e}")
    else:
        print("Process finished without successful download or sale number identification.")

if __name__ == "__main__":
    process_jthomas_district_average()
