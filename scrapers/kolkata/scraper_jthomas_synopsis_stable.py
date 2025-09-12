from playwright.sync_api import sync_playwright
import datetime
import os
import json
import re
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import fitz # PyMuPDF
import requests
from pathlib import Path

# --- Configuration ---
LANDING_PAGE_URL = "https://jthomasindia.com/market_synopsis.php"
BASE_URL = "https://jthomasindia.com/"

# --- CORRECTED Directory Setup ---
# Ensures the script can be run from any directory
SCRIPT_DIR = Path(__file__).parent
REPO_ROOT = SCRIPT_DIR.parent.parent
# This will be a temporary directory to hold the PDF during processing
TEMP_PDF_DIR = REPO_ROOT / "temp_downloads"
# This is where the final, clean JSON will be saved
FINAL_OUTPUT_DIR = REPO_ROOT / "source_reports" / "kolkata"


def process_jthomas_synopsis():
    """
    An end-to-end script that navigates the site, downloads the latest synopsis PDF,
    parses it, and saves the extracted text to a JSON file WITH the correct sale number.
    """
    print("--- Starting Full J. Thomas Synopsis Process (Stable Playwright Version) ---")
    TEMP_PDF_DIR.mkdir(exist_ok=True)
    FINAL_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    downloaded_pdf_path = None # Variable to hold the path of the downloaded PDF
    sale_number = None # Variable to hold the extracted sale number

    # --- PART 1: SCRAPING (Using Playwright) ---
    print("\n[PART 1/2] Finding and downloading the latest PDF...")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        try:
            page.goto(LANDING_PAGE_URL, wait_until="networkidle", timeout=60000)
            page.select_option("#cbocenter", label="KOLKATA")
            
            # Wait for the sale dropdown to have more than the default "Select Sale" option
            page.wait_for_function("document.querySelectorAll('#cbosale option').length > 1")
            
            # Get the text of the latest sale option (which is the second one, index 1)
            latest_sale_text = page.locator('#cbosale option').nth(1).inner_text()
            
            # Use regex to extract the sale number (e.g., from "36/2025")
            match = re.search(r'(\d+)/\d+', latest_sale_text)
            if match:
                sale_number = match.group(1)
                print(f"Successfully identified latest Sale No: {sale_number}")
            else:
                print("!!! WARNING: Could not extract sale number from dropdown. Filename will use date only.")

            # Select the latest sale option
            page.select_option("#cbosale", index=1)
            
            with page.context.expect_page() as new_page_info:
                page.click("#refresh")

            report_page = new_page_info.value
            report_page.wait_for_load_state("networkidle")

            report_page_html = report_page.content()
            soup = BeautifulSoup(report_page_html, 'lxml')

            pdf_tag = soup.find('embed') or soup.find('iframe')
            if not pdf_tag: raise Exception("Could not find <embed> or <iframe> tag on report page.")

            pdf_relative_url = pdf_tag.get('src')
            if not pdf_relative_url: raise Exception("Found tag but it has no 'src' link.")

            pdf_full_url = urljoin(BASE_URL, pdf_relative_url)
            print(f"Found PDF download link: {pdf_full_url}")

            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
            response_pdf = requests.get(pdf_full_url, headers=headers, timeout=90)
            response_pdf.raise_for_status()

            pdf_filename = f"synopsis_temp_{sale_number}.pdf" if sale_number else "synopsis_temp.pdf"
            downloaded_pdf_path = TEMP_PDF_DIR / pdf_filename

            with open(downloaded_pdf_path, 'wb') as f:
                f.write(response_pdf.content)
            print(f"Temporarily downloaded PDF to: {downloaded_pdf_path}")

        except Exception as e:
            print(f"!!! An error occurred during the scraping phase: {e}")
        finally:
            browser.close()

    # --- PART 2: PARSING (Using PyMuPDF) ---
    if downloaded_pdf_path and sale_number:
        print("\n[PART 2/2] Parsing the downloaded PDF and saving final JSON...")
        try:
            doc = fitz.open(downloaded_pdf_path)
            full_text = "".join(page.get_text() for page in doc)
            doc.close()

            # Create the final JSON object
            output_data = {
                "source": "JThomas",
                "type": "Market Synopsis",
                "sale_number": sale_number,
                "location": "Kolkata",
                "content": full_text
            }

            # Create the standardized final filename
            today_str = datetime.datetime.now().strftime('%Y%m%d')
            sale_suffix = f"S{sale_number}_{today_str}"
            output_filename = f"market_synopsis_{sale_suffix}.json"
            output_path = FINAL_OUTPUT_DIR / output_filename

            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=4, ensure_ascii=False)

            print(f"\n--- PROCESS COMPLETE ---")
            print(f"Successfully saved final parsed data to: {output_path}")
            
            # Clean up the temp file
            os.remove(downloaded_pdf_path)

        except Exception as e:
            print(f"!!! An error occurred during the parsing phase: {e}")
    else:
        print("\n--- PROCESS HALTED: Could not complete scraping, parsing step skipped. ---")


if __name__ == "__main__":
    process_jthomas_synopsis()
