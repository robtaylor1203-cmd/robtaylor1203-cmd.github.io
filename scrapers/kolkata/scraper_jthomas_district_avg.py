from playwright.sync_api import sync_playwright
import datetime
import os
import json
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import fitz # PyMuPDF
import requests
from pathlib import Path

# --- Configuration ---
REPO_BASE = Path(__file__).resolve().parent.parent.parent
LANDING_PAGE_URL = "https://jthomasindia.com/district_average.php"
BASE_URL = "https://jthomasindia.com/"
TEMP_PDF_DIR = REPO_BASE / "temp_downloads"
RAW_DOWNLOAD_DIR = REPO_BASE / "raw_downloads" / "kolkata"
FINAL_OUTPUT_DIR = REPO_BASE / "source_reports" / "kolkata"

def process_jthomas_district_average():
    print(f"--- Starting J. Thomas District Average Process for {LANDING_PAGE_URL} ---")
    os.makedirs(TEMP_PDF_DIR, exist_ok=True)
    os.makedirs(FINAL_OUTPUT_DIR, exist_ok=True)
    os.makedirs(RAW_DOWNLOAD_DIR, exist_ok=True)
    
    downloaded_pdf_path = None
    latest_sale_no = "Unknown"
    sale_suffix = "SUnknown" # Initialize suffix

    # --- PART 1: SCRAPING (Using Playwright) ---
    print("\n[PART 1/2] Finding and downloading the latest PDF...")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        try:
            page.goto(LANDING_PAGE_URL, wait_until="networkidle", timeout=90000)

            print("Selecting the latest sale from the dropdown...")
            # The ID for the sale dropdown on this page is 'cbosale'
            SALE_SELECTOR = "#cbosale"
            
            # Wait robustly for options to attach
            page.wait_for_selector(f"{SALE_SELECTOR} option:nth-child(2)", state="attached", timeout=60000)
            
            # !!! THE FIX: Read the innerText (e.g., "36/2025") instead of the value (e.g., "343")
            latest_sale_text = page.eval_on_selector(SALE_SELECTOR, 
                "(select) => select.options[1].innerText")

            if latest_sale_text:
                # Parse the Sale No from the text
                try:
                    latest_sale_no = latest_sale_text.strip().split('/')[0]
                    # Ensure it's a valid number
                    int(latest_sale_no)
                    sale_suffix = f"S{str(latest_sale_no).zfill(2)}"
                    print(f"Latest Sale No identified: {latest_sale_no}")
                except (IndexError, ValueError):
                     print(f"Warning: Extracted sale text '{latest_sale_text}' is not a valid number.")
            else:
                 print("Warning: Could not determine sale number dynamically.")

            # Select the option (selection by index still works)
            page.select_option(SALE_SELECTOR, index=1)
            page.wait_for_timeout(2000)

            print("Clicking 'Refresh' and waiting for new page to open...")
            # The button ID on this page is 'refresh'
            with page.context.expect_page() as new_page_info:
                page.click("#refresh")

            report_page = new_page_info.value
            report_page.wait_for_load_state("networkidle")
            print(f"Switched to new report page: {report_page.url}")

            print("Searching for the embedded PDF link on the new page...")
            report_page_html = report_page.content()
            # Use lxml
            soup = BeautifulSoup(report_page_html, 'lxml')

            pdf_tag = soup.find('embed') or soup.find('iframe')
            if not pdf_tag: raise Exception("Could not find <embed> or <iframe> tag.")

            pdf_relative_url = pdf_tag.get('src')
            if not pdf_relative_url: raise Exception("Found tag but it has no 'src' link.")

            pdf_full_url = urljoin(BASE_URL, pdf_relative_url)
            print(f"Found PDF download link: {pdf_full_url}")

            # Download PDF using requests
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'}
            response_pdf = requests.get(pdf_full_url, headers=headers, timeout=90)
            response_pdf.raise_for_status()

            # Save PDF (using the correctly determined suffix)
            today = datetime.date.today().strftime("%Y%m%d")
            pdf_filename = f"JThomas_DistrictAverage_{sale_suffix}_{today}.pdf"
            
            raw_pdf_path = RAW_DOWNLOAD_DIR / pdf_filename
            downloaded_pdf_path = TEMP_PDF_DIR / pdf_filename

            # Save to raw archive and temp location
            with open(raw_pdf_path, 'wb') as f:
                f.write(response_pdf.content)
            with open(downloaded_pdf_path, 'wb') as f:
                f.write(response_pdf.content)
                
            print(f"Downloaded PDF to: {raw_pdf_path}")

        except Exception as e:
            print(f"!!! An error occurred during the scraping phase: {e}")
            page.screenshot(path=str(REPO_BASE / 'error_screenshot_dist_avg.png'))
            print("Saved an error screenshot.")
        finally:
            browser.close()

    # --- PART 2: PARSING (Using PyMuPDF) ---
    if downloaded_pdf_path and downloaded_pdf_path.exists():
        print("\n[PART 2/2] Parsing the downloaded PDF to extract text...")
        try:
            doc = fitz.open(downloaded_pdf_path)
            full_text = ""
            for page in doc:
                full_text += page.get_text()
            doc.close()

            # Wrap the raw text into the standardized format.
            output_data = [
                {
                    "type": "District Averages (Raw Text)",
                    "comment": full_text
                }
            ]

            # Standardized output filename (using the correctly determined suffix)
            output_filename = f"district_averages_{sale_suffix}.json"
            output_path = FINAL_OUTPUT_DIR / output_filename

            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=4, ensure_ascii=False)

            print(f"\n--- PROCESS COMPLETE ---")
            print(f"Successfully saved final parsed data to: {output_path}")

            # Clean up the temporary file
            try:
                os.remove(downloaded_pdf_path)
            except OSError as e:
                print(f"Warning: Could not remove temporary file: {e}")

        except Exception as e:
            print(f"!!! An error occurred during the parsing phase: {e}")

if __name__ == "__main__":
    process_jthomas_district_average()
