from playwright.sync_api import sync_playwright
import datetime
import os
import json
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import fitz # PyMuPDF
import requests

# --- Configuration ---
LANDING_PAGE_URL = "https://jthomasindia.com/district_average.php"
BASE_URL = "https://jthomasindia.com/"
TEMP_PDF_DIR = "temp_downloads"
FINAL_OUTPUT_DIR = "source_reports/kolkata_district_average_reports"

def process_jthomas_district_average():
    """
    End-to-end script that navigates the site, downloads the latest District Average PDF,
    parses it, and saves the extracted text to a JSON file.
    """
    print(f"--- Starting Full J. Thomas District Average Process for {LANDING_PAGE_URL} ---")
    os.makedirs(TEMP_PDF_DIR, exist_ok=True)
    os.makedirs(FINAL_OUTPUT_DIR, exist_ok=True)
    
    downloaded_pdf_path = None

    # --- PART 1: SCRAPING (Using Playwright) ---
    print("\n[PART 1/2] Finding and downloading the latest PDF...")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        try:
            page.goto(LANDING_PAGE_URL, wait_until="networkidle", timeout=60000)
            
            # --- Simplified Interaction: Just one dropdown for the Sale number ---
            print("Selecting the latest sale from the dropdown...")
            # The ID for the sale dropdown on this page is 'cbosale'
            page.select_option("#cbosale", index=1) 
            page.wait_for_timeout(2000)

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

            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'}
            response_pdf = requests.get(pdf_full_url, headers=headers, timeout=90)
            response_pdf.raise_for_status()

            pdf_filename = os.path.basename(pdf_full_url) or f"district_avg_temp.pdf"
            downloaded_pdf_path = os.path.join(TEMP_PDF_DIR, pdf_filename)
            
            with open(downloaded_pdf_path, 'wb') as f:
                f.write(response_pdf.content)
            print(f"Temporarily downloaded PDF to: {downloaded_pdf_path}")

        except Exception as e:
            print(f"!!! An error occurred during the scraping phase: {e}")
            page.screenshot(path='error_screenshot_dist_avg.png')
            print("Saved an error screenshot to error_screenshot_dist_avg.png")
        finally:
            browser.close()

    # --- PART 2: PARSING (Using PyMuPDF) ---
    if downloaded_pdf_path:
        print("\n[PART 2/2] Parsing the downloaded PDF to extract text...")
        try:
            doc = fitz.open(downloaded_pdf_path)
            full_text = ""
            for page in doc:
                full_text += page.get_text()
            doc.close()
            
            output_data = {
                "report_title": f"J Thomas District Average - {datetime.datetime.now().strftime('%Y-%m-%d')}",
                "source_pdf": os.path.basename(downloaded_pdf_path),
                "raw_text": full_text
            }
            
            date_str = datetime.datetime.now().strftime('%Y-%m-%d')
            output_filename = f"JThomas_DistrictAverage_{date_str}.json"
            output_path = os.path.join(FINAL_OUTPUT_DIR, output_filename)

            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=4, ensure_ascii=False)
            
            print(f"\n--- PROCESS COMPLETE ---")
            print(f"Successfully saved final parsed data to: {output_path}")

        except Exception as e:
            print(f"!!! An error occurred during the parsing phase: {e}")

if __name__ == "__main__":
    process_jthomas_district_average()
