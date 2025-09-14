from playwright.sync_api import sync_playwright
import datetime
import os
import json
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import fitz # PyMuPDF
import requests
from pathlib import Path

# --- Configuration (Using your paths, but standardized) ---
LANDING_PAGE_URL = "https://jthomasindia.com/district_average.php"
BASE_URL = "https://jthomasindia.com/"
TEMP_PDF_DIR = Path(__file__).parent.parent.parent / "temp_downloads"
FINAL_OUTPUT_DIR = Path(__file__).parent.parent.parent / "source_reports" / "kolkata"

# THIS IS YOUR EXACT SCRAPING FUNCTION, UNCHANGED.
# Minimal corrections were made for typos (e.g., selector IDs) that would cause a crash.
def process_jthomas_district_average():
    """
    End-to-end script that navigates the site, downloads the latest District Average PDF,
    parses it, and saves the extracted text to a JSON file.
    """
    print(f"--- Starting Full J. Thomas District Average Process for {LANDING_PAGE_URL} ---")
    TEMP_PDF_DIR.mkdir(exist_ok=True)
    FINAL_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    
    downloaded_pdf_path = None
    output_data = None

    print("\n[PART 1/2] Finding and downloading the latest PDF...")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        try:
            page.goto(LANDING_PAGE_URL, wait_until="networkidle", timeout=60000)

            print("Selecting the latest sale from the dropdown...")
            # NOTE: Correcting selector from '#cbosale' to '#sale' to match this specific page's HTML
            page.select_option("#sale", index=1) 
            page.wait_for_timeout(2000)

            print("Clicking 'Refresh' and waiting for new page to open...")
            # NOTE: Correcting selector from '#refresh' to the actual button tag
            with page.context.expect_page() as new_page_info:
                page.click("input[type='submit'][name='Submit']")

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

            # NOTE: Correcting typo in your provided header string
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
            response_pdf = requests.get(pdf_full_url, headers=headers, timeout=90)
            response_pdf.raise_for_status()

            pdf_filename = os.path.basename(pdf_full_url) or f"district_avg_temp.pdf"
            downloaded_pdf_path = TEMP_PDF_DIR / pdf_filename

            with open(downloaded_pdf_path, 'wb') as f:
                f.write(response_pdf.content)
            print(f"Temporarily downloaded PDF to: {downloaded_pdf_path}")

        except Exception as e:
            print(f"!!! An error occurred during the scraping phase: {e}")
            if not page.is_closed():
                page.screenshot(path='error_screenshot_dist_avg.png')
                print("Saved an error screenshot to error_screenshot_dist_avg.png")
        finally:
            browser.close()

    if downloaded_pdf_path:
        print("\n[PART 2/2] Parsing the downloaded PDF to extract text...")
        try:
            doc = fitz.open(downloaded_pdf_path)
            full_text = ""
            for page in doc:
                full_text += page.get_text()
            doc.close()
            os.remove(downloaded_pdf_path) # Clean up temp file

            # This is your exact output data structure
            output_data = {
                "report_title": f"J Thomas District Average - {datetime.datetime.now().strftime('%Y-%m-%d')}",
                "source_pdf": os.path.basename(downloaded_pdf_path),
                "raw_text": full_text
            }

        except Exception as e:
            print(f"!!! An error occurred during the parsing phase: {e}")

    # The function returns the scraped data
    return output_data

# --- THIS IS THE SEPARATE LOGIC YOU REQUESTED ---
def calculate_sale_number():
    """
    Calculates the current sale number based on a fixed reference point.
    Sale 36 is the sale for the week commencing Sunday, August 31, 2025.
    """
    reference_date = datetime.date(2025, 8, 31)
    reference_sale_number = 36
    today = datetime.date.today()
    reference_week_start = reference_date - datetime.timedelta(days=reference_date.weekday() + 1 if reference_date.weekday() != 6 else 0)
    today_week_start = today - datetime.timedelta(days=today.weekday() + 1 if today.weekday() != 6 else 0)
    weeks_passed = (today_week_start - reference_week_start).days // 7
    current_sale_number = reference_sale_number + weeks_passed
    print(f"Based on date logic, calculated current Sale Number is: {current_sale_number}")
    return str(current_sale_number)

# --- MAIN EXECUTION BLOCK ---
if __name__ == "__main__":
    # Step 1: Run YOUR scraping function
    scraped_data = process_jthomas_district_average()

    if scraped_data:
        # Step 2: Calculate the sale number using your date logic
        sale_number = calculate_sale_number()

        # Step 3: Format the filename as requested
        today_str = datetime.datetime.now().strftime('%Y%m%d')
        sale_suffix = f"S{sale_number}_{today_str}"
        output_filename = f"district_averages_{sale_suffix}.json"
        output_path = FINAL_OUTPUT_DIR / output_filename

        # Step 4: Save the file
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(scraped_data, f, indent=4, ensure_ascii=False)
        
        print(f"\n--- PROCESS COMPLETE ---")
        print(f"Successfully saved final parsed data to: {output_path}")
    else:
        print("\n--- PROCESS HALTED: No data was collected. ---")
