from playwright.sync_api import sync_playwright
import requests
import datetime
import os
from urllib.parse import urljoin
from bs4 import BeautifulSoup

TARGET_URL = "https://jthomasindia.com/market_synopsis.php"
BASE_URL = "https://jthomasindia.com/"
OUTPUT_DIR = "source_reports/kolkata_synopsis_reports"

def scrape_jthomas_synopsis():
    print(f"Starting scraper for J. Thomas Market Synopsis at {TARGET_URL}...")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
    }

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        # Apply our manual stealth technique
        page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        try:
            print("Navigating to page and waiting for it to settle...")
            page.goto(TARGET_URL, wait_until="networkidle", timeout=60000)

            # --- Step 1: Interact with the form ---
            print("Selecting 'Kolkata'...")
            page.select_option("#cbocenter", label="KOLKATA")

            print("Waiting for sale numbers to load...")
            page.wait_for_function("document.querySelectorAll('#cbosale option').length > 1")

            print("Selecting latest sale...")
            page.select_option("#cbosale", index=1)
            
            # --- Step 2: Click 'Refresh' and catch the new page/tab ---
            print("Clicking 'Refresh' and waiting for new page to open...")
            with page.context.expect_page() as new_page_info:
                page.click("#refresh")
            
            report_page = new_page_info.value
            report_page.wait_for_load_state("networkidle")
            print(f"Switched to new report page: {report_page.url}")

            # --- Step 3: Find the PDF link on the new page ---
            print("Searching for the embedded PDF link on the new page...")
            report_page_html = report_page.content()
            soup = BeautifulSoup(report_page_html, 'lxml')

            # The PDF is usually inside an <embed> or <iframe> tag
            pdf_tag = soup.find('embed') or soup.find('iframe')
            if not pdf_tag:
                raise Exception("Could not find <embed> or <iframe> tag on the report page.")

            pdf_relative_url = pdf_tag.get('src')
            if not pdf_relative_url:
                raise Exception("Found tag, but it has no 'src' with the PDF link.")

            # Create the full, absolute URL for the PDF
            pdf_full_url = urljoin(BASE_URL, pdf_relative_url)
            print(f"Found final PDF download link: {pdf_full_url}")

            # --- Step 4: Download the final PDF file ---
            print("\nDownloading the PDF file...")
            response_pdf = requests.get(pdf_full_url, headers=headers, timeout=90)
            response_pdf.raise_for_status()

            pdf_filename = os.path.basename(pdf_full_url) or f"synopsis_{datetime.datetime.now().strftime('%Y-%m-%d')}.pdf"
            output_path = os.path.join(OUTPUT_DIR, pdf_filename)
            
            with open(output_path, 'wb') as f:
                f.write(response_pdf.content)

            print(f"Success! Synopsis PDF saved to: {output_path}")

        except Exception as e:
            print(f"!!! An error occurred: {e}")
            page.screenshot(path='error_screenshot_synopsis.png')
            print("Saved an error screenshot to error_screenshot_synopsis.png")
        finally:
            browser.close()
            print("Browser closed.")

if __name__ == "__main__":
    scrape_jthomas_synopsis()
