from playwright.sync_api import sync_playwright
import requests # We will still use this for a clean file download at the end
import datetime
import os

LANDING_PAGE_URL = "https://www.tbeal.net/download-category/tbea-market-report-2025/"
OUTPUT_DIR = "source_reports/mombasa_raw_data"

def scrape_tbea_latest_report_playwright():
    print(f"Starting Playwright scraper for TBEA reports at {LANDING_PAGE_URL}...")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
    }

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        try:
            print("Navigating to page and waiting for JavaScript to load content...")
            page.goto(LANDING_PAGE_URL, wait_until="networkidle", timeout=60000)

            # Define the selector for the first download button
            first_download_link_selector = "a.btn.btn-success"

            print(f"Waiting for the download link ('{first_download_link_selector}') to be visible...")
            page.wait_for_selector(first_download_link_selector, timeout=30000)
            
            # Get the first link element
            latest_report_link_tag = page.locator(first_download_link_selector).first
            
            doc_url = latest_report_link_tag.get_attribute('href')
            if not doc_url:
                raise Exception("Found the link tag, but it has no 'href' with the download link.")

            print(f"Found the latest report link: {doc_url}")

            # --- Download the document using the found URL ---
            print("\nDownloading the document file...")
            response_doc = requests.get(doc_url, headers=headers, timeout=60)
            response_doc.raise_for_status()
            
            if 'content-disposition' in response_doc.headers:
                disposition = response_doc.headers['content-disposition']
                filename = disposition.split('filename=')[1].strip('"')
            else:
                filename = f"tbea_report_{datetime.datetime.now().strftime('%Y-%m-%d')}.docx"

            output_path = os.path.join(OUTPUT_DIR, filename)
            
            with open(output_path, 'wb') as f:
                f.write(response_doc.content)

            print(f"Success! Report document saved to: {output_path}")

        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            browser.close()

if __name__ == "__main__":
    scrape_tbea_latest_report_playwright()
