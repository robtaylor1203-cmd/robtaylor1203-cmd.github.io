from playwright.sync_api import sync_playwright
import requests
import datetime
import os

# --- Configuration ---
LANDING_PAGE_URL = "https://www.tbeal.net/download-category/tbea-market-report-2025/"
OUTPUT_DIR = "source_reports/mombasa_raw_data"

def scrape_tbea_latest_report():
    """
    Uses a stealth Playwright approach to handle cookies and download the latest
    TBEA report document.
    """
    print(f"Starting STEALTH scraper for TBEA reports at {LANDING_PAGE_URL}...")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
    }

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        # Apply our proven manual stealth technique
        page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        try:
            print("Navigating to page and waiting for content to load...")
            page.goto(LANDING_PAGE_URL, wait_until="networkidle", timeout=60000)

            # --- Step 1: Handle the Cookie Banner ---
            cookie_accept_button = page.locator("button.cky-btn-accept")
            print("Looking for cookie consent banner...")
            try:
                # Wait for a short time to see if the banner appears
                cookie_accept_button.wait_for(timeout=5000)
                print("Cookie banner found. Clicking 'Accept All'...")
                cookie_accept_button.click()
                page.wait_for_timeout(2000) # Wait a moment for the banner to disappear
            except Exception:
                print("Cookie banner not found or already accepted. Continuing...")

            # --- Step 2: Find the Latest Download Link ---
            # Based on the debug HTML, the correct selector is 'a.wpdm-download-link'
            download_link_selector = "a.wpdm-download-link"
            print(f"Waiting for the download link ('{download_link_selector}') to be visible...")
            
            # Get the very first download link on the page, which is the latest report
            latest_report_link_tag = page.locator(download_link_selector).first
            latest_report_link_tag.wait_for()
            
            doc_url = latest_report_link_tag.get_attribute('href')
            if not doc_url:
                raise Exception("Found the link tag, but it has no 'href' with the download link.")

            print(f"Found the latest report link: {doc_url}")

            # --- Step 3: Download the document ---
            print("\nDownloading the document file...")
            response_doc = requests.get(doc_url, headers=headers, timeout=60)
            response_doc.raise_for_status()
            
            # Try to get the real filename from the download headers
            filename = f"tbea_report_{datetime.datetime.now().strftime('%Y-%m-%d')}.docx" # Default name
            if 'content-disposition' in response_doc.headers:
                disposition = response_doc.headers['content-disposition']
                try:
                    # Extracts the filename if it's present
                    filename = disposition.split('filename=')[1].strip('"')
                except IndexError:
                    print("Could not parse filename from header, using default.")

            output_path = os.path.join(OUTPUT_DIR, filename)
            
            with open(output_path, 'wb') as f:
                f.write(response_doc.content)

            print(f"Success! Report document saved to: {output_path}")

        except Exception as e:
            print(f"An error occurred: {e}")
            page.screenshot(path='error_screenshot_tbea.png')
            print("Saved an error screenshot to error_screenshot_tbea.png")
        finally:
            browser.close()

if __name__ == "__main__":
    scrape_tbea_latest_report()
