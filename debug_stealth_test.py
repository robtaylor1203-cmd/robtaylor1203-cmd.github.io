from playwright.sync_api import sync_playwright
from playwright_stealth import stealth_sync # Import the new tool

URL = "https://www.tbeal.net/download-category/tbea-market-report-2025/" # The TBEAL fortress
SCREENSHOT_FILE = "debug_stealth_screenshot.png"

def stealth_test():
    print(f"Starting STEALTH debug session for {URL}...")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        # Apply the stealth patches to the page
        stealth_sync(page)

        try:
            print("Navigating to page with stealth...")
            page.goto(URL, wait_until="networkidle", timeout=60000)
            print("Page loaded.")

            page.screenshot(path=SCREENSHOT_FILE, full_page=True)
            print(f"SUCCESS: Screenshot saved to '{SCREENSHOT_FILE}'")
            print("Please check the screenshot to see if the download links are visible.")

        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            browser.close()

if __name__ == "__main__":
    stealth_test()
