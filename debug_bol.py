from playwright.sync_api import sync_playwright

URL = "https://jthomasindia.com/stats/BOL.php"
SCREENSHOT_FILE = "debug_bol_screenshot.png"
HTML_FILE = "debug_bol_source.html"

def capture_page_state():
    print(f"Starting DEBUG session for {URL}...")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")

        try:
            print("Navigating to page and waiting for it to settle...")
            page.goto(URL, wait_until="networkidle", timeout=60000)
            print("Page loaded.")

            page.screenshot(path=SCREENSHOT_FILE, full_page=True)
            print(f"SUCCESS: Screenshot saved to '{SCREENSHOT_FILE}'")

            html_content = page.content()
            with open(HTML_FILE, 'w', encoding='utf-8') as f:
                f.write(html_content)
            print(f"SUCCESS: Page HTML saved to '{HTML_FILE}'")

        except Exception as e:
            print(f"An error occurred during the debug capture: {e}")
        finally:
            browser.close()

if __name__ == "__main__":
    capture_page_state()
