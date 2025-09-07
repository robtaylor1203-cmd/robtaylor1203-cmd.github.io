from playwright.sync_api import sync_playwright

URL = "https://web.forbestea.com/market-reports"
SCREENSHOT_FILE = "debug_forbes_screenshot.png"
HTML_FILE = "debug_forbes_source.html"

def capture_forbes_page_state():
    print(f"Starting DEBUG session for {URL}...")

    with sync_playwright() as p:
        user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'

        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=user_agent)
        page = context.new_page()

        try:
            print("Navigating to page and waiting for it to settle...")
            page.goto(URL, wait_until="networkidle", timeout=60000)
            print("Page loaded.")

            # Take a screenshot of the page
            page.screenshot(path=SCREENSHOT_FILE, full_page=True)
            print(f"SUCCESS: Screenshot saved to '{SCREENSHOT_FILE}'")

            # Save the HTML content of the page
            html_content = page.content()
            with open(HTML_FILE, 'w', encoding='utf-8') as f:
                f.write(html_content)
            print(f"SUCCESS: Page HTML saved to '{HTML_FILE}'")

        except Exception as e:
            print(f"An error occurred during the debug capture: {e}")
        finally:
            browser.close()

if __name__ == "__main__":
    capture_forbes_page_state()
