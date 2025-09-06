from playwright.sync_api import sync_playwright

URL = "https://jthomasindia.com/auction_prices.php"
OUTPUT_FILE = "debug_page_source.html"

def get_page_source():
    print(f"Attempting to get page source from {URL}...")
    with sync_playwright() as p:
        user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'

        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=user_agent)
        page = context.new_page()

        try:
            # Go to the page and wait for it to be fully settled
            page.goto(URL, wait_until="networkidle", timeout=60000)
            print("Page loaded. Getting HTML content...")

            # Get the entire HTML content of the page
            html_content = page.content()

            # Save the content to a file
            with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
                f.write(html_content)

            print(f"SUCCESS: The page's HTML has been saved to '{OUTPUT_FILE}'")

        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            browser.close()

if __name__ == "__main__":
    get_page_source()
