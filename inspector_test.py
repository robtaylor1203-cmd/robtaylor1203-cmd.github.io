from playwright.sync_api import sync_playwright
import time

URL = "https://www.atbltd.com/Docs/current_market_report"

with sync_playwright() as p:
    # We will force the browser to be visible
    browser = p.chromium.launch(headless=False)
    page = browser.new_page()

    print(f"Navigating to {URL}...")
    page.goto(URL)

    print("\n-----------------------------------------------------")
    print("Page is now open and paused for 5 minutes.")
    print("Please use the browser's Inspector to find the elements.")
    print("Press Ctrl+C in this terminal to end the script early.")
    print("-----------------------------------------------------")

    time.sleep(300) # Pause for 300 seconds (5 minutes)

    print("Pause finished. Closing browser.")
    browser.close()
