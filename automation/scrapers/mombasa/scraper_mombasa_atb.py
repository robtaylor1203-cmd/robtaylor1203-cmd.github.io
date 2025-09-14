from playwright.sync_api import sync_playwright # Corrected from sync_wright
import requests 
import datetime
import os

LANDING_PAGE_URL = "https://www.atbltd.com/Docs/current_market_report"
BASE_URL = "https://www.atbltd.com/Docs/"

OUTPUT_DIR = "source_reports/mombasa_raw_data"

def download_atb_multipage_report():
    print(f"Starting multi-page scraper for {LANDING_PAGE_URL}...")
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'}
    date_str = datetime.datetime.now().strftime('%Y-%m-%d')

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        print("Navigating to the landing page...")
        page.goto(LANDING_PAGE_URL, wait_until="networkidle")

        # --- PAGE 1 ---
        print("Processing Page 1...")
        page.wait_for_selector("img") 
        image_tag_1 = page.locator("img").first
        relative_url_1 = image_tag_1.get_attribute('src')
        full_image_url_1 = BASE_URL + relative_url_1

        print(f"Found Page 1 image link: {full_image_url_1}")
        response_1 = requests.get(full_image_url_1, headers=headers)

        output_path_1 = os.path.join(OUTPUT_DIR, f"atb_report_{date_str}_page_1.jpg")
        with open(output_path_1, 'wb') as f:
            f.write(response_1.content)
        print(f"Saved Page 1 image to {output_path_1}")

        # --- PAGE 2 ---
        print("\nProcessing Page 2...")
        next_button_selector = "input[type='submit'][value='Next']"
        if page.locator(next_button_selector).is_visible():
            print("Found 'Next' button. Clicking it...")
            page.click(next_button_selector)

            page.wait_for_timeout(5000)

            image_tag_2 = page.locator("img").first
            relative_url_2 = image_tag_2.get_attribute('src')

            # Check if the image source is different from page 1
            if relative_url_2 != relative_url_1:
                full_image_url_2 = BASE_URL + relative_url_2
                print(f"Found Page 2 image link: {full_image_url_2}")
                response_2 = requests.get(full_image_url_2, headers=headers)

                output_path_2 = os.path.join(OUTPUT_DIR, f"atb_report_{date_str}_page_2.jpg")
                with open(output_path_2, 'wb') as f:
                    f.write(response_2.content)
                print(f"Saved Page 2 image to {output_path_2}")
            else:
                print("Page 2 image is the same as Page 1. Assuming end of report.")

        else:
            print("Could not find a 'Next' button. Report is only one page.")

        browser.close()
    print("\nMulti-page scraping complete!")

if __name__ == "__main__":
    download_atb_multipage_report()
