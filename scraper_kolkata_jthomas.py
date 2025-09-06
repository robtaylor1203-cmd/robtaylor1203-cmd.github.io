import json
from playwright.sync_api import sync_playwright
import datetime

URL = "https://jthomasindia.com/auction_prices.php"
OUTPUT_FILE = f"raw_data_kolkata_prices_{datetime.datetime.now().strftime('%Y-%m-%d')}.json"

def scrape_jthomas_prices():
    print(f"Starting scraper for {URL}...")
    
    scraped_data = []

    with sync_playwright() as p:
        user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
        
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(user_agent=user_agent)
        page = context.new_page()

        print("Navigating to page and waiting for it to settle...")
        page.goto(URL, wait_until="networkidle", timeout=60000)

        print("Page is fully loaded. Selecting 'Kolkata'...")
        centre_dropdown_selector = "#cbocentre" 
        kolkata_option_value = "5"
        page.select_option(centre_dropdown_selector, value=kolkata_option_value)
        
        print("Auction centre selected. Waiting for page to update...")
        page.wait_for_timeout(3000)

        print("Selecting the most recent sale number...")
        sale_dropdown_selector = "#cbosale"
        page.select_option(sale_dropdown_selector, index=1)
        page.wait_for_timeout(2000)

        print("Clicking 'SHOW PRICES' button...")
        show_prices_button_selector = "#filter"
        page.click(show_prices_button_selector)

        print("Waiting for the final data table to be visible...")
        # --- THE FINAL FIX ---
        # The results table is inside a div with id="showdata". This selector finds it.
        table_selector = "#showdata table"
        page.wait_for_selector(table_selector, timeout=30000) 
        
        print("Table is visible. Finding data rows...")
        # We need to find the rows within the results table
        # The old selector for rows is still correct, we just update the table part
        rows = page.locator(f"{table_selector} tr:nth-child(n+2)").all()
        
        print(f"Found {len(rows)} data rows. Extracting data...")

        if not rows:
            print("Warning: No data rows were found. The website structure may have changed.")
        else:
            for row in rows:
                cells = row.locator("td").all_text_contents()
                # The final table has 6 columns, not 5.
                if len(cells) >= 6:
                    scraped_data.append({
                        "lot_no": cells[0].strip(),
                        "garden": cells[1].strip(),
                        "grade": cells[2].strip(),
                        "invoice": cells[3].strip(),
                        "packages": cells[4].strip(),
                        "price": cells[5].strip()
                    })
        
        browser.close()

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(scraped_data, f, indent=4, ensure_ascii=False)
    
    print(f"Success! Scraped {len(scraped_data)} rows. Data saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    scrape_jthomas_prices()
