import asyncio
from playwright.async_api import async_playwright
from pathlib import Path
import json
import datetime
import re # Import regular expressions

# Define the base directory and output directory
REPO_BASE = Path(__file__).resolve().parent.parent.parent
# This script targets the summary table, so output goes to source_reports
OUTPUT_DIR = REPO_BASE / "source_reports" / "kolkata"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

TARGET_URL = "https://jthomasindia.com/auction_prices.php"

async def scrape_jthomas_prices():
    print(f"Starting J Thomas Auction Prices (Summary) scraper...")
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
    
    async with async_playwright() as p:
        # Launch the browser (headless=True runs in background)
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent=user_agent)
        page = await context.new_page()

        try:
            print(f"Navigating to {TARGET_URL}...")
            await page.goto(TARGET_URL, timeout=60000)

            # --- 1. Form Interaction ---
            SALE_NO_SELECTOR = 'select[name="Sale_no"]'
            YEAR_SELECTOR = 'select[name="Year"]'
            VIEW_BUTTON_SELECTOR = 'input[type="submit"][value="View"]'

            # Get the latest available Sale No.
            # THE FIX: Doubled {{ }} used here to escape braces in the f-string
            latest_sale_no = await page.eval_on_selector(SALE_NO_SELECTOR, 
                """(select) => {
                    const options = Array.from(select.options);
                    // Filter for options that are purely digits
                    const validOptions = options.filter(opt => /^\d+$/.test(opt.value));
                    // Return the last valid option (assuming they are sorted)
                    return validOptions.length > 0 ? validOptions[validOptions.length - 1].value : null;
                }""")
            
            if not latest_sale_no:
                raise Exception("Could not determine the latest Sale Number.")

            # For the year, we use the current year (Sept 2025)
            current_year = str(datetime.date.today().year)

            # Fallback: Ensure the selected year is valid in the dropdown
            try:
                # Check if the option exists before selecting
                await page.wait_for_selector(f'{YEAR_SELECTOR} option[value="{current_year}"]', timeout=5000)
                await page.select_option(YEAR_SELECTOR, current_year)
            except Exception:
                print(f"Warning: Year {current_year} not available, defaulting to latest available year.")
                current_year = await page.eval_on_selector(YEAR_SELECTOR, 
                    "(select) => select.options[select.options.length - 1].value")
                await page.select_option(YEAR_SELECTOR, current_year)


            print(f"Selecting Sale No: {latest_sale_no}, Year: {current_year}...")

            # Select the Sale No
            await page.select_option(SALE_NO_SELECTOR, latest_sale_no)
            

            # Click the View button and wait for the navigation (this avoids the AJAX timeout issue)
            async with page.expect_navigation(wait_until="domcontentloaded", timeout=45000):
                await page.click(VIEW_BUTTON_SELECTOR)

            # --- 2. Data Extraction from HTML Table ---
            print("Extracting data from the results table...")
            
            # The results table is typically the 4th table on the page.
            TABLE_SELECTOR = "table:nth-of-type(4)"
            await page.wait_for_selector(TABLE_SELECTOR, timeout=10000)

            # Use page.evaluate to scrape the table.
            # THE FIX: Doubled {{ }} used here to escape braces in the f-string
            data = await page.evaluate(f"""(selector) => {{
                const table = document.querySelector(selector);
                if (!table) return null;

                const rows = Array.from(table.querySelectorAll('tr'));
                const results = [];

                // Start iterating from the second row (index 1)
                for (let i = 1; i < rows.length; i++) {{
                    const cols = rows[i].querySelectorAll('td');
                    if (cols.length >= 6) {{
                        const entry = {{
                            grade: cols[0].innerText.trim(),
                            ctc_this_week_inr: cols[1].innerText.trim(),
                            ctc_last_week_inr: cols[2].innerText.trim(),
                            orth_this_week_inr: cols[3].innerText.trim(),
                            orth_last_week_inr: cols[4].innerText.trim(),
                            darj_this_week_inr: cols[5].innerText.trim(),
                            darj_last_week_inr: cols[6] ? cols[6].innerText.trim() : ""
                        }};
                        // Basic filter to skip headers or empty rows
                        if (entry.grade && entry.grade !== 'Grade') {{
                            results.push(entry);
                        }}
                    }}
                }}
                return results;
            }}""", TABLE_SELECTOR)

            if not data:
                raise Exception("Could not find or extract data from the results table.")

            print(f"Successfully extracted {len(data)} records.")

            # --- 3. Save Data ---
            # zfill(2) ensures two digits (e.g., S09 instead of S9)
            # We rename the output file to distinguish it from the detailed lot results
            output_filename = f"auction_prices_summary_S{latest_sale_no.zfill(2)}.json"
            save_path = OUTPUT_DIR / output_filename

            with open(save_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)

            print(f"\nSUCCESS: Data saved to {save_path}")
            print(f"REMINDER: Create the manifest.json for Sale {latest_sale_no} in the 'kolkata' folder and run consolidation.")

        except Exception as e:
            print(f"\nERROR: Failed during scraping J Thomas. Details: {e}")
            error_screenshot = REPO_BASE / "error_screenshot_jthomas.png"
            await page.screenshot(path=error_screenshot)
            print(f"Saved error screenshot to {error_screenshot}")
        
        finally:
            await browser.close()
            print("Browser closed.")

if __name__ == "__main__":
    # Check if an event loop is already running (common in some environments)
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        # If running in an environment with an active loop (like Jupyter)
        task = loop.create_task(scrape_jthomas_prices())
        # In a script, we wouldn't typically await here, but this handles edge cases.
    else:
        # Standard script execution
        asyncio.run(scrape_jthomas_prices())
