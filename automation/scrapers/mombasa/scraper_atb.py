import asyncio
from playwright.async_api import async_playwright
from pathlib import Path
import datetime
import shutil

# Define the base directory of the repository (assuming script is in scrapers/mombasa)
REPO_BASE = Path(__file__).resolve().parent.parent.parent
# Define where the raw PDFs should be saved
RAW_DOWNLOAD_DIR = REPO_BASE / "raw_downloads" / "mombasa"
# Ensure the download directory exists
RAW_DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

TARGET_URL = "https://www.atbltd.com/Docs/current_market_report"

async def scrape_atb_report():
    print(f"Starting ATB scraper...")
    async with async_playwright() as p:
        # Launch the browser (headless=True means it runs in the background)
        browser = await p.chromium.launch(headless=True)
        
        # Create a browser context that explicitly accepts downloads
        context = await browser.new_context(accept_downloads=True)
        page = await context.new_page()

        try:
            # We expect a download when navigating to the URL.
            async with page.expect_download() as download_info:
                # Navigate to the URL. This URL directly triggers the PDF download.
                print(f"Navigating to {TARGET_URL} and waiting for download...")
                # We use wait_until="commit" because the URL serves a file, not HTML.
                await page.goto(TARGET_URL, wait_until="commit", timeout=60000)
            
            download = await download_info.value
            
            # Generate a unique filename based on the date
            # (Note: The date in this environment is currently mocked to Sept 8, 2025)
            today = datetime.date.today().strftime("%Y%m%d")
            suggested_filename = download.suggested_filename
            
            # Create a clean, standardized filename: ATB_Mombasa_YYYYMMDD_OriginalName.pdf
            new_filename = f"ATB_Mombasa_{today}_{suggested_filename}"
            save_path = RAW_DOWNLOAD_DIR / new_filename

            # Optional: Check if we already downloaded this specific file today
            if save_path.exists():
                print(f"Report '{new_filename}' already exists. Skipping download.")
                return

            # Get the temporary path where Playwright stored the file
            temp_path = await download.path()
            
            # Move the file from the temporary location to the desired final location
            shutil.move(temp_path, save_path)

            print(f"\nSUCCESS: Report downloaded and saved to {save_path}")

        except Exception as e:
            print(f"\nERROR: Failed to download the report from ATB. Details: {e}")
            # Save a screenshot for debugging if an error occurs
            error_screenshot = REPO_BASE / "error_screenshot_atb.png"
            await page.screenshot(path=error_screenshot)
            print(f"Saved error screenshot to {error_screenshot}")
        
        finally:
            await browser.close()
            print("Browser closed.")

if __name__ == "__main__":
    # Run the async function
    asyncio.run(scrape_atb_report())
