import os
import requests
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# --- Configuration ---
INBOX_PATH = "Inbox"

# --- Helper Function ---
def download_file(url, folder, filename):
    """Downloads a file from a URL into a specified folder and filename."""
    if not os.path.exists(folder):
        os.makedirs(folder)
    
    filepath = os.path.join(folder, filename)
    
    # Check if the file already exists to avoid re-downloading
    if os.path.exists(filepath):
        print(f"Skipping download, {filename} already exists in Inbox.")
        return

    try:
        response = requests.get(url, stream=True)
        response.raise_for_status() # Raise an exception for bad status codes
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Successfully downloaded {filename}")
    except requests.exceptions.RequestException as e:
        print(f"Error downloading {url}: {e}")

def setup_driver():
    """Sets up the automated web browser for Selenium."""
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    return webdriver.Chrome(options=options)

# --- Adapter for ATB Ltd. ---
def harvest_atb():
    print("\n--- Checking ATB Ltd. ---")
    url = "https://www.atbltd.com/Docs/current_market_report"
    filename = f"ATB_Report_{datetime.now().strftime('%Y-%m-%d')}.pdf" # Create a unique name
    download_file(url, INBOX_PATH, filename)

# --- Adapter for TBEA ---
def harvest_tbea():
    print("\n--- Checking TBEA ---")
    driver = setup_driver()
    try:
        driver.get("https://www.tbeal.net/tbea-market-report/")
        # Wait for the main content to be present
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "elementor-widget-container"))
        )
        # Find all links that contain 'WEEKLY-TEA-MARKET-REPORT' in their href
        report_links = driver.find_elements(By.PARTIAL_LINK_TEXT, "WEEKLY TEA MARKET REPORT")
        if report_links:
            latest_link = report_links[0] # Assume the first one is the latest
            report_url = latest_link.get_attribute('href')
            # Create a unique filename from the link text
            filename = f"TBEA_Report_{latest_link.text.replace(' ', '_')}.pdf"
            print(f"Found TBEA link: {report_url}")
            download_file(report_url, INBOX_PATH, filename)
        else:
            print("No TBEA report links found on the page.")
    except Exception as e:
        print(f"An error occurred while harvesting TBEA: {e}")
    finally:
        driver.quit()

# --- Adapter for Forbes & Walker ---
def harvest_forbes_walker():
    print("\n--- Checking Forbes & Walker ---")
    driver = setup_driver()
    try:
        driver.get("https://web.forbestea.com/market-reports")
        # Let the page and JS load
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "year"))
        )
        
        # Select the latest year
        year_dropdown = Select(driver.find_element(By.ID, "year"))
        latest_year = year_dropdown.options[1].text # options[0] is 'Select Year'
        year_dropdown.select_by_visible_text(latest_year)
        print(f"Selected year: {latest_year}")

        # Wait for month dropdown to populate
        WebDriverWait(driver, 10).until(
            lambda d: len(Select(d.find_element(By.ID, "month")).options) > 1
        )
        
        # Select the latest month
        month_dropdown = Select(driver.find_element(By.ID, "month"))
        latest_month = month_dropdown.options[1].text
        month_dropdown.select_by_visible_text(latest_month)
        print(f"Selected month: {latest_month}")

        # Find the search button and click it
        search_button = driver.find_element(By.XPATH, "//button[contains(text(), 'Search')]")
        search_button.click()
        print("Searching for reports...")
        
        # Wait for the report links to appear
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//a[contains(@href, '.pdf')]"))
        )
        
        report_links = driver.find_elements(By.XPATH, "//a[contains(@href, '.pdf')]")
        if report_links:
            # Assuming the first link is the one we want
            report_url = report_links[0].get_attribute('href')
            filename = f"Forbes_Walker_{latest_year}_{latest_month}.pdf"
            print(f"Found Forbes & Walker link: {report_url}")
            download_file(report_url, INBOX_PATH, filename)
        else:
            print("No Forbes & Walker report links found after search.")

    except Exception as e:
        print(f"An error occurred while harvesting Forbes & Walker: {e}")
    finally:
        driver.quit()

# --- Main Execution ---
if __name__ == "__main__":
    print("Starting Market Report Harvester...")
    harvest_atb()
    harvest_tbea()
    harvest_forbes_walker()
    print("\nHarvester run complete.")