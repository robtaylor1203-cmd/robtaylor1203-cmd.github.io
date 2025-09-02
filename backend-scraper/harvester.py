import requests
import re
import logging
import os
import time
from datetime import datetime
from urllib.parse import urljoin

# Selenium imports
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
# webdriver_manager handles the driver binaries automatically
from webdriver_manager.chrome import ChromeDriverManager

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')

class ReportHarvesterV2:
    """
    Acquires the latest market reports using a hybrid approach (Static API and Dynamic Selenium Scraping).
    """
    def __init__(self, inbox_dir="Inbox"):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'TeaTrade-Harvester/2.0 (Dynamic; https://teatrade.co.uk)'
        })
        self.inbox_dir = inbox_dir
        self.driver = None # Selenium WebDriver instance

        # Ensure the Inbox directory exists and get its absolute path for Selenium downloads
        if not os.path.exists(inbox_dir):
            os.makedirs(inbox_dir)
        self.download_dir = os.path.abspath(inbox_dir)
        logging.info(f"Download directory set to: {self.download_dir}")

    def initialize_driver(self):
        """Initializes the Selenium WebDriver for dynamic scraping."""
        if self.driver:
            return self.driver

        logging.info("Initializing Headless Chrome WebDriver...")
        chrome_options = Options()
        # Essential arguments for running headless in GitHub Actions
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument(f"user-agent={self.session.headers['User-Agent']}")
        
        # Configure download preferences for sites like ATB Ltd interaction
        prefs = {
            "download.default_directory": self.download_dir,
            "download.prompt_for_download": False,
            "plugins.always_open_pdf_externally": True
        }
        chrome_options.add_experimental_option("prefs", prefs)

        # Use webdriver_manager to install and manage ChromeDriver
        try:
            # Use the Chrome version installed by the GitHub Action setup-chrome
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
        except Exception as e:
            logging.error(f"Failed to initialize WebDriver: {e}")
            raise
        return self.driver

    def close_driver(self):
        """Closes the Selenium WebDriver."""
        if self.driver:
            logging.info("Closing WebDriver.")
            self.driver.quit()
            self.driver = None

    def run(self):
        logging.info("Starting TeaTrade Report Harvester (V2)...")
        
        # Define targets and methods
        targets = {
            "TBEA (Dynamic)": self.scrape_tbea_dynamic,
            "CeylonTeaBrokers (Dynamic)": self.scrape_ceylon_tea_brokers_dynamic,
            "ForbesWalker (API)": self.scrape_forbes_walker_api,
            "ATB Ltd (Dynamic Interaction)": self.scrape_atb_ltd,
        }

        for name, method in targets.items():
            try:
                logging.info(f"--- Scanning {name} ---")
                # Methods for TBEA, CTB, ForbesWalker return report_info (URL based)
                # Method for ATB handles download directly via browser interaction
                if name != "ATB Ltd (Dynamic Interaction)":
                    report_info = method()
                    if report_info:
                        self.download_report(report_info)
                    else:
                        logging.warning(f"Could not find the latest report for {name}.")
                else:
                    method() # ATB handles its own download process

            except Exception as e:
                logging.error(f"Error harvesting {name}: {e}", exc_info=True)

        self.close_driver()
        logging.info("Harvester finished.")

    # --- Utility Functions ---

    def sanitize_filename(self, title):
        filename = re.sub(r'[<>:"/\\|?*]', '', title)
        return filename.replace(' ', '_')[:150]

    def download_report(self, report_info):
        """Downloads report from a direct URL (Used by TBEA, CTB, ForbesWalker)."""
        url = report_info['url']
        title = report_info['title']
        source = report_info['source']
        
        extension = os.path.splitext(url.split('?')[0])[1] # Handle URLs with parameters
        if not extension or len(extension) > 5 or 'aspx' in extension.lower():
             extension = ".pdf" # Default assumption if URL is obscure

        filename = f"{source}_{self.sanitize_filename(title)}{extension}"
        filepath = os.path.join(self.download_dir, filename)

        if os.path.exists(filepath):
            logging.info(f"Report already exists, skipping: {filename}")
            return

        try:
            logging.info(f"Downloading report from {url}...")
            # Use the requests session for standard downloads
            response = self.session.get(url, stream=True)
            response.raise_for_status()
            
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            logging.info(f"Successfully downloaded and saved to: {filepath}")
        except Exception as e:
            logging.error(f"Failed to download report from {url}: {e}")

    # --- Dynamic Scraper Implementations (Selenium) ---

    def scrape_tbea_dynamic(self):
        # Handles https://www.tbeal.net/tbea-market-report/
        # Clicks the latest year tab and finds the latest report link.
        BASE_URL = "https://www.tbeal.net/tbea-market-report/"
        driver = self.initialize_driver()
        driver.get(BASE_URL)

        try:
            # Wait for the year tabs (e.g., "2025") to load and be clickable
            WebDriverWait(driver, 15).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, ".elementor-tabs-wrapper .elementor-tab-title"))
            )
            
            # Find the latest year tab (assuming the first visible one is the latest)
            latest_year_tab = driver.find_elements(By.CSS_SELECTOR, ".elementor-tabs-wrapper .elementor-tab-title")[0]
            logging.info(f"Interacting with TBEA tab: {latest_year_tab.text}")
            
            # Click the tab if it's not already active
            if "elementor-active" not in latest_year_tab.get_attribute("class"):
                latest_year_tab.click()

            # Wait for the content area to update after the click
            active_content = WebDriverWait(driver, 10).until(
                 EC.visibility_of_element_located((By.CSS_SELECTOR, ".elementor-tabs-content-wrapper .elementor-tab-content[aria-hidden='false']"))
            )

            # Find the latest report link within the active area
            report_regex = re.compile(r"Market Report.*Sale\s*\d+", re.IGNORECASE)
            
            links = active_content.find_elements(By.TAG_NAME, "a")
            for link in links:
                text = link.text.strip()
                href = link.get_attribute("href")
                if href and report_regex.search(text):
                     report_url = urljoin(BASE_URL, href)
                     return {"source": "TBEA", "title": text, "url": report_url}
        
        except Exception as e:
            logging.error(f"Dynamic interaction failed on TBEA: {e}")
            return None

    def scrape_ceylon_tea_brokers_dynamic(self):
        # Handles https://ceylonteabrokers.com/market-reports/
        # Locates the specific button designated for the latest report.
        BASE_URL = "https://ceylonteabrokers.com/market-reports/"
        driver = self.initialize_driver()
        driver.get(BASE_URL)

        try:
            # Wait for the page to stabilize
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )

            # Identify the prominent button element that links to the latest report.
            # We use XPath to find links containing specific text and ending in .pdf
            latest_report_button = WebDriverWait(driver, 10).until(
                EC.visibility_of_element_located((By.XPATH, "//a[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'weekly tea market report') and contains(@href, '.pdf')]"))
            )
            
            report_url = latest_report_button.get_attribute("href")
            title_text = latest_report_button.text.strip()
            
            # Use a generic title if the button text isn't descriptive
            if not title_text or len(title_text) < 10:
                 title_text = f"CTB_Weekly_Report_{datetime.now().strftime('%Y%m%d')}"


            if report_url:
                 return {"source": "CTB", "title": title_text, "url": report_url}

        except Exception as e:
            logging.error(f"Dynamic interaction failed on Ceylon Tea Brokers: {e}")
            return None

    def scrape_atb_ltd(self):
        # Handles https://www.atbltd.com/Docs/current_market_report
        # Interacts with the embedded PDF viewer to trigger the download.
        BASE_URL = "https://www.atbltd.com/Docs/current_market_report"
        driver = self.initialize_driver()
        driver.get(BASE_URL)

        try:
            # The report is embedded in an iframe. We must switch context to the iframe.
            logging.info("Switching to ATB iframe context...")
            WebDriverWait(driver, 20).until(
                # Wait for the iframe that contains the viewer application
                EC.frame_to_be_available_and_switch_to_it((By.CSS_SELECTOR, "iframe[src*='Viewer']"))
            )
            
            # Wait for the download button within the iframe viewer controls
            logging.info("Locating download button within the viewer...")
            download_button = WebDriverWait(driver, 20).until(
                # Look for common viewer download button selectors
                EC.element_to_be_clickable((By.CSS_SELECTOR, "button[title='Download'], button#download, button[data-action='download']"))
            )
            
            logging.info("Attempting to click ATB download button...")
            
            # Get initial file count in the Inbox
            initial_files = set(os.listdir(self.download_dir))
            
            download_button.click()
            
            # Wait for the download to complete (Monitoring the download directory)
            logging.info("Waiting for download to complete (max 90s)...")
            timeout = 90
            start_time = time.time()
            downloaded_file = None

            # Monitor the directory for the new file
            while time.time() - start_time < timeout:
                current_files = set(os.listdir(self.download_dir))
                new_files = current_files - initial_files
                
                # Check if a new file has appeared and is not a temporary download file
                if new_files:
                    potential_file = new_files.pop()
                    if not potential_file.endswith('.crdownload') and not potential_file.endswith('.tmp'):
                        downloaded_file = potential_file
                        break
                time.sleep(1)

            if downloaded_file:
                # Rename the file to a standard format
                new_filename = f"ATB_Market_Report_{datetime.now().strftime('%Y%m%d')}{os.path.splitext(downloaded_file)[1]}"
                os.rename(os.path.join(self.download_dir, downloaded_file), os.path.join(self.download_dir, new_filename))
                logging.info(f"Successfully downloaded ATB report: {new_filename}")
            else:
                logging.error("ATB download timed out or failed.")

        except Exception as e:
            logging.error(f"Dynamic interaction failed on ATB Ltd: {e}")
        finally:
             # Ensure we switch back to the main content context
            try:
                if self.driver:
                    self.driver.switch_to.default_content()
            except:
                pass


    # --- API/Static Scraper Implementations ---

    def scrape_forbes_walker_api(self):
        # Handles https://web.forbestea.com/market-reports using their API
        # This site does not require Selenium as the API provides the data directly.
        API_URL = "https://web.forbestea.com/api/reports?page=1&search=&year=&category_id="
        logging.info(f"Querying Forbes & Walker API: {API_URL}")
        
        # Use requests session for API calls
        response = self.session.get(API_URL)
        if not response.ok:
            logging.error(f"Forbes Walker API request failed: {response.status_code}")
            return None
            
        data = response.json()

        if 'data' in data and data['data']:
            latest_report = data['data'][0]
            title = latest_report.get('title', 'ForbesWalker_Market_Report')
            report_url = latest_report.get('file', {}).get('url')
            
            if report_url:
                 return {"source": "ForbesWalker", "title": title, "url": report_url}
        return None

# Entry point for the script
if __name__ == "__main__":
    # The script expects to run from the root of the repository.
    harvester = ReportHarvesterV2(inbox_dir="Inbox")
    harvester.run()