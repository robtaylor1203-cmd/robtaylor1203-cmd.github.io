import requests
import re
import logging
import os
from datetime import datetime
from urllib.parse import urljoin, urlparse, parse_qs
import time

# Selenium imports
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
# Import Select for dropdown interaction
from selenium.webdriver.support.ui import WebDriverWait, Select 
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# V4 Configuration: Increased timeouts for resilience
DYNAMIC_TIMEOUT = 60 

class ReportHarvesterV4:
    """
    Acquires the latest market reports with extreme resilience strategies.
    """
    def __init__(self, inbox_dir="Inbox"):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'TeaTrade-Harvester/4.0 (Resilient; https://teatrade.co.uk)'
        })
        self.inbox_dir = inbox_dir
        self.driver = None

        if not os.path.exists(inbox_dir):
            os.makedirs(inbox_dir)
        self.download_dir = os.path.abspath(inbox_dir)
        logging.info(f"Download directory set to: {self.download_dir}")

    def initialize_driver(self):
        """Initializes Selenium WebDriver (Headless Chrome)."""
        if self.driver:
            return self.driver

        logging.info("Initializing Headless Chrome WebDriver (V4)...")
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        # V4 FIX: Ensure window size is sufficient for complex layouts
        chrome_options.add_argument("--window-size=1920,1080") 
        
        try:
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            # Set a global implicit wait as a fallback
            self.driver.implicitly_wait(10) 
        except Exception as e:
            logging.error(f"Failed to initialize WebDriver: {e}")
            raise
        return self.driver

    def close_driver(self):
        if self.driver:
            logging.info("Closing WebDriver.")
            self.driver.quit()
            self.driver = None

    def run(self):
        logging.info("Starting TeaTrade Report Harvester (V4)...")
        
        targets = {
            "TBEA (Resilient Scan)": self.scrape_tbea_resilient,
            "CeylonTeaBrokers (Dynamic)": self.scrape_ceylon_tea_brokers_dynamic,
            # V4 Change: ForbesWalker switched from API to Dynamic due to 404 errors
            "ForbesWalker (Dynamic Dropdown)": self.scrape_forbes_walker_dynamic,
            "ATB Ltd (Iframe Extraction)": self.scrape_atb_ltd_iframe_extraction,
        }

        for name, method in targets.items():
            try:
                logging.info(f"--- Scanning {name} ---")
                report_info = method()
                if report_info:
                    self.download_report(report_info)
                else:
                    logging.warning(f"Could not find the latest report for {name}.")

            except Exception as e:
                logging.error(f"Error harvesting {name}: {e}", exc_info=True)

        self.close_driver()
        logging.info("Harvester finished.")

    # --- Utility Functions ---
    
    def sanitize_filename(self, title):
        filename = re.sub(r'[<>:"/\\|?*]', '', title)
        return filename.replace(' ', '_')[:150]

    def download_report(self, report_info):
        url = report_info['url']
        title = report_info['title']
        source = report_info['source']
        
        parsed_url = urlparse(url)
        extension = os.path.splitext(parsed_url.path)[1]
        if not extension or len(extension) > 5 or 'aspx' in extension.lower():
             extension = ".pdf"

        filename = f"{source}_{self.sanitize_filename(title)}{extension}"
        filepath = os.path.join(self.download_dir, filename)

        if os.path.exists(filepath):
            logging.info(f"Report already exists, skipping: {filename}")
            return

        try:
            logging.info(f"Downloading report from {url}...")
            # Increase timeout for the download request itself
            response = self.session.get(url, stream=True, timeout=120) 
            response.raise_for_status()
            
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            logging.info(f"Successfully downloaded and saved to: {filepath}")
        except Exception as e:
            logging.error(f"Failed to download report from {url}: {e}")

    # --- Scraper Implementations (V4 Resilience Enhancements) ---

    def scrape_tbea_resilient(self):
        # Strategy: Scan all links, find the highest sale number. Increased timeouts.
        BASE_URL = "https://www.tbeal.net/tbea-market-report/"
        driver = self.initialize_driver()
        driver.get(BASE_URL)

        try:
            # V4: Increased timeout
            WebDriverWait(driver, DYNAMIC_TIMEOUT).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".elementor-widget-container"))
            )

            report_regex = re.compile(r"Market Report.*Sale\s*(\d+)", re.IGNORECASE)
            found_reports = []

            # Scan ALL links
            links = driver.find_elements(By.TAG_NAME, "a")
            for link in links:
                # V4: Use get_attribute('textContent') for more reliable text extraction
                text = link.get_attribute('textContent').strip()
                href = link.get_attribute("href")
                
                if href and '.pdf' in href.lower():
                    match = report_regex.search(text)
                    if match:
                        try:
                            sale_number = int(match.group(1))
                            found_reports.append({
                                "sale": sale_number,
                                "title": text,
                                "url": urljoin(BASE_URL, href)
                            })
                        except ValueError:
                            continue

            if found_reports:
                latest_report = sorted(found_reports, key=lambda x: x['sale'], reverse=True)[0]
                logging.info(f"Found latest TBEA report: Sale {latest_report['sale']}")
                return {"source": "TBEA", "title": latest_report['title'], "url": latest_report['url']}

        except Exception as e:
            logging.error(f"Resilient scan failed on TBEA (Timeout likely): {e}")
        return None

    def scrape_ceylon_tea_brokers_dynamic(self):
        # Strategy: Use flexible XPath. Increased timeouts.
        BASE_URL = "https://ceylonteabrokers.com/market-reports/"
        driver = self.initialize_driver()
        driver.get(BASE_URL)

        try:
            # V4: Increased timeout and wait for visibility
            latest_report_button = WebDriverWait(driver, DYNAMIC_TIMEOUT).until(
                EC.visibility_of_element_located((By.XPATH, "//a[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'report') and substring(@href, string-length(@href) - 3) = '.pdf']"))
            )
            
            report_url = latest_report_button.get_attribute("href")
            # V4: Use get_attribute('textContent')
            title_text = latest_report_button.get_attribute('textContent').strip()
            
            if not title_text or len(title_text) < 10:
                 title_text = f"CTB_Weekly_Report_{datetime.now().strftime('%Y%m%d')}"

            if report_url:
                 return {"source": "CTB", "title": title_text, "url": report_url}

        except Exception as e:
            logging.error(f"Dynamic interaction failed on Ceylon Tea Brokers (Timeout likely): {e}")
        return None

    def scrape_atb_ltd_iframe_extraction(self):
        # Strategy: Extract direct PDF URL from the iframe. Increased timeouts.
        BASE_URL = "https://www.atbltd.com/Docs/current_market_report"
        driver = self.initialize_driver()
        driver.get(BASE_URL)

        try:
            logging.info("Locating ATB iframe...")
            # V4: Increased timeout
            iframe = WebDriverWait(driver, DYNAMIC_TIMEOUT).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "iframe[src*='Viewer']"))
            )
            
            iframe_src = iframe.get_attribute("src")
            logging.info(f"Extracted iframe src: {iframe_src}")

            parsed_src = urlparse(iframe_src)
            query_params = parse_qs(parsed_src.query)
            
            pdf_path = query_params.get('file', [None])[0] or query_params.get('src', [None])[0]

            if pdf_path:
                direct_pdf_url = urljoin(BASE_URL, pdf_path)
                logging.info(f"Constructed direct PDF URL: {direct_pdf_url}")
                
                title = f"ATB_Market_Report_{datetime.now().strftime('%Y%m%d')}"
                return {"source": "ATB", "title": title, "url": direct_pdf_url}
            else:
                logging.error("Could not extract PDF path from iframe src.")

        except Exception as e:
            logging.error(f"Iframe extraction failed on ATB Ltd (Timeout likely): {e}")
        return None


    def scrape_forbes_walker_dynamic(self):
        # V4 Strategy: Interact with dropdowns dynamically because the API failed (404 error).
        BASE_URL = "https://web.forbestea.com/market-reports"
        driver = self.initialize_driver()
        driver.get(BASE_URL)

        try:
            logging.info("Interacting with Forbes & Walker dropdowns...")
            
            # 1. Wait for and select the Year
            # The year dropdown ID is identified as 'year'
            year_select_element = WebDriverWait(driver, DYNAMIC_TIMEOUT).until(
                EC.element_to_be_clickable((By.ID, "year"))
            )
            year_select = Select(year_select_element)
            
            # Select the latest year (assuming the first option is the latest)
            latest_year_option = year_select.options[0]
            if not latest_year_option.get_attribute("value"): # Handle potential empty first option
                 latest_year_option = year_select.options[1]

            latest_year = latest_year_option.get_attribute("value")
            logging.info(f"Selecting Year: {latest_year}")
            
            # Use JavaScript execution to ensure the change event fires reliably in headless mode
            driver.execute_script("arguments[0].value = arguments[1]; arguments[0].dispatchEvent(new Event('change'));", year_select_element, latest_year)
            
            # 2. Wait for the Report dropdown to update
            # The report dropdown ID is identified as 'report_id'
            time.sleep(5) # Explicit wait for AJAX update (crucial for dropdown synchronization)
            report_select_element = WebDriverWait(driver, DYNAMIC_TIMEOUT).until(
                EC.element_to_be_clickable((By.ID, "report_id"))
            )
            report_select = Select(report_select_element)

            # Wait until the options list has populated after the year change
            WebDriverWait(driver, DYNAMIC_TIMEOUT).until(
                lambda d: len(report_select.options) > 1
            )

            # 3. Select the latest report
            latest_report_option = report_select.options[0]
            if not latest_report_option.get_attribute("value"):
                latest_report_option = report_select.options[1]

            title = latest_report_option.text.strip()
            report_id = latest_report_option.get_attribute("value")
            logging.info(f"Selecting Report: {title} (ID: {report_id})")
            
            # Select the report
            driver.execute_script("arguments[0].value = arguments[1]; arguments[0].dispatchEvent(new Event('change'));", report_select_element, report_id)

            # 4. Trigger the download. The site uses a form submission tied to a button/link.
            # The submit button is often styled as a link 'a.btn-gradient'
            submit_button = WebDriverWait(driver, DYNAMIC_TIMEOUT).until(
                 EC.element_to_be_clickable((By.CSS_SELECTOR, "a.btn-gradient, button[type='submit']"))
            )
            
            logging.info("Submitting form to access report...")
            # Use JavaScript click for robustness
            driver.execute_script("arguments[0].click();", submit_button)

            # 5. Capture the resulting URL. The browser navigates to the PDF URL.
            WebDriverWait(driver, DYNAMIC_TIMEOUT).until(
                 lambda d: d.current_url != BASE_URL and ("pdf" in d.current_url.lower() or "download" in d.current_url.lower())
            )
            
            final_url = driver.current_url
            logging.info(f"Captured final report URL: {final_url}")

            if final_url and final_url != BASE_URL:
                 return {"source": "ForbesWalker", "title": title, "url": final_url}
            else:
                logging.error("Forbes & Walker navigation did not result in a report URL.")

        except Exception as e:
            logging.error(f"Dynamic navigation failed on Forbes & Walker: {e}")
        return None

# Entry point for the script
if __name__ == "__main__":
    harvester = ReportHarvesterV4(inbox_dir="Inbox")
    harvester.run()