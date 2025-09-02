import requests
import re
import logging
import os
from datetime import datetime
from urllib.parse import urljoin, urlparse, parse_qs

# Selenium imports
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
# FIX: Import webdriver_manager for reliable driver setup
from webdriver_manager.chrome import ChromeDriverManager

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ReportHarvesterV3_1:
    """
    Acquires the latest market reports using a resilient hybrid approach.
    """
    def __init__(self, inbox_dir="Inbox"):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'TeaTrade-Harvester/3.1 (Stable; https://teatrade.co.uk)'
        })
        self.inbox_dir = inbox_dir
        self.driver = None

        if not os.path.exists(inbox_dir):
            os.makedirs(inbox_dir)
        self.download_dir = os.path.abspath(inbox_dir)
        logging.info(f"Download directory set to: {self.download_dir}")

    def initialize_driver(self):
        """Initializes Selenium WebDriver using webdriver_manager for stability."""
        if self.driver:
            return self.driver

        logging.info("Initializing Headless Chrome WebDriver (Stable Mode)...")
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        
        try:
            # V3.1 FIX: Use ChromeDriverManager to install/locate the correct driver binary
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
        except Exception as e:
            logging.error(f"Failed to initialize WebDriver: {e}")
            raise
        return self.driver

    # [The rest of the Python script (close_driver, run, download_report, sanitize_filename, and all scraper implementations) remains the same as V3. Included below for completeness.]

    def close_driver(self):
        if self.driver:
            logging.info("Closing WebDriver.")
            self.driver.quit()
            self.driver = None

    def run(self):
        logging.info("Starting TeaTrade Report Harvester (V3.1)...")
        
        targets = {
            "TBEA (Resilient Scan)": self.scrape_tbea_resilient,
            "CeylonTeaBrokers (Dynamic)": self.scrape_ceylon_tea_brokers_dynamic,
            "ForbesWalker (API)": self.scrape_forbes_walker_api,
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
        """Downloads report from a direct URL using requests."""
        url = report_info['url']
        title = report_info['title']
        source = report_info['source']
        
        # Determine extension reliably
        parsed_url = urlparse(url)
        extension = os.path.splitext(parsed_url.path)[1]
        if not extension or len(extension) > 5 or 'aspx' in extension.lower():
             extension = ".pdf" # Default assumption

        filename = f"{source}_{self.sanitize_filename(title)}{extension}"
        filepath = os.path.join(self.download_dir, filename)

        if os.path.exists(filepath):
            logging.info(f"Report already exists, skipping: {filename}")
            return

        try:
            logging.info(f"Downloading report from {url}...")
            response = self.session.get(url, stream=True)
            response.raise_for_status()
            
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            logging.info(f"Successfully downloaded and saved to: {filepath}")
        except Exception as e:
            logging.error(f"Failed to download report from {url}: {e}")

    # --- Scraper Implementations ---

    def scrape_tbea_resilient(self):
        # Strategy: Scan all links and find the highest sale number. Avoids clicking tabs.
        BASE_URL = "https://www.tbeal.net/tbea-market-report/"
        driver = self.initialize_driver()
        driver.get(BASE_URL)

        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, ".elementor-widget-container"))
            )

            report_regex = re.compile(r"Market Report.*Sale\s*(\d+)", re.IGNORECASE)
            found_reports = []

            links = driver.find_elements(By.TAG_NAME, "a")
            for link in links:
                text = link.text.strip()
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
                            continue # Handle cases where the number isn't an integer

            if found_reports:
                latest_report = sorted(found_reports, key=lambda x: x['sale'], reverse=True)[0]
                logging.info(f"Found latest TBEA report: Sale {latest_report['sale']}")
                return {"source": "TBEA", "title": latest_report['title'], "url": latest_report['url']}

        except Exception as e:
            logging.error(f"Resilient scan failed on TBEA: {e}")
        return None

    def scrape_ceylon_tea_brokers_dynamic(self):
        # Strategy: Use flexible XPath to locate the main report button.
        BASE_URL = "https://ceylonteabrokers.com/market-reports/"
        driver = self.initialize_driver()
        driver.get(BASE_URL)

        try:
            WebDriverWait(driver, 15).until(EC.presence_of_element_located((By.TAG_NAME, "body")))

            # Flexible XPath: Look for a link ending in .pdf that contains the word 'report' (case-insensitive)
            latest_report_button = WebDriverWait(driver, 20).until(
                EC.visibility_of_element_located((By.XPATH, "//a[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'report') and substring(@href, string-length(@href) - 3) = '.pdf']"))
            )
            
            report_url = latest_report_button.get_attribute("href")
            title_text = latest_report_button.text.strip()
            
            # Fallback title
            if not title_text or len(title_text) < 10:
                 title_text = f"CTB_Weekly_Report_{datetime.now().strftime('%Y%m%d')}"

            if report_url:
                 return {"source": "CTB", "title": title_text, "url": report_url}

        except Exception as e:
            logging.error(f"Dynamic interaction failed on Ceylon Tea Brokers: {e}")
        return None

    def scrape_atb_ltd_iframe_extraction(self):
        # Strategy: Extract the direct PDF URL from the iframe source, bypassing the viewer interaction.
        BASE_URL = "https://www.atbltd.com/Docs/current_market_report"
        driver = self.initialize_driver()
        driver.get(BASE_URL)

        try:
            logging.info("Locating ATB iframe...")
            # Wait for the iframe that contains the viewer application
            iframe = WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "iframe[src*='Viewer']"))
            )
            
            iframe_src = iframe.get_attribute("src")
            logging.info(f"Extracted iframe src: {iframe_src}")

            # The PDF path is embedded in a query parameter (e.g., ?file=...)
            parsed_src = urlparse(iframe_src)
            query_params = parse_qs(parsed_src.query)
            
            # Common parameter names for embedded viewers
            pdf_path = query_params.get('file', [None])[0] or query_params.get('src', [None])[0]

            if pdf_path:
                # Construct the absolute URL to the PDF
                direct_pdf_url = urljoin(BASE_URL, pdf_path)
                logging.info(f"Constructed direct PDF URL: {direct_pdf_url}")
                
                title = f"ATB_Market_Report_{datetime.now().strftime('%Y%m%d')}"
                return {"source": "ATB", "title": title, "url": direct_pdf_url}
            else:
                logging.error("Could not extract PDF path from iframe src.")

        except Exception as e:
            logging.error(f"Iframe extraction failed on ATB Ltd: {e}")
        return None


    def scrape_forbes_walker_api(self):
        # Strategy: Use the reliable backend API.
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
    harvester = ReportHarvesterV3_1(inbox_dir="Inbox")
    harvester.run()