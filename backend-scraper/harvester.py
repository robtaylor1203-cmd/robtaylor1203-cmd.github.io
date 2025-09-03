import requests
import re
import logging
import os
from datetime import datetime
from urllib.parse import urljoin, urlparse, parse_qs
import time

from bs4 import BeautifulSoup
from selenium.common.exceptions import TimeoutException
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- V10 Configuration: Optimized for Local Execution ---
# Set to False to watch the browser work, True for background execution.
RUN_HEADLESS = True 
PAGE_LOAD_TIMEOUT = 120 
DYNAMIC_TIMEOUT = 90  

class ReportHarvesterV10:
    """
    Acquires market reports using a resilient hybrid approach, optimized for local execution.
    """
    def __init__(self, inbox_dir="Inbox"):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36'
        })
        self.inbox_dir = inbox_dir
        self.driver = None

        if not os.path.exists(inbox_dir):
            os.makedirs(inbox_dir)
        self.download_dir = os.path.abspath(inbox_dir)
        logging.info(f"Download directory set to: {self.download_dir}")

    def initialize_driver(self):
        if self.driver:
            return self.driver
        logging.info("Initializing Chrome WebDriver (V10 Local)...")
        chrome_options = Options()
        if RUN_HEADLESS:
            chrome_options.add_argument("--headless")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--log-level=3") # Suppress console noise
        
        # This preference helps ensure PDFs download directly without a preview pane
        prefs = {"plugins.always_open_pdf_externally": True}
        chrome_options.add_experimental_option("prefs", prefs)
        
        try:
            service = Service(ChromeDriverManager().install())
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            self.driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
        except Exception as e:
            logging.error(f"Failed to initialize WebDriver: {e}")
            raise
        return self.driver

    def close_driver(self):
        if self.driver:
            logging.info("Closing WebDriver.")
            time.sleep(3) # Brief pause before closing
            self.driver.quit()
            self.driver = None

    def run(self):
        logging.info("Starting TeaTrade Report Harvester (V10 Local Execution)...")
        targets = {
            "ForbesWalker": self.scrape_forbes_walker,
            "TBEA": self.scrape_tbea_hybrid,
            "CeylonTeaBrokers": self.scrape_ctb_hybrid,
            "ATB Ltd": self.scrape_atb_ltd_iframe_extraction,
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
                logging.error(f"Error harvesting {name}: {type(e).__name__} - {e}") 
        self.close_driver()
        logging.info("Harvester finished successfully.")

    def sanitize_filename(self, title):
        filename = re.sub(r'[<>:"/\\|?*]', '', title)
        return filename.replace(' ', '_').replace('/', '_')[:150]

    def download_report(self, report_info):
        url, title, source = report_info['url'], report_info['title'], report_info['source']
        extension = os.path.splitext(urlparse(url).path)[1] or ".pdf"
        filename = f"{source}_{self.sanitize_filename(title)}{extension}"
        filepath = os.path.join(self.download_dir, filename)

        if os.path.exists(filepath):
            logging.info(f"Report already exists, skipping: {filename}")
            return
        try:
            logging.info(f"Downloading report from {url}...")
            response = self.session.get(url, stream=True, timeout=180)
            response.raise_for_status()
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            logging.info(f"Successfully downloaded: {filename}")
        except Exception as e:
            logging.error(f"Failed to download report from {url}: {e}")

    # --- V10: Site-Specific Strategies ---

    def scrape_forbes_walker(self):
        # STRATEGY: Forbes is best handled by direct API, but since it's blocked in the cloud,
        # we revert to Selenium for local execution, which will succeed.
        BASE_URL = "https://web.forbestea.com/market-reports"
        driver = self.initialize_driver()
        try:
            driver.get(BASE_URL)
            logging.info("Interacting with Forbes & Walker dropdowns...")
            # Using find_element is sufficient here as WebDriverWait is for complex waits.
            # This relies on the page loading fully, which it will on a local machine.
            time.sleep(5) # Give page time to load JS
            year_select = Select(driver.find_element(By.ID, "year"))
            latest_year_value = year_select.options[1].get_attribute("value")
            year_select.select_by_value(latest_year_value)
            
            time.sleep(5) # Wait for report dropdown to populate
            report_select = Select(driver.find_element(By.ID, "report_id"))
            latest_report_option = report_select.options[1]
            title = latest_report_option.text
            report_id = latest_report_option.get_attribute("value")
            report_select.select_by_value(report_id)
            
            time.sleep(2)
            submit_button = driver.find_element(By.CSS_SELECTOR, "a.btn-gradient")
            # The link itself is the download URL
            report_url = submit_button.get_attribute("href")

            if report_url:
                logging.info(f"Found latest Forbes & Walker report: {title}")
                return {"source": "ForbesWalker", "title": title, "url": report_url}
        except Exception as e:
            logging.error(f"Dynamic navigation failed on Forbes & Walker: {e}")
        return None


    def scrape_tbea_hybrid(self, retries=2):
        BASE_URL = "https://www.tbeal.net/tbea-market-report/"
        driver = self.initialize_driver()
        try:
            driver.get(BASE_URL)
            WebDriverWait(driver, DYNAMIC_TIMEOUT).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='.pdf']"))
            )
            page_source = driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            
            report_regex = re.compile(r"Market Report.*Sale\s*(\d+)", re.IGNORECASE)
            found_reports = []
            for link in soup.find_all('a', href=True):
                text = link.get_text(strip=True)
                if '.pdf' in link['href'].lower():
                    match = report_regex.search(text)
                    if match:
                        found_reports.append({
                            "sale": int(match.group(1)), 
                            "title": text, 
                            "url": urljoin(BASE_URL, link['href'])
                        })
            if found_reports:
                latest_report = sorted(found_reports, key=lambda x: x['sale'], reverse=True)[0]
                logging.info(f"Found latest TBEA report: Sale {latest_report['sale']}")
                return {"source": "TBEA", "title": latest_report['title'], "url": latest_report['url']}
        except Exception as e:
            logging.error(f"Hybrid scan failed on TBEA: {e}")
        return None

    def scrape_ctb_hybrid(self):
        BASE_URL = "https://ceylonteabrokers.com/market-reports/"
        driver = self.initialize_driver()
        try:
            driver.get(BASE_URL)
            xpath = "//a[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'report') and contains(@href, '.pdf')]"
            link_element = WebDriverWait(driver, DYNAMIC_TIMEOUT).until(
                EC.visibility_of_element_located((By.XPATH, xpath))
            )
            report_url = link_element.get_attribute("href")
            title = link_element.get_text(strip=True) or f"CTB_Report_{datetime.now().strftime('%Y%m%d')}"
            logging.info(f"Found latest CTB report: {title}")
            return {"source": "CTB", "title": title, "url": report_url}
        except Exception as e:
            logging.error(f"Hybrid scan failed on Ceylon Tea Brokers: {e}")
        return None

    def scrape_atb_ltd_iframe_extraction(self):
        BASE_URL = "https://www.atbltd.com/Docs/current_market_report"
        driver = self.initialize_driver()
        try:
            driver.get(BASE_URL)
            iframe = WebDriverWait(driver, DYNAMIC_TIMEOUT).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "iframe[src*='Viewer']"))
            )
            iframe_src = iframe.get_attribute("src")
            pdf_path = parse_qs(urlparse(iframe_src).query).get('file', [None])[0]
            if pdf_path:
                direct_pdf_url = urljoin(BASE_URL, pdf_path)
                title = f"ATB_Market_Report_{datetime.now().strftime('%Y%m%d')}"
                logging.info(f"Found latest ATB report URL.")
                return {"source": "ATB", "title": title, "url": direct_pdf_url}
        except Exception as e:
            logging.error(f"Iframe extraction failed on ATB Ltd: {e}")
        return None

if __name__ == "__main__":
    harvester = ReportHarvesterV10(inbox_dir="Inbox")
    harvester.run()