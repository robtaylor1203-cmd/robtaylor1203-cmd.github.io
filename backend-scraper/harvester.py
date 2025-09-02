import requests
import re
import logging
import os
from datetime import datetime
from urllib.parse import urljoin, urlparse, parse_qs
import time

# CRITICAL FIX: Import BeautifulSoup and TimeoutException
from bs4 import BeautifulSoup
from selenium.common.exceptions import TimeoutException

# Selenium imports
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# V8 Configuration: Extreme Resilience Timeouts
PAGE_LOAD_TIMEOUT = 180 # 3 minutes for a page to load (Kept from V7)
DYNAMIC_TIMEOUT = 150  # V8: Increased to 2.5 minutes for an element to appear

class ReportHarvesterV8:
    """
    Acquires market reports using an Intelligent Hybrid approach, optimized for slow cloud environments.
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
        """Initializes a resilient Selenium WebDriver."""
        if self.driver:
            return self.driver
        logging.info("Initializing Headless Chrome WebDriver (V8)...")
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1920,1080")
        # V8: Add arguments to minimize resource usage
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-extensions")
        
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
            self.driver.quit()
            self.driver = None

    def run(self):
        logging.info("Starting TeaTrade Report Harvester (V8 Extreme Resilience)...")
        # V8: Updated target names to reflect the strategy
        targets = {
            "ForbesWalker (API V3 Spoof)": self.scrape_forbes_walker_api_v3,
            "TBEA (Hybrid + Fallback)": self.scrape_tbea_hybrid,
            "CeylonTeaBrokers (Hybrid + Fallback)": self.scrape_ctb_hybrid,
            "ATB Ltd (Hybrid Iframe Extraction)": self.scrape_atb_ltd_iframe_extraction,
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
                # V8: Simplified error logging for cleaner cloud logs
                logging.error(f"Error harvesting {name}: {type(e).__name__} - {e}") 
        self.close_driver()
        logging.info("Harvester finished.")

    def sanitize_filename(self, title):
        filename = re.sub(r'[<>:"/\\|?*]', '', title)
        return filename.replace(' ', '_').replace('/', '_')[:150]

    def download_report(self, report_info):
        url = report_info['url']
        title = report_info['title']
        source = report_info['source']
        parsed_url = urlparse(url)
        extension = os.path.splitext(parsed_url.path)[1]
        if not extension or len(extension) > 5:
             extension = ".pdf"
        filename = f"{source}_{self.sanitize_filename(title)}{extension}"
        filepath = os.path.join(self.download_dir, filename)
        if os.path.exists(filepath):
            logging.info(f"Report already exists, skipping: {filename}")
            return
        try:
            logging.info(f"Downloading report from {url}...")
            # V8: Increased download timeout
            response = self.session.get(url, stream=True, timeout=180) 
            response.raise_for_status()
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            logging.info(f"Successfully downloaded and saved to: {filepath}")
        except Exception as e:
            logging.error(f"Failed to download report from {url}: {e}")

    # --- V8: Site-Specific Hybrid Strategies ---

    def scrape_forbes_walker_api_v3(self):
        # V8 STRATEGY: API First with Header Spoofing to bypass potential cloud blocking (404 errors).
        API_URL = "https://web.forbestea.com/api/get-reports-by-category/1"
        logging.info(f"Querying Forbes & Walker API V3: {API_URL}")
        try:
            headers = self.session.headers.copy()
            # V8: Explicitly define Referer and Origin to mimic a browser request
            headers['Referer'] = 'https://web.forbestea.com/market-reports'
            headers['Origin'] = 'https://web.forbestea.com'
            headers['Accept'] = 'application/json, text/plain, */*'
            
            response = self.session.get(API_URL, headers=headers, timeout=60)
            response.raise_for_status()
            data = response.json()
            
            if 'reports' in data and data['reports']:
                latest_report = data['reports'][0] 
                title = latest_report.get('title', 'ForbesWalker_Market_Report')
                report_url = latest_report.get('file', {}).get('url')
                
                if report_url:
                    logging.info(f"Successfully retrieved API data for {title}")
                    return {"source": "ForbesWalker", "title": title, "url": report_url}
        except Exception as e:
            logging.error(f"API request failed on Forbes & Walker: {e}")
        return None

    def scrape_tbea_hybrid(self):
        # V8 STRATEGY: Render with Selenium (long wait), then parse with BeautifulSoup. Implement Blind Fallback.
        BASE_URL = "https://www.tbeal.net/tbea-market-report/"
        driver = self.initialize_driver()
        page_source = ""
        try:
            logging.info("Loading TBEA page with Selenium...")
            driver.get(BASE_URL)
            logging.info(f"Waiting for dynamic content (Max wait: {DYNAMIC_TIMEOUT}s)...")
            WebDriverWait(driver, DYNAMIC_TIMEOUT).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='.pdf']"))
            )
            logging.info("Dynamic content detected.")
            page_source = driver.page_source
        except TimeoutException:
            # V8: Blind Fallback - If timeout occurs, grab whatever source is available and try parsing anyway.
            logging.warning("Timeout waiting for dynamic content. Attempting Blind Fallback parsing.")
            if driver.page_source:
                page_source = driver.page_source
        except Exception as e:
            logging.error(f"Selenium error during TBEA scan: {e}")
            return None

        if not page_source:
            logging.error("TBEA scan failed: No page source captured.")
            return None

        # BeautifulSoup Parsing
        try:
            logging.info("Parsing source with BeautifulSoup...")
            soup = BeautifulSoup(page_source, 'html.parser')
            
            report_regex = re.compile(r"Market Report.*Sale\s*(\d+)", re.IGNORECASE)
            found_reports = []
            for link in soup.find_all('a', href=True):
                text = link.get_text(strip=True)
                href = link['href']
                if '.pdf' in href.lower():
                    match = report_regex.search(text)
                    if match:
                        try:
                            sale_number = int(match.group(1))
                            full_url = urljoin(BASE_URL, href)
                            found_reports.append({"sale": sale_number, "title": text, "url": full_url})
                        except ValueError:
                            continue
            
            if found_reports:
                latest_report = sorted(found_reports, key=lambda x: x['sale'], reverse=True)[0]
                logging.info(f"Found latest TBEA report: Sale {latest_report['sale']}")
                return {"source": "TBEA", "title": latest_report['title'], "url": latest_report['url']}
            else:
                logging.warning("Could not find report links in TBEA page source.")
        except Exception as e:
            logging.error(f"Parsing error during TBEA scan: {e}")
        return None

    def scrape_ctb_hybrid(self):
        # V8 STRATEGY: Same as TBEA. Render, Wait, Parse, with Blind Fallback.
        BASE_URL = "https://ceylonteabrokers.com/market-reports/"
        driver = self.initialize_driver()
        page_source = ""
        try:
            logging.info("Loading CTB page with Selenium...")
            driver.get(BASE_URL)
            # Using a robust XPath to find relevant PDF links
            latest_report_button_xpath = "//a[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'report') and substring(@href, string-length(@href) - 3) = '.pdf']"
            logging.info(f"Waiting for dynamic content (Max wait: {DYNAMIC_TIMEOUT}s)...")
            WebDriverWait(driver, DYNAMIC_TIMEOUT).until(
                EC.visibility_of_element_located((By.XPATH, latest_report_button_xpath))
            )
            logging.info("Dynamic content detected.")
            page_source = driver.page_source
        except TimeoutException:
             # V8: Blind Fallback
            logging.warning("Timeout waiting for dynamic content. Attempting Blind Fallback parsing.")
            if driver.page_source:
                page_source = driver.page_source
        except Exception as e:
            logging.error(f"Selenium error during CTB scan: {e}")
            return None

        if not page_source:
            logging.error("CTB scan failed: No page source captured.")
            return None

        # BeautifulSoup Parsing
        try:
            logging.info("Parsing source with BeautifulSoup...")
            soup = BeautifulSoup(page_source, 'html.parser')

            # We look for links ending in .pdf that contain the word "Report"
            potential_links = []
            for link in soup.select("a[href$='.pdf']"):
                text = link.get_text(strip=True).lower()
                if "report" in text:
                    potential_links.append(link)

            if potential_links:
                # Assuming the first relevant link found is the latest
                link_element = potential_links[0]
                report_url = urljoin(BASE_URL, link_element['href'])
                title_text = link_element.get_text(strip=True) or f"CTB_Weekly_Report_{datetime.now().strftime('%Y%m%d')}"
                logging.info(f"Found latest CTB report: {title_text}")
                return {"source": "CTB", "title": title_text, "url": report_url}
            else:
                logging.warning("Could not find report links in CTB page source.")
        except Exception as e:
            logging.error(f"Parsing error during CTB scan: {e}")
        return None

    def scrape_atb_ltd_iframe_extraction(self):
        # V8 STRATEGY: Use Selenium to load the page and extract the iframe src. High timeout is key.
        BASE_URL = "https://www.atbltd.com/Docs/current_market_report"
        driver = self.initialize_driver()
        try:
            logging.info("Loading ATB page with Selenium...")
            driver.get(BASE_URL)
            logging.info(f"Locating ATB iframe (Max wait: {DYNAMIC_TIMEOUT}s)...")
            # V8: Wait for the iframe to be present in the DOM
            iframe = WebDriverWait(driver, DYNAMIC_TIMEOUT).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "iframe[src*='Viewer']"))
            )
            iframe_src = iframe.get_attribute("src")
            logging.info(f"Extracted iframe src: {iframe_src}")
            
            # Extract the PDF path from the iframe source URL parameters
            parsed_src = urlparse(iframe_src)
            query_params = parse_qs(parsed_src.query)
            pdf_path = query_params.get('file', [None])[0] or query_params.get('src', [None])[0]
            
            if pdf_path:
                direct_pdf_url = urljoin(BASE_URL, pdf_path)
                logging.info(f"Constructed direct PDF URL: {direct_pdf_url}")
                title = f"ATB_Market_Report_{datetime.now().strftime('%Y%m%d')}"
                return {"source": "ATB", "title": title, "url": direct_pdf_url}
            else:
                logging.warning("Could not extract PDF path from iframe src.")
        except TimeoutException:
            logging.error("Iframe extraction failed on ATB Ltd: Timeout waiting for iframe.")
        except Exception as e:
            logging.error(f"Iframe extraction failed on ATB Ltd: {e}")
        return None

if __name__ == "__main__":
    # V8: Ensure the directory path is correct for the execution environment
    harvester = ReportHarvesterV8(inbox_dir="Inbox")
    harvester.run()