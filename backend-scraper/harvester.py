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

# V9 Configuration: Final Resilience Timeouts
PAGE_LOAD_TIMEOUT = 180 
DYNAMIC_TIMEOUT = 200  # V9: Increased to 200s for maximum patience

class ReportHarvesterV9:
    """
    Acquires market reports using an Intelligent Hybrid approach, simulating a patient user.
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
        logging.info("Initializing Headless Chrome WebDriver (V9)...")
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1920,1080")
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
        logging.info("Starting TeaTrade Report Harvester (V9 Patient Human Simulation)...")
        targets = {
            "ForbesWalker (API V4 Spoof)": self.scrape_forbes_walker_api_v4,
            "TBEA (Patient Hybrid)": self.scrape_tbea_patient_hybrid,
            "CeylonTeaBrokers (Patient Hybrid)": self.scrape_ctb_patient_hybrid,
            "ATB Ltd (Patient Iframe Extraction)": self.scrape_atb_ltd_patient_iframe_extraction,
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
            response = self.session.get(url, stream=True, timeout=180) 
            response.raise_for_status()
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            logging.info(f"Successfully downloaded and saved to: {filepath}")
        except Exception as e:
            logging.error(f"Failed to download report from {url}: {e}")

    # --- V9: Site-Specific Patient Hybrid Strategies ---

    def scrape_forbes_walker_api_v4(self):
        # V9 STRATEGY: API First with enhanced, human-like headers.
        API_URL = "https://web.forbestea.com/api/get-reports-by-category/1"
        logging.info(f"Querying Forbes & Walker API V4: {API_URL}")
        try:
            headers = self.session.headers.copy()
            # V9: Enhance headers to look even more like a real browser request
            headers.update({
                'Accept': 'application/json, text/plain, */*',
                'Accept-Language': 'en-US,en;q=0.9',
                'Origin': 'https://web.forbestea.com',
                'Referer': 'https://web.forbestea.com/market-reports',
                'Sec-Fetch-Dest': 'empty',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Site': 'same-origin',
            })
            
            response = self.session.get(API_URL, headers=headers, timeout=60)
            response.raise_for_status()
            data = response.json()
            
            if 'reports' in data and data['reports']:
                latest_report = data['reports'][0] 
                title = latest_report.get('title', 'ForbesWalker_Market_Report')
                report_url = latest_report.get('file', {}).get('url')
                
                if report_url:
                    logging.info(f"Successfully retrieved API data for: {title}")
                    return {"source": "ForbesWalker", "title": title, "url": report_url}
        except Exception as e:
            logging.error(f"API request failed on Forbes & Walker: {e}")
        return None

    def _patient_render_and_parse(self, url, wait_condition, parser_function):
        # V9: Centralized function for the "Patient Render" strategy
        driver = self.initialize_driver()
        try:
            logging.info(f"Patiently loading page: {url}...")
            driver.get(url)
            
            # 1. Wait for the document to be technically "complete"
            WebDriverWait(driver, PAGE_LOAD_TIMEOUT).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )
            logging.info("Document readyState is complete.")

            # 2. Wait for a specific element that indicates JS has started rendering
            logging.info(f"Waiting for key content (Max wait: {DYNAMIC_TIMEOUT}s)...")
            WebDriverWait(driver, DYNAMIC_TIMEOUT).until(wait_condition)
            logging.info("Key content detected.")

            # 3. Add an explicit sleep as a final fallback for slow scripts
            logging.info("Adding 10s explicit wait for final rendering...")
            time.sleep(10)

            page_source = driver.page_source
            if not page_source or len(page_source) < 1000:
                 logging.error("Failed to capture a valid page source.")
                 return None

            return parser_function(page_source, url)

        except TimeoutException:
            logging.error("Timeout exceeded while waiting for page content.")
        except Exception as e:
            logging.error(f"Selenium error during patient render: {e}")
        return None

    def scrape_tbea_patient_hybrid(self):
        wait_condition = EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='.pdf']"))
        return self._patient_render_and_parse("https://www.tbeal.net/tbea-market-report/", wait_condition, self._parse_tbea_source)

    def _parse_tbea_source(self, page_source, base_url):
        logging.info("Parsing TBEA source with BeautifulSoup...")
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
                        full_url = urljoin(base_url, href)
                        found_reports.append({"sale": sale_number, "title": text, "url": full_url})
                    except ValueError: continue
        if found_reports:
            latest_report = sorted(found_reports, key=lambda x: x['sale'], reverse=True)[0]
            logging.info(f"Found latest TBEA report: Sale {latest_report['sale']}")
            return {"source": "TBEA", "title": latest_report['title'], "url": latest_report['url']}
        logging.warning("Could not find report links in TBEA page source.")
        return None

    def scrape_ctb_patient_hybrid(self):
        xpath = "//a[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'report') and substring(@href, string-length(@href) - 3) = '.pdf']"
        wait_condition = EC.visibility_of_element_located((By.XPATH, xpath))
        return self._patient_render_and_parse("https://ceylonteabrokers.com/market-reports/", wait_condition, self._parse_ctb_source)

    def _parse_ctb_source(self, page_source, base_url):
        logging.info("Parsing CTB source with BeautifulSoup...")
        soup = BeautifulSoup(page_source, 'html.parser')
        potential_links = []
        for link in soup.select("a[href$='.pdf']"):
            if "report" in link.get_text(strip=True).lower():
                potential_links.append(link)
        if potential_links:
            link_element = potential_links[0]
            report_url = urljoin(base_url, link_element['href'])
            title_text = link_element.get_text(strip=True) or f"CTB_Weekly_Report_{datetime.now().strftime('%Y%m%d')}"
            logging.info(f"Found latest CTB report: {title_text}")
            return {"source": "CTB", "title": title_text, "url": report_url}
        logging.warning("Could not find report links in CTB page source.")
        return None

    def scrape_atb_ltd_patient_iframe_extraction(self):
        # V9 STRATEGY: Use patient loading to ensure the iframe is rendered.
        BASE_URL = "https://www.atbltd.com/Docs/current_market_report"
        driver = self.initialize_driver()
        try:
            logging.info("Patiently loading ATB page...")
            driver.get(BASE_URL)
            logging.info(f"Locating ATB iframe (Max wait: {DYNAMIC_TIMEOUT}s)...")
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
        except TimeoutException:
            logging.error("Iframe extraction failed on ATB Ltd: Timeout waiting for iframe.")
        except Exception as e:
            logging.error(f"Iframe extraction failed on ATB Ltd: {e}")
        return None

if __name__ == "__main__":
    harvester = ReportHarvesterV9(inbox_dir="Inbox")
    harvester.run()