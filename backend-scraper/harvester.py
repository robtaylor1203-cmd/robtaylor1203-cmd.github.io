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
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# V7 Configuration: Generous timeouts
PAGE_LOAD_TIMEOUT = 180 # 3 minutes for a page to load
DYNAMIC_TIMEOUT = 90  # 1.5 minutes for an element to appear

class ReportHarvesterV7:
    """
    Acquires market reports using an Intelligent Hybrid approach, tailored to each site.
    """
    def __init__(self, inbox_dir="Inbox"):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
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
        logging.info("Initializing Headless Chrome WebDriver (V7)...")
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--window-size=1920,1080")
        
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
        logging.info("Starting TeaTrade Report Harvester (V7 Intelligent Hybrid)...")
        targets = {
            "ForbesWalker (API First)": self.scrape_forbes_walker_api_v2,
            "TBEA (Hybrid Render-Parse)": self.scrape_tbea_hybrid,
            "CeylonTeaBrokers (Hybrid Render-Parse)": self.scrape_ctb_hybrid,
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
                logging.error(f"Error harvesting {name}: {e}", exc_info=False)
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
            response = self.session.get(url, stream=True, timeout=120)
            response.raise_for_status()
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            logging.info(f"Successfully downloaded and saved to: {filepath}")
        except Exception as e:
            logging.error(f"Failed to download report from {url}: {e}")

    # --- V7: Site-Specific Hybrid Strategies ---

    def scrape_forbes_walker_api_v2(self):
        # V7 STRATEGY: The API is the fastest route. The endpoint has changed. This uses the new, correct one.
        API_URL = "https://web.forbestea.com/api/get-reports-by-category/1" # '1' is the category for Market Reports
        logging.info(f"Querying Forbes & Walker API V2: {API_URL}")
        try:
            headers = self.session.headers.copy()
            headers['Referer'] = 'https://web.forbestea.com/market-reports'
            headers['Accept'] = 'application/json'
            response = self.session.get(API_URL, headers=headers, timeout=45)
            response.raise_for_status()
            data = response.json()
            if 'reports' in data and data['reports']:
                latest_report = data['reports'][0]
                title = latest_report.get('title', 'ForbesWalker_Market_Report')
                report_url = latest_report.get('file', {}).get('url')
                if report_url:
                    return {"source": "ForbesWalker", "title": title, "url": report_url}
        except Exception as e:
            logging.error(f"API request failed on Forbes & Walker: {e}")
        return None

    def scrape_tbea_hybrid(self):
        # V7 STRATEGY: Use Selenium to render the slow JS, then dump the page source for fast parsing.
        BASE_URL = "https://www.tbeal.net/tbea-market-report/"
        driver = self.initialize_driver()
        try:
            logging.info("Loading TBEA page with Selenium...")
            driver.get(BASE_URL)
            WebDriverWait(driver, DYNAMIC_TIMEOUT).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='.pdf']"))
            )
            logging.info("Page rendered. Parsing source with BeautifulSoup...")
            page_source = driver.page_source
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
                            found_reports.append({"sale": sale_number, "title": text, "url": urljoin(BASE_URL, href)})
                        except ValueError:
                            continue
            if found_reports:
                latest_report = sorted(found_reports, key=lambda x: x['sale'], reverse=True)[0]
                logging.info(f"Found latest TBEA report: Sale {latest_report['sale']}")
                return {"source": "TBEA", "title": latest_report['title'], "url": latest_report['url']}
        except Exception as e:
            logging.error(f"Hybrid scan failed on TBEA: {e}")
        return None

    def scrape_ctb_hybrid(self):
        # V7 STRATEGY: Same as TBEA. Render with Selenium, parse with BeautifulSoup.
        BASE_URL = "https://ceylonteabrokers.com/market-reports/"
        driver = self.initialize_driver()
        try:
            logging.info("Loading CTB page with Selenium...")
            driver.get(BASE_URL)
            latest_report_button_xpath = "//a[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'report') and substring(@href, string-length(@href) - 3) = '.pdf']"
            WebDriverWait(driver, DYNAMIC_TIMEOUT).until(
                EC.visibility_of_element_located((By.XPATH, latest_report_button_xpath))
            )
            logging.info("Page rendered. Parsing source...")
            page_source = driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')

            # Re-find the element in the static soup for robustness
            link_element = soup.select_one("a[href$='.pdf']") # A simpler but effective selector
            if link_element:
                report_url = urljoin(BASE_URL, link_element['href'])
                title_text = link_element.get_text(strip=True) or f"CTB_Weekly_Report_{datetime.now().strftime('%Y%m%d')}"
                return {"source": "CTB", "title": title_text, "url": report_url}
        except Exception as e:
            logging.error(f"Hybrid scan failed on Ceylon Tea Brokers: {e}")
        return None

    def scrape_atb_ltd_iframe_extraction(self):
        # V7 STRATEGY: Use Selenium to load the page and extract the iframe src. High timeout is key.
        BASE_URL = "https://www.atbltd.com/Docs/current_market_report"
        driver = self.initialize_driver()
        try:
            logging.info("Loading ATB page with Selenium...")
            driver.get(BASE_URL)
            logging.info("Locating ATB iframe...")
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
        except Exception as e:
            logging.error(f"Iframe extraction failed on ATB Ltd: {e}")
        return None

if __name__ == "__main__":
    harvester = ReportHarvesterV7(inbox_dir="Inbox")
    harvester.run()