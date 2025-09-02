import requests
from bs4 import BeautifulSoup
import re
import logging
import os
from datetime import datetime
from urllib.parse import urljoin, urlparse, parse_qs

# Configure basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# V5 Configuration: Timeout for static requests
STATIC_TIMEOUT = 45 

class ReportHarvesterV5:
    """
    Acquires the latest market reports using a hyper-optimized static scraping approach (Static Blitz).
    """
    def __init__(self, inbox_dir="Inbox"):
        self.session = requests.Session()
        # Using a standard browser User-Agent to avoid basic bot detection
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        })
        self.inbox_dir = inbox_dir

        if not os.path.exists(inbox_dir):
            os.makedirs(inbox_dir)
        self.download_dir = os.path.abspath(inbox_dir)
        logging.info(f"Download directory set to: {self.download_dir}")

    def run(self):
        logging.info("Starting TeaTrade Report Harvester (V5 - Static Blitz)...")
        
        targets = {
            "TBEA (Static Scan)": self.scrape_tbea_static,
            "CeylonTeaBrokers (Static Scan)": self.scrape_ceylon_tea_brokers_static,
            "ForbesWalker (API)": self.scrape_forbes_walker_api,
            "ATB Ltd (Static Iframe Extraction)": self.scrape_atb_ltd_static_extraction,
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
            # Long timeout for the actual file download
            response = self.session.get(url, stream=True, timeout=120) 
            response.raise_for_status()
            
            with open(filepath, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            logging.info(f"Successfully downloaded and saved to: {filepath}")
        except Exception as e:
            logging.error(f"Failed to download report from {url}: {e}")

    # --- Scraper Implementations (V5 Static Blitz) ---

    def scrape_tbea_static(self):
        # V5 Strategy: Download HTML, parse all links, find the highest sale number.
        BASE_URL = "https://www.tbeal.net/tbea-market-report/"
        try:
            response = self.session.get(BASE_URL, timeout=STATIC_TIMEOUT)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')

            report_regex = re.compile(r"Market Report.*Sale\s*(\d+)", re.IGNORECASE)
            found_reports = []

            # Scan ALL links in the HTML (assumes links under tabs are present statically)
            for link in soup.find_all('a', href=True):
                text = link.get_text(strip=True)
                href = link['href']
                
                if '.pdf' in href.lower():
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
                # Sort by sale number descending and return the latest
                latest_report = sorted(found_reports, key=lambda x: x['sale'], reverse=True)[0]
                logging.info(f"Found latest TBEA report: Sale {latest_report['sale']}")
                return {"source": "TBEA", "title": latest_report['title'], "url": latest_report['url']}

        except requests.exceptions.RequestException as e:
            logging.error(f"Static request failed on TBEA: {e}")
        return None

    def scrape_ceylon_tea_brokers_static(self):
        # V5 Strategy: Download HTML, find the primary report link using BeautifulSoup selectors.
        BASE_URL = "https://ceylonteabrokers.com/market-reports/"
        try:
            response = self.session.get(BASE_URL, timeout=STATIC_TIMEOUT)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')

            # Look for links containing 'report' (case-insensitive) and ending in .pdf
            report_regex = re.compile(r"report", re.IGNORECASE)
            
            # Find all potential PDF links
            pdf_links = soup.find_all('a', href=re.compile(r'\.pdf$', re.IGNORECASE))
            
            for link in pdf_links:
                text = link.get_text(strip=True)
                if report_regex.search(text):
                    # Basic filtering for noise links
                    if "adobe" in text.lower() or "reader" in text.lower():
                        continue
                        
                    report_url = urljoin(BASE_URL, link['href'])
                    title_text = text
                    
                    if not title_text or len(title_text) < 10:
                        title_text = f"CTB_Weekly_Report_{datetime.now().strftime('%Y%m%d')}"
                        
                    # Return the first valid match found
                    return {"source": "CTB", "title": title_text, "url": report_url}

        except requests.exceptions.RequestException as e:
            logging.error(f"Static request failed on Ceylon Tea Brokers: {e}")
        return None

    def scrape_atb_ltd_static_extraction(self):
        # V5 Strategy: Download HTML, locate the iframe, extract the direct PDF URL from the src attribute.
        BASE_URL = "https://www.atbltd.com/Docs/current_market_report"
        try:
            response = self.session.get(BASE_URL, timeout=STATIC_TIMEOUT)
            response.raise_for_status()
            soup = BeautifulSoup(response.content, 'html.parser')

            logging.info("Locating ATB iframe in HTML...")
            # Find the iframe using a CSS selector (much faster than Selenium wait)
            iframe = soup.select_one("iframe[src*='Viewer']")

            if iframe and iframe.get('src'):
                iframe_src = iframe['src']
                logging.info(f"Extracted iframe src: {iframe_src}")

                # Extract the PDF path from the query parameter
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
            else:
                logging.error("Could not find the ATB iframe in the HTML.")

        except requests.exceptions.RequestException as e:
            logging.error(f"Static request failed on ATB Ltd: {e}")
        return None


    def scrape_forbes_walker_api(self):
        # V5 Strategy: Use the API endpoint. Add necessary headers (Referer) to ensure the request is accepted.
        API_URL = "https://web.forbestea.com/api/reports?page=1&search=&year=&category_id="
        logging.info(f"Querying Forbes & Walker API: {API_URL}")
        
        try:
            # Ensure headers mimic a request coming from the website itself
            headers = self.session.headers.copy()
            headers['Referer'] = 'https://web.forbestea.com/market-reports'
            headers['Accept'] = 'application/json'

            response = self.session.get(API_URL, headers=headers, timeout=STATIC_TIMEOUT)
            
            if not response.ok:
                # Log the specific error if the API fails (e.g., 404, 500)
                logging.error(f"Forbes Walker API request failed: {response.status_code} - {response.reason}")
                return None
                
            data = response.json()

            if 'data' in data and data['data']:
                latest_report = data['data'][0]
                title = latest_report.get('title', 'ForbesWalker_Market_Report')
                report_url = latest_report.get('file', {}).get('url')
                
                if report_url:
                    return {"source": "ForbesWalker", "title": title, "url": report_url}
            else:
                logging.warning("Forbes Walker API returned empty data.")

        except requests.exceptions.RequestException as e:
            logging.error(f"API request failed on Forbes & Walker: {e}")
        return None

# Entry point for the script
if __name__ == "__main__":
    harvester = ReportHarvesterV5(inbox_dir="Inbox")
    harvester.run()