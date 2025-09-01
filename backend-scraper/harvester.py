import requests
from bs4 import BeautifulSoup
import re
import logging
import os
from datetime import datetime
from urllib.parse import urljoin

# Configure basic logging for GitHub Actions output
logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')

class ReportHarvester:
    """
    Acquires the latest market reports from target broker websites.
    """
    def __init__(self, inbox_dir="Inbox"):
        self.session = requests.Session()
        self.session.headers.update({
            # Identify the bot professionally
            'User-Agent': 'TeaTrade-Harvester/1.0 (https://teatrade.co.uk; contact@teatrade.co.uk)'
        })
        # Define the directory relative to the repository root
        self.inbox_dir = inbox_dir
        # Ensure the Inbox directory exists
        if not os.path.exists(inbox_dir):
            os.makedirs(inbox_dir)
            logging.info(f"Created Inbox directory: {inbox_dir}")

    def run(self):
        logging.info("Starting TeaTrade Report Harvester...")
        
        # Define the scraping targets and their respective methods
        targets = {
            "TBEA": self.scrape_tbea,
            "CeylonTeaBrokers": self.scrape_ceylon_tea_brokers,
            "ForbesWalker": self.scrape_forbes_walker,
            # Note: EATTA often requires complex interaction; NBL (Bangladesh) deferred for V2.
        }

        for name, method in targets.items():
            try:
                logging.info(f"Scanning {name}...")
                report_info = method()
                if report_info:
                    self.download_report(report_info)
                else:
                    logging.warning(f"Could not find the latest report for {name}.")
            except Exception as e:
                logging.error(f"Error harvesting {name}: {e}")

        logging.info("Harvester finished.")

    def sanitize_filename(self, title):
        """Cleans the title to create a safe filename."""
        # Remove unsafe characters and limit length
        filename = re.sub(r'[<>:"/\\|?*]', '', title)
        filename = filename.replace(' ', '_')[:150]
        return filename

    def download_report(self, report_info):
        """Downloads the report file into the Inbox."""
        url = report_info['url']
        title = report_info['title']
        source = report_info['source']
        
        # Determine file extension (assuming PDF for most brokers)
        extension = os.path.splitext(url)[1]
        if not extension or len(extension) > 5:
            extension = ".pdf" 

        filename = f"{source}_{self.sanitize_filename(title)}{extension}"
        filepath = os.path.join(self.inbox_dir, filename)

        # Check if file already exists (to prevent re-downloading the same report)
        if os.path.exists(filepath):
            logging.info(f"Report already exists, skipping download: {filename}")
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

    def scrape_tbea(self):
        # Scrapes https://www.tbeal.net/tbea-market-report/
        BASE_URL = "https://www.tbeal.net/tbea-market-report/"
        response = self.session.get(BASE_URL)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        report_regex = re.compile(r"Market Report.*Sale\s*\d+", re.IGNORECASE)

        # Find the first matching link (assuming it's the latest)
        for link in soup.find_all('a', href=True):
            text = link.get_text(strip=True)
            if report_regex.search(text) and '.pdf' in link['href'].lower():
                report_url = urljoin(BASE_URL, link['href'])
                return {"source": "TBEA", "title": text, "url": report_url}
        return None

    def scrape_ceylon_tea_brokers(self):
        # Scrapes https://ceylonteabrokers.com/all-market-reports/
        BASE_URL = "https://ceylonteabrokers.com/all-market-reports/"
        response = self.session.get(BASE_URL)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')
        report_regex = re.compile(r"Weekly Tea Market Report", re.IGNORECASE)

        for link in soup.find_all('a', href=True):
            text = link.get_text(strip=True)
            if report_regex.search(text) and '.pdf' in link['href'].lower():
                # Filter out noise links
                if "adobe" in text.lower() or "reader" in text.lower():
                    continue
                report_url = urljoin(BASE_URL, link['href'])
                return {"source": "CTB", "title": text, "url": report_url}
        return None

    def scrape_forbes_walker(self):
        # Scrapes https://web.forbestea.com/market-reports using their API endpoint
        # API endpoint is more reliable than HTML scraping
        API_URL = "https://web.forbestea.com/api/reports?page=1&search=&year=&category_id="
        logging.info(f"Querying Forbes & Walker API: {API_URL}")
        
        response = self.session.get(API_URL)
        response.raise_for_status()
        data = response.json()

        # The API returns a list of reports; the first item is the latest
        if 'data' in data and data['data']:
            latest_report = data['data'][0]
            title = latest_report.get('title', 'ForbesWalker_Market_Report')
            # The URL is nested within the 'file' object
            report_url = latest_report.get('file', {}).get('url')
            
            if report_url:
                 return {"source": "ForbesWalker", "title": title, "url": report_url}
        return None

# Entry point for the script
if __name__ == "__main__":
    # The script expects to run from the root of the repository (default for GitHub Actions).
    harvester = ReportHarvester(inbox_dir="Inbox")
    harvester.run()
