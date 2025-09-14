#!/usr/bin/env python3
"""
J. Thomas Complete Scraper - Enterprise Grade
Scrapes ALL auction centres, ALL sales, ALL data
Optimized for Chromebook Linux with comprehensive error handling
"""

import json
import sys
import time
from pathlib import Path
from playwright.sync_api import sync_playwright, TimeoutError
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import re

# Add utils to path
sys.path.append(str(Path(__file__).parent.parent.parent))
from utils.pipeline_utils import (
    setup_logging, standardize_data_format, save_to_database,
    generate_weekly_reports, safe_int, safe_float
)

# Configuration
BASE_URL = "https://jthomasindia.com"
AUCTION_URL = f"{BASE_URL}/auction_prices.php"

# Chromebook optimized settings
MAX_TIMEOUT = 300000  # 5 minutes
PAGE_TIMEOUT = 60000  # 1 minute
USER_AGENT = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'

# All auction centres
AUCTION_CENTRES = {
    'Kolkata': {'id': 1, 'currency': 'INR'},
    'Guwahati': {'id': 2, 'currency': 'INR'},
    'Siliguri': {'id': 3, 'currency': 'INR'},
    'Coimbatore': {'id': 4, 'currency': 'INR'},
    'Kochi': {'id': 5, 'currency': 'INR'},
    'Cochin': {'id': 6, 'currency': 'INR'}
}

class JThomasCompleteScraper:
    """Enterprise-grade J. Thomas auction data scraper"""
    
    def __init__(self):
        self.logger = setup_logging('J_THOMAS')
        self.browser = None
        self.context = None
        self.page = None
        self.scraped_data = []
        self.total_records = 0
        
    def initialize_browser(self) -> bool:
        """Initialize Playwright browser with Chromebook optimizations"""
        try:
            self.logger.info("🚀 Initializing browser for J Thomas scraping")
            
            playwright = sync_playwright().start()
            
            # Chromebook-optimized browser settings
            self.browser = playwright.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-gpu',
                    '--disable-web-security',
                    '--disable-background-timer-throttling',
                    '--disable-renderer-backgrounding',
                    '--disable-backgrounding-occluded-windows',
                    '--memory-pressure-off'
                ]
            )
            
            # Create context with timeout settings
            self.context = self.browser.new_context(
                user_agent=USER_AGENT,
                viewport={'width': 1366, 'height': 768},
                ignore_https_errors=True
            )
            
            # Set timeouts
            self.context.set_default_timeout(PAGE_TIMEOUT)
            self.context.set_default_navigation_timeout(MAX_TIMEOUT)
            
            self.page = self.context.new_page()
            
            self.logger.info("✅ Browser initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Browser initialization failed: {e}")
            return False
    
    def scrape_auction_centre(self, centre_name: str, centre_info: Dict) -> List[Dict]:
        """Scrape complete data for a specific auction centre"""
        
        centre_data = []
        
        try:
            self.logger.info(f"🎯 Scraping {centre_name} auction centre")
            
            # Navigate to auction page
            self.page.goto(AUCTION_URL, timeout=MAX_TIMEOUT)
            self.page.wait_for_load_state('networkidle', timeout=60000)
            
            # Select auction centre
            if self.page.locator('select[name="centre"]').is_visible():
                self.page.select_option('select[name="centre"]', value=str(centre_info['id']))
                time.sleep(2)
            
            # Get available dates (last 30 days)
            end_date = datetime.now()
            start_date = end_date - timedelta(days=30)
            
            current_date = start_date
            while current_date <= end_date:
                date_str = current_date.strftime('%Y-%m-%d')
                
                try:
                    # Set date
                    if self.page.locator('input[name="date"]').is_visible():
                        self.page.fill('input[name="date"]', date_str)
                        self.page.click('button[type="submit"]')
                        self.page.wait_for_load_state('networkidle', timeout=30000)
                    
                    # Extract auction data for this date
                    date_data = self.extract_auction_data(centre_name, date_str)
                    centre_data.extend(date_data)
                    
                    if date_data:
                        self.logger.info(f"📊 {centre_name} {date_str}: {len(date_data)} lots")
                    
                except Exception as date_error:
                    self.logger.warning(f"⚠️ Error scraping {centre_name} {date_str}: {date_error}")
                
                current_date += timedelta(days=1)
                time.sleep(1)  # Rate limiting
            
            self.logger.info(f"✅ {centre_name} complete: {len(centre_data)} total lots")
            
        except Exception as e:
            self.logger.error(f"❌ Error scraping {centre_name}: {e}")
        
        return centre_data
    
    def extract_auction_data(self, centre_name: str, date_str: str) -> List[Dict]:
        """Extract auction lot data from current page"""
        
        lots = []
        
        try:
            # Wait for table to load
            if not self.page.locator('table').is_visible(timeout=10000):
                return lots
            
            # Extract table rows
            rows = self.page.locator('table tbody tr').all()
            
            for row in rows:
                try:
                    cells = row.locator('td').all()
                    
                    if len(cells) >= 6:
                        lot_data = {
                            'location': centre_name,
                            'garden': cells[0].inner_text().strip(),
                            'grade': cells[1].inner_text().strip(),
                            'quantity': safe_int(cells[2].inner_text().strip()),
                            'price': safe_float(cells[3].inner_text().strip()),
                            'sale_no': safe_int(cells[4].inner_text().strip()) if len(cells) > 4 else None,
                            'lot_no': safe_int(cells[5].inner_text().strip()) if len(cells) > 5 else None,
                            'auction_date': date_str,
                            'currency': 'INR',
                            'source': 'J_THOMAS'
                        }
                        
                        # Validate essential data
                        if (lot_data['garden'] and 
                            lot_data['quantity'] and lot_data['quantity'] > 0 and
                            lot_data['price'] and lot_data['price'] > 0):
                            
                            lots.append(lot_data)
                
                except Exception as row_error:
                    continue
        
        except Exception as e:
            self.logger.warning(f"Data extraction error for {centre_name} {date_str}: {e}")
        
        return lots
    
    def run_complete_scrape(self) -> bool:
        """Execute complete scraping of all J Thomas data"""
        
        try:
            self.logger.info("🚀 Starting J Thomas COMPLETE scraping")
            
            if not self.initialize_browser():
                return False
            
            # Scrape all auction centres
            for centre_name, centre_info in AUCTION_CENTRES.items():
                centre_data = self.scrape_auction_centre(centre_name, centre_info)
                
                if centre_data:
                    # Standardize data format
                    standardized_data = [
                        standardize_data_format(lot, 'J_THOMAS')['processed_data'] 
                        for lot in centre_data
                    ]
                    
                    # Save to database
                    if save_to_database(standardized_data, 'auction_lots', 'J_THOMAS'):
                        self.total_records += len(standardized_data)
                        self.scraped_data.extend(standardized_data)
                    
                time.sleep(5)  # Rate limiting between centres
            
            # Generate weekly reports
            generate_weekly_reports()
            
            self.logger.info(f"✅ J Thomas COMPLETE scraping finished")
            self.logger.info(f"📊 Total records: {self.total_records}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Complete scrape failed: {e}")
            return False
        
        finally:
            self.close_browser()
    
    def close_browser(self):
        """Clean browser resources"""
        try:
            if self.page:
                self.page.close()
            if self.context:
                self.context.close()
            if self.browser:
                self.browser.close()
        except:
            pass

def main():
    """Main execution function"""
    scraper = JThomasCompleteScraper()
    success = scraper.run_complete_scrape()
    
    if success:
        print("✅ J Thomas complete scraping finished successfully")
        return 0
    else:
        print("❌ J Thomas scraping failed")
        return 1

if __name__ == "__main__":
    exit(main())
