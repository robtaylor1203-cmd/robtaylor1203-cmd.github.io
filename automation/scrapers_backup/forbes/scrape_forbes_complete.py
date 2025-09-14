#!/usr/bin/env python3
"""
Forbes Tea Complete Scraper - Enterprise Grade
Direct auction data parsing from Forbes Tea
"""

import json
import sys
import time
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import requests
from bs4 import BeautifulSoup
import re

# Add utils to path
sys.path.append(str(Path(__file__).parent.parent.parent))
from utils.pipeline_utils import (
    setup_logging, standardize_data_format, save_to_database,
    generate_weekly_reports, safe_int, safe_float, clean_text
)

# Configuration
BASE_URL = "https://forbestea.com"
AUCTION_URL = f"{BASE_URL}/auction-results"

class ForbesTeaScraper:
    """Enterprise Forbes Tea auction scraper"""
    
    def __init__(self):
        self.logger = setup_logging('FORBES')
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'
        })
        self.scraped_data = []
    
    def get_auction_dates(self) -> List[str]:
        """Get available auction dates"""
        
        dates = []
        
        try:
            self.logger.info("📅 Fetching available auction dates")
            
            response = self.session.get(AUCTION_URL, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Look for date selectors or links
            date_elements = soup.find_all(['select', 'option', 'a'], text=re.compile(r'\d{4}-\d{2}-\d{2}|\d{2}/\d{2}/\d{4}'))
            
            for element in date_elements:
                date_text = element.get_text().strip()
                parsed_date = self.parse_date(date_text)
                if parsed_date:
                    dates.append(parsed_date)
            
            # If no dates found, generate recent dates
            if not dates:
                end_date = datetime.now()
                for i in range(30):
                    date = end_date - timedelta(days=i)
                    dates.append(date.strftime('%Y-%m-%d'))
            
            self.logger.info(f"📅 Found {len(dates)} auction dates")
            
        except Exception as e:
            self.logger.error(f"❌ Error fetching auction dates: {e}")
        
        return dates[:30]  # Limit to recent 30 days
    
    def parse_date(self, date_text: str) -> Optional[str]:
        """Parse date from various formats"""
        
        date_patterns = [
            r'(\d{4})-(\d{2})-(\d{2})',
            r'(\d{2})/(\d{2})/(\d{4})',
            r'(\d{1,2})\s*(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s*(\d{4})'
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, date_text)
            if match:
                try:
                    groups = match.groups()
                    if len(groups) == 3:
                        if groups[1].isalpha():  # Month name
                            month_map = {
                                'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4,
                                'may': 5, 'jun': 6, 'jul': 7, 'aug': 8,
                                'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
                            }
                            month = month_map.get(groups[1][:3].lower())
                            if month:
                                return f"{groups[2]}-{month:02d}-{int(groups[0]):02d}"
                        else:
                            # Check format
                            if len(groups[0]) == 4:  # YYYY-MM-DD
                                return f"{groups[0]}-{groups[1]}-{groups[2]}"
                            else:  # DD/MM/YYYY
                                return f"{groups[2]}-{int(groups[1]):02d}-{int(groups[0]):02d}"
                except:
                    continue
        
        return None
    
    def scrape_auction_data(self, date: str) -> List[Dict]:
        """Scrape auction data for specific date"""
        
        lots = []
        
        try:
            self.logger.info(f"📊 Scraping Forbes auction data for {date}")
            
            # Try different URL patterns
            url_patterns = [
                f"{AUCTION_URL}?date={date}",
                f"{AUCTION_URL}/{date}",
                f"{BASE_URL}/auctions/{date}",
                AUCTION_URL  # Fallback to main page
            ]
            
            for url in url_patterns:
                try:
                    response = self.session.get(url, timeout=30)
                    
                    if response.status_code == 200:
                        soup = BeautifulSoup(response.content, 'html.parser')
                        
                        # Extract from tables
                        table_lots = self.extract_from_tables(soup, date)
                        lots.extend(table_lots)
                        
                        # Extract from structured text
                        text_lots = self.extract_from_text(soup.get_text(), date)
                        lots.extend(text_lots)
                        
                        if lots:
                            break
                
                except Exception as url_error:
                    continue
            
            # Remove duplicates
            unique_lots = self.remove_duplicates(lots)
            
            self.logger.info(f"📈 Forbes {date}: {len(unique_lots)} lots")
            
        except Exception as e:
            self.logger.warning(f"⚠️ Error scraping Forbes {date}: {e}")
        
        return lots
    
    def extract_from_tables(self, soup: BeautifulSoup, date: str) -> List[Dict]:
        """Extract auction data from HTML tables"""
        
        lots = []
        
        try:
            tables = soup.find_all('table')
            
            for table in tables:
                rows = table.find_all('tr')
                
                # Skip header rows
                data_rows = rows[1:] if rows else []
                
                for row in data_rows:
                    cells = row.find_all(['td', 'th'])
                    
                    if len(cells) >= 4:
                        try:
                            lot_data = {
                                'garden': clean_text(cells[0].get_text()),
                                'grade': clean_text(cells[1].get_text()),
                                'quantity': safe_int(cells[2].get_text()),
                                'price': safe_float(cells[3].get_text()),
                                'auction_date': date,
                                'source': 'FORBES'
                            }
                            
                            # Additional fields if available
                            if len(cells) > 4:
                                lot_data['buyer'] = clean_text(cells[4].get_text())
                            if len(cells) > 5:
                                lot_data['seller'] = clean_text(cells[5].get_text())
                            
                            # Validate essential data
                            if (lot_data['garden'] and 
                                lot_data['quantity'] and lot_data['quantity'] > 0 and
                                lot_data['price'] and lot_data['price'] > 0):
                                
                                lots.append(lot_data)
                        
                        except Exception as row_error:
                            continue
        
        except Exception as e:
            self.logger.warning(f"Table extraction error: {e}")
        
        return lots
    
    def extract_from_text(self, content: str, date: str) -> List[Dict]:
        """Extract auction data from text content"""
        
        lots = []
        
        try:
            # Common patterns for tea auction data
            patterns = [
                r'([A-Z][A-Za-z\s]+Estate)\s+([A-Z]+)\s+(\d+)\s+(\d+\.?\d*)',
                r'([A-Z][A-Za-z\s]+)\s+([A-Z]{2,})\s+(\d+)\s+Kgs?\s+(\d+\.?\d*)',
                r'Estate:\s*([A-Za-z\s]+)\s+Grade:\s*([A-Z]+)\s+Qty:\s*(\d+)\s+Price:\s*(\d+\.?\d*)'
            ]
            
            for pattern in patterns:
                matches = re.finditer(pattern, content, re.IGNORECASE)
                
                for match in matches:
                    try:
                        lot_data = {
                            'garden': clean_text(match.group(1)),
                            'grade': clean_text(match.group(2)),
                            'quantity': safe_int(match.group(3)),
                            'price': safe_float(match.group(4)),
                            'auction_date': date,
                            'source': 'FORBES'
                        }
                        
                        if (lot_data['garden'] and 
                            lot_data['quantity'] and lot_data['quantity'] > 0 and
                            lot_data['price'] and lot_data['price'] > 0):
                            
                            lots.append(lot_data)
                    
                    except:
                        continue
        
        except Exception as e:
            self.logger.warning(f"Text extraction error: {e}")
        
        return lots
    
    def remove_duplicates(self, lots: List[Dict]) -> List[Dict]:
        """Remove duplicate auction lots"""
        
        seen = set()
        unique_lots = []
        
        for lot in lots:
            # Create unique key
            key = f"{lot['garden']}_{lot['grade']}_{lot['quantity']}_{lot['price']}"
            
            if key not in seen:
                seen.add(key)
                unique_lots.append(lot)
        
        return unique_lots
    
    def run_complete_scrape(self) -> bool:
        """Execute complete Forbes Tea scraping"""
        
        try:
            self.logger.info("🚀 Starting Forbes Tea COMPLETE scraping")
            
            # Get auction dates
            dates = self.get_auction_dates()
            
            if not dates:
                self.logger.warning("⚠️ No auction dates found")
                return False
            
            all_lots = []
            
            # Scrape each date
            for date in dates[:15]:  # Limit to prevent overload
                lots = self.scrape_auction_data(date)
                all_lots.extend(lots)
                time.sleep(2)  # Rate limiting
            
            if all_lots:
                # Remove global duplicates
                unique_lots = self.remove_duplicates(all_lots)
                
                # Standardize data format
                standardized_data = [
                    standardize_data_format(lot, 'FORBES')['processed_data'] 
                    for lot in unique_lots
                ]
                
                # Save to database
                if save_to_database(standardized_data, 'auction_lots', 'FORBES'):
                    self.scraped_data = standardized_data
                    
                    self.logger.info(f"✅ Forbes complete: {len(standardized_data)} lots saved")
                    return True
            
            self.logger.warning("⚠️ No valid auction data found")
            return False
            
        except Exception as e:
            self.logger.error(f"❌ Forbes complete scrape failed: {e}")
            return False

def main():
    """Main execution function"""
    scraper = ForbesTeaScraper()
    success = scraper.run_complete_scrape()
    
    if success:
        print("✅ Forbes Tea scraping completed successfully")
        return 0
    else:
        print("❌ Forbes Tea scraping failed")
        return 1

if __name__ == "__main__":
    exit(main())
