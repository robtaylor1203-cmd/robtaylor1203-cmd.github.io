#!/usr/bin/env python3
"""
Ceylon Tea Brokers Complete Scraper - Enterprise Grade
Comprehensive data extraction from Ceylon Tea Brokers reports
"""

import json
import sys
import time
import re
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import requests
from bs4 import BeautifulSoup

# Add utils to path
sys.path.append(str(Path(__file__).parent.parent.parent))
from utils.pipeline_utils import (
    setup_logging, standardize_data_format, save_to_database,
    generate_weekly_reports, safe_int, safe_float, clean_text
)

# Configuration
BASE_URL = "https://ceylonteabrokers.com"
REPORTS_URL = f"{BASE_URL}/reports"

class CeylonTeaBrokersScraper:
    """Enterprise Ceylon Tea Brokers scraper"""
    
    def __init__(self):
        self.logger = setup_logging('CEYLON')
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'
        })
        self.scraped_data = []
        
    def get_available_reports(self) -> List[Dict]:
        """Get list of available reports"""
        
        reports = []
        
        try:
            self.logger.info("🔍 Fetching Ceylon Tea Brokers reports")
            
            response = self.session.get(REPORTS_URL, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find report links
            for link in soup.find_all('a', href=True):
                href = link.get('href')
                text = link.get_text().strip()
                
                if any(keyword in text.lower() for keyword in ['auction', 'sale', 'report', 'weekly']):
                    reports.append({
                        'url': href if href.startswith('http') else f"{BASE_URL}/{href}",
                        'title': text,
                        'date': self.extract_date_from_title(text)
                    })
            
            self.logger.info(f"📋 Found {len(reports)} potential reports")
            
        except Exception as e:
            self.logger.error(f"❌ Error fetching reports: {e}")
        
        return reports
    
    def extract_date_from_title(self, title: str) -> Optional[str]:
        """Extract date from report title"""
        
        # Common date patterns in Ceylon reports
        date_patterns = [
            r'(\d{1,2})\s*[-/]\s*(\d{1,2})\s*[-/]\s*(\d{4})',
            r'(\d{4})\s*[-/]\s*(\d{1,2})\s*[-/]\s*(\d{1,2})',
            r'(\d{1,2})\s*(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s*(\d{4})'
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, title, re.IGNORECASE)
            if match:
                try:
                    if len(match.groups()) == 3:
                        if match.group(2).isalpha():  # Month name format
                            month_map = {
                                'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4,
                                'may': 5, 'jun': 6, 'jul': 7, 'aug': 8,
                                'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
                            }
                            month = month_map.get(match.group(2)[:3].lower())
                            if month:
                                return f"{match.group(3)}-{month:02d}-{int(match.group(1)):02d}"
                        else:
                            # Numeric date format
                            return f"{match.group(3)}-{int(match.group(2)):02d}-{int(match.group(1)):02d}"
                except:
                    continue
        
        return None
    
    def scrape_report(self, report: Dict) -> List[Dict]:
        """Scrape individual report for auction data"""
        
        lots = []
        
        try:
            self.logger.info(f"📄 Scraping report: {report['title']}")
            
            response = self.session.get(report['url'], timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract auction data from tables
            tables = soup.find_all('table')
            
            for table in tables:
                rows = table.find_all('tr')
                
                for row in rows[1:]:  # Skip header
                    cells = row.find_all(['td', 'th'])
                    
                    if len(cells) >= 5:
                        try:
                            lot_data = {
                                'estate': clean_text(cells[0].get_text()),
                                'grade': clean_text(cells[1].get_text()),
                                'quantity': safe_int(cells[2].get_text()),
                                'price': safe_float(cells[3].get_text()),
                                'sale_date': report.get('date') or datetime.now().strftime('%Y-%m-%d'),
                                'sale_no': safe_int(cells[4].get_text()) if len(cells) > 4 else None,
                                'lot_no': safe_int(cells[5].get_text()) if len(cells) > 5 else None,
                                'source': 'CEYLON'
                            }
                            
                            # Validate data
                            if (lot_data['estate'] and 
                                lot_data['quantity'] and lot_data['quantity'] > 0 and
                                lot_data['price'] and lot_data['price'] > 0):
                                
                                lots.append(lot_data)
                        
                        except Exception as row_error:
                            continue
            
            # Also extract from text content (for PDF-like reports)
            text_content = soup.get_text()
            text_lots = self.extract_from_text_content(text_content, report.get('date'))
            lots.extend(text_lots)
            
            self.logger.info(f"📊 Extracted {len(lots)} lots from report")
            
        except Exception as e:
            self.logger.warning(f"⚠️ Error scraping report {report['title']}: {e}")
        
        return lots
    
    def extract_from_text_content(self, content: str, date: str) -> List[Dict]:
        """Extract auction data from text content"""
        
        lots = []
        
        try:
            # Pattern for estate/garden data
            estate_pattern = r'([A-Z][A-Za-z\s]+)\s+([A-Z]+)\s+(\d+)\s+(\d+\.?\d*)'
            
            matches = re.finditer(estate_pattern, content)
            
            for match in matches:
                try:
                    lot_data = {
                        'estate': clean_text(match.group(1)),
                        'grade': clean_text(match.group(2)),
                        'quantity': safe_int(match.group(3)),
                        'price': safe_float(match.group(4)),
                        'sale_date': date or datetime.now().strftime('%Y-%m-%d'),
                        'source': 'CEYLON'
                    }
                    
                    if (lot_data['estate'] and 
                        lot_data['quantity'] and lot_data['quantity'] > 0 and
                        lot_data['price'] and lot_data['price'] > 0):
                        
                        lots.append(lot_data)
                
                except:
                    continue
        
        except Exception as e:
            self.logger.warning(f"Text extraction error: {e}")
        
        return lots
    
    def run_complete_scrape(self) -> bool:
        """Execute complete Ceylon Tea Brokers scraping"""
        
        try:
            self.logger.info("🚀 Starting Ceylon Tea Brokers COMPLETE scraping")
            
            # Get available reports
            reports = self.get_available_reports()
            
            if not reports:
                self.logger.warning("⚠️ No reports found")
                return False
            
            # Scrape recent reports (last 30 days)
            recent_reports = [
                r for r in reports 
                if r.get('date') and 
                (datetime.now() - datetime.strptime(r['date'], '%Y-%m-%d')).days <= 30
            ]
            
            if not recent_reports:
                recent_reports = reports[:10]  # Take first 10 if no recent dates
            
            all_lots = []
            
            for report in recent_reports[:15]:  # Limit to prevent overload
                lots = self.scrape_report(report)
                all_lots.extend(lots)
                time.sleep(2)  # Rate limiting
            
            if all_lots:
                # Standardize data format
                standardized_data = [
                    standardize_data_format(lot, 'CEYLON')['processed_data'] 
                    for lot in all_lots
                ]
                
                # Save to database
                if save_to_database(standardized_data, 'auction_lots', 'CEYLON'):
                    self.scraped_data = standardized_data
                    
                    self.logger.info(f"✅ Ceylon complete: {len(standardized_data)} lots saved")
                    return True
            
            self.logger.warning("⚠️ No valid auction data found")
            return False
            
        except Exception as e:
            self.logger.error(f"❌ Ceylon complete scrape failed: {e}")
            return False

def main():
    """Main execution function"""
    scraper = CeylonTeaBrokersScraper()
    success = scraper.run_complete_scrape()
    
    if success:
        print("✅ Ceylon Tea Brokers scraping completed successfully")
        return 0
    else:
        print("❌ Ceylon Tea Brokers scraping failed")
        return 1

if __name__ == "__main__":
    exit(main())
