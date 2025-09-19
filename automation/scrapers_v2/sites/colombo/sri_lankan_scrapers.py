#!/usr/bin/env python3
"""Sri Lankan Tea Auction Scrapers"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
import requests
from bs4 import BeautifulSoup

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))

from core.advanced_scraper import AdvancedWebDriver, ScrapingResult, DataWarehouse, HumanBehaviorSimulator

class SriLankanTeaScrapers:
    def __init__(self, data_warehouse: DataWarehouse):
        self.data_warehouse = data_warehouse
        self.behavior_simulator = HumanBehaviorSimulator()
        
        self.sources = {
            'forbes_tea': 'https://web.forbestea.com/market-reports',
            'ceylon_brokers': 'https://ceylonteabrokers.com/market-reports/',
            'john_keells': 'https://johnkeellstea.com/market-reports-2/'
        }
        
    async def scrape_all_sri_lankan_sources(self) -> List[ScrapingResult]:
        results = []
        
        for source_name, url in self.sources.items():
            try:
                result = await self.scrape_source(source_name, url)
                if result:
                    results.append(result)
                self.behavior_simulator.random_delay(60, 180)
            except Exception as e:
                logging.error(f"Sri Lankan {source_name} failed: {e}")
                
        return results
        
    async def scrape_source(self, source_name: str, url: str) -> Optional[ScrapingResult]:
        try:
            headers = {
                'User-Agent': self.behavior_simulator.get_random_user_agent(),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
            }
            
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            extracted_data = self.extract_sri_lankan_data(soup, source_name)
            
            return ScrapingResult(
                source_url=url,
                auction_center=f"SriLanka_{source_name}",
                data_type="market_reports",
                timestamp=datetime.now(),
                raw_data=extracted_data,
                success=True
            )
            
        except Exception as e:
            logging.error(f"Sri Lankan scraping failed for {url}: {e}")
            return None
            
    def extract_sri_lankan_data(self, soup: BeautifulSoup, source_name: str) -> Dict[str, Any]:
        data = {
            'source': source_name,
            'page_title': soup.title.string if soup.title else '',
            'extraction_timestamp': datetime.now().isoformat()
        }
        
        # Extract tables
        tables = soup.find_all('table')
        for i, table in enumerate(tables):
            data[f'table_{i}'] = self.extract_table_data(table)
            
        # Extract report links
        report_links = soup.find_all('a', href=lambda x: x and any(word in x.lower() for word in ['report', 'pdf', 'doc']))
        for i, link in enumerate(report_links[:10]):
            data[f'report_link_{i}'] = {
                'url': link.get('href'),
                'text': link.get_text(strip=True)
            }
            
        return data
        
    def extract_table_data(self, table) -> List[Dict[str, str]]:
        rows = []
        
        try:
            headers = []
            header_row = table.find('thead')
            if header_row:
                header_cells = header_row.find_all(['th', 'td'])
                headers = [cell.get_text(strip=True) for cell in header_cells]
            
            tbody = table.find('tbody') or table
            data_rows = tbody.find_all('tr')
            
            for row in data_rows[1:] if not table.find('thead') else data_rows:
                cells = row.find_all(['td', 'th'])
                if cells and headers and len(cells) == len(headers):
                    row_data = {headers[i]: cells[i].get_text(strip=True) for i in range(len(headers))}
                    rows.append(row_data)
                elif cells:
                    row_data = {f'column_{i}': cells[i].get_text(strip=True) for i in range(len(cells))}
                    rows.append(row_data)
                    
        except Exception as e:
            logging.debug(f"Table extraction error: {e}")
            
        return rows
