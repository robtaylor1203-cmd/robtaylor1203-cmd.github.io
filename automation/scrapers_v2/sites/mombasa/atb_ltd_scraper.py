#!/usr/bin/env python3
"""ATB Ltd Scraper - Handles Mombasa and multi-region data"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
import requests
from bs4 import BeautifulSoup
import re

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))

from core.advanced_scraper import AdvancedWebDriver, ScrapingResult, DataWarehouse, HumanBehaviorSimulator

class ATBLtdScraper:
    def __init__(self, data_warehouse: DataWarehouse):
        self.base_url = "https://atbltd.com"
        self.data_warehouse = data_warehouse
        self.behavior_simulator = HumanBehaviorSimulator()
        
        self.endpoints = {
            'current_market_report': '/Docs/current_market_report',
            'auction_prices': '/Docs/auctionprices',
            'average_prices': '/Docs/averageprices',
            'crop_statistics': '/Docs/crop_statistics'
        }
        
    async def scrape_all_endpoints(self) -> List[ScrapingResult]:
        results = []
        
        for data_type, endpoint in self.endpoints.items():
            try:
                result = await self.scrape_endpoint(endpoint, data_type)
                if result:
                    results.append(result)
                self.behavior_simulator.random_delay(30, 90)
            except Exception as e:
                logging.error(f"ATB Ltd {data_type} failed: {e}")
                
        return results
        
    async def scrape_endpoint(self, endpoint: str, data_type: str) -> Optional[ScrapingResult]:
        url = f"{self.base_url}{endpoint}"
        
        try:
            headers = {
                'User-Agent': self.behavior_simulator.get_random_user_agent(),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
            }
            
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            extracted_data = self.extract_data(soup, data_type)
            
            return ScrapingResult(
                source_url=url,
                auction_center="ATB_Mombasa",
                data_type=data_type,
                timestamp=datetime.now(),
                raw_data=extracted_data,
                success=True
            )
            
        except Exception as e:
            logging.error(f"ATB scraping failed for {url}: {e}")
            return None
            
    def extract_data(self, soup: BeautifulSoup, data_type: str) -> Dict[str, Any]:
        data = {
            'page_title': soup.title.string if soup.title else '',
            'extraction_timestamp': datetime.now().isoformat()
        }
        
        # Extract tables
        tables = soup.find_all('table')
        for i, table in enumerate(tables):
            data[f'table_{i}'] = self.extract_table_data(table)
            
        # Extract text content
        main_content = soup.get_text()
        
        # Extract prices and volumes with regex
        prices = re.findall(r'(\d+\.?\d*)\s*(?:cents?|USD|\$)', main_content, re.IGNORECASE)
        volumes = re.findall(r'(\d+(?:,\d{3})*)\s*(?:kg|tons?)', main_content, re.IGNORECASE)
        
        data['extracted_prices'] = prices
        data['extracted_volumes'] = volumes
        
        return data
        
    def extract_table_data(self, table) -> List[Dict[str, str]]:
        rows = []
        
        try:
            # Get headers
            headers = []
            header_row = table.find('thead')
            if header_row:
                header_cells = header_row.find_all(['th', 'td'])
                headers = [cell.get_text(strip=True) for cell in header_cells]
            
            # Get data rows
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
