#!/usr/bin/env python3
"""J Thomas India Scraper - Handles Indian auction centers"""

import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select, WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../../'))

from core.advanced_scraper import AdvancedWebDriver, ScrapingResult, DataWarehouse, HumanBehaviorSimulator

class JThomasIndiaScraper:
    def __init__(self, data_warehouse: DataWarehouse):
        self.base_url = "https://jthomasindia.com"
        self.data_warehouse = data_warehouse
        self.behavior_simulator = HumanBehaviorSimulator()
        
        self.endpoints = {
            'auction_prices': '/auction_prices.php',
            'market_report': '/market_report.php',
            'district_average': '/district_average.php'
        }
        
        self.auction_centers = ['Kolkata', 'Guwahati', 'Siliguri']
        
    async def scrape_all_centers_and_endpoints(self) -> List[ScrapingResult]:
        results = []
        
        for center in self.auction_centers:
            for data_type, endpoint in self.endpoints.items():
                try:
                    result = await self.scrape_center_endpoint(center, endpoint, data_type)
                    if result and result.success:
                        results.append(result)
                    self.behavior_simulator.random_delay(45, 120)
                except Exception as e:
                    logging.error(f"J Thomas {center} - {data_type} failed: {e}")
                    
        return results
        
    async def scrape_center_endpoint(self, center: str, endpoint: str, data_type: str) -> Optional[ScrapingResult]:
        driver_manager = AdvancedWebDriver()
        
        try:
            driver = driver_manager.setup_driver()
            url = f"{self.base_url}{endpoint}"
            
            if not driver_manager.safe_get(url):
                return None
                
            # Handle center selection if dropdown exists
            await self.handle_center_selection(driver, center)
            
            extracted_data = await self.extract_data(driver, data_type, center)
            
            return ScrapingResult(
                source_url=url,
                auction_center=f"JThomas_{center}",
                data_type=data_type,
                timestamp=datetime.now(),
                raw_data=extracted_data,
                success=True
            )
            
        except Exception as e:
            logging.error(f"J Thomas scraping failed: {e}")
            return None
        finally:
            driver_manager.close()
            
    async def handle_center_selection(self, driver, target_center: str):
        try:
            # Look for center dropdowns
            selectors = ["select[name*='center']", "select[id*='center']"]
            
            for selector in selectors:
                try:
                    dropdown = WebDriverWait(driver, 5).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                    )
                    
                    select = Select(dropdown)
                    
                    # Try to select target center
                    for option in select.options:
                        if target_center.lower() in option.text.lower():
                            select.select_by_visible_text(option.text)
                            self.behavior_simulator.random_delay(2, 5)
                            return True
                except:
                    continue
                    
        except Exception as e:
            logging.debug(f"Center selection failed: {e}")
            
    async def extract_data(self, driver, data_type: str, center: str) -> Dict[str, Any]:
        data = {
            'auction_center': center,
            'data_type': data_type,
            'extraction_timestamp': datetime.now().isoformat()
        }
        
        # Extract tables
        tables = driver.find_elements(By.TAG_NAME, "table")
        for i, table in enumerate(tables):
            table_data = self.extract_selenium_table(table)
            if table_data:
                data[f'table_{i}'] = table_data
                
        # Extract page content
        data['page_content'] = driver.find_element(By.TAG_NAME, "body").text
        
        return data
        
    def extract_selenium_table(self, table_element) -> List[Dict[str, Any]]:
        rows = []
        
        try:
            # Get headers
            headers = []
            try:
                header_row = table_element.find_element(By.TAG_NAME, "thead")
                header_cells = header_row.find_elements(By.TAG_NAME, "th")
                headers = [cell.text.strip() for cell in header_cells]
            except:
                pass
                
            # Get data rows
            try:
                tbody = table_element.find_element(By.TAG_NAME, "tbody")
                data_rows = tbody.find_elements(By.TAG_NAME, "tr")
            except:
                data_rows = table_element.find_elements(By.TAG_NAME, "tr")
                
            for row in data_rows:
                cells = row.find_elements(By.TAG_NAME, "td")
                if cells:
                    if headers and len(cells) == len(headers):
                        row_data = {headers[i]: cells[i].text.strip() for i in range(len(headers))}
                    else:
                        row_data = {f'column_{i}': cells[i].text.strip() for i in range(len(cells))}
                    rows.append(row_data)
                    
        except Exception as e:
            logging.debug(f"Table extraction error: {e}")
            
        return rows
