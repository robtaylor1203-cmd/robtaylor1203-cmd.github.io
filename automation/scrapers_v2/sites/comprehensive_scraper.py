#!/usr/bin/env python3
"""
Comprehensive Tea Industry Scraper - ALL URLs
Handles all auction houses, news sources, and statistical data
"""

import asyncio
import logging
import requests
from datetime import datetime
from typing import Dict, List, Optional, Any
from bs4 import BeautifulSoup
import re
from pathlib import Path
import json

import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../'))

from core.advanced_scraper import AdvancedWebDriver, ScrapingResult, DataWarehouse, HumanBehaviorSimulator

class ComprehensiveTeaScraper:
    """Master scraper for all tea industry sources"""
    
    def __init__(self, data_warehouse: DataWarehouse):
        self.data_warehouse = data_warehouse
        self.behavior_simulator = HumanBehaviorSimulator()
        
        # Complete URL mapping from your list
        self.all_sources = {
            # MOMBASA SOURCES
            'atb_current_report': 'https://www.atbltd.com/Docs/current_market_report',
            'atb_auction_prices': 'https://atbltd.com/Docs/auctionprices',
            'atb_average_prices': 'https://atbltd.com/Docs/averageprices',
            'atb_buyers_purchases': 'https://atbltd.com/Docs/buyers_purchases',
            'atb_crop_weather': 'https://atbltd.com/Docs/crop_weather',
            'atb_crop_statistics': 'https://atbltd.com/Docs/crop_statistics',
            'atb_other_centres': 'https://atbltd.com/Docs/other_market_centres',
            'atb_auction_offerings': 'https://atbltd.com/Docs/auction_offerings',
            'atb_monthly_review': 'https://atbltd.com/Docs/monthly_review',
            'tbeal_reports': 'https://www.tbeal.net/download-category/tbea-market-report-2025/',
            'contemporary_mombasa': 'https://contemporarybrokers.com/market-report/reports/final-market-report',
            'contemporary_crop': 'https://contemporarybrokers.com/statistics/reports/crop',
            
            # COLOMBO SOURCES
            'john_keells': 'https://johnkeellstea.com/market-reports-2/',
            'ceylon_brokers_reports': 'https://ceylonteabrokers.com/market-reports/',
            'ceylon_weekly_averages': 'https://ceylonteabrokers.com/weekly-sales-averages/',
            'ceylon_weekly_qty': 'https://ceylonteabrokers.com/weekly-sold-qty-avg/',
            'ceylon_monthly_qty': 'https://ceylonteabrokers.com/monthly-sold-qty-avg/',
            'ceylon_production': 'https://ceylonteabrokers.com/monthly-production/',
            'ceylon_exports': 'https://ceylonteabrokers.com/export-countrywise/',
            'forbes_reports': 'https://web.forbestea.com/market-reports',
            'forbes_production': 'https://web.forbestea.com/statistics/sri-lankan-statistics/65-sri-lanka-tea-production/1301-sri-lanka-tea-production',
            'forbes_exports': 'https://web.forbestea.com/statistics/sri-lankan-statistics/64-sri-lanka-tea-exports/1302-sri-lanka-tea',
            'forbes_elevation': 'https://web.forbestea.com/statistics/sri-lankan-statistics/24-monthly-elevation-averages/1300-monthly-elevation-averages',
            'forbes_quantities': 'https://web.forbestea.com/statistics/sri-lankan-statistics/90-weekly-tea-auction-quantities-averages/1299-sri-lanka-weekly-tea-auction-quantities-averages',
            
            # INDIAN SOURCES
            'jthomas_prices': 'https://jthomasindia.com/auction_prices.php',
            'jthomas_bol': 'https://jthomasindia.com/stats/BOL.php',
            'jthomas_catalogue': 'https://jthomasindia.com/catalogue.php',
            'jthomas_market_report': 'https://jthomasindia.com/market_report.php',
            'jthomas_synopsis': 'https://jthomasindia.com/market_synopsis.php',
            'jthomas_district': 'https://jthomasindia.com/district_average.php',
            'teaboard_weekly': 'https://www.teaboard.gov.in/WEEKLYPRICES/2025',
            
            # BANGLADESH SOURCES
            'national_brokers_reports': 'https://nationalbrokersbd.com/market-reports/',
            'national_brokers_crop': 'https://nationalbrokersbd.com/statistics/crop/',
            'national_brokers_garden': 'https://nationalbrokersbd.com/statistics/garden-wise-average/',
            'national_brokers_buyers': 'https://nationalbrokersbd.com/statistics/buyer-purchase-statement/',
            'national_brokers_sold': 'https://nationalbrokersbd.com/statistics/soldunsold-percentage/',
            'national_brokers_export': 'https://nationalbrokersbd.com/statistics/export/',
            
            # NEWS SOURCES (BONUS)
            'tea_industry_news': 'https://www.teaboard.gov.in/NewsEvent',
            'tea_guardian': 'https://www.theteaguardian.com/news',
            'world_tea_news': 'https://worldteanews.com/news-features',
            'fresh_cup_tea': 'https://www.freshcup.com/category/tea/',
        }
        
        # Group sources by region for organized processing
        self.source_groups = {
            'mombasa': [k for k in self.all_sources.keys() if any(x in k for x in ['atb', 'tbeal', 'contemporary'])],
            'colombo': [k for k in self.all_sources.keys() if any(x in k for x in ['john', 'ceylon', 'forbes'])],
            'india': [k for k in self.all_sources.keys() if any(x in k for x in ['jthomas', 'teaboard'])],
            'bangladesh': [k for k in self.all_sources.keys() if 'national_brokers' in k],
            'news': [k for k in self.all_sources.keys() if any(x in k for x in ['news', 'guardian', 'cup'])]
        }
        
    async def scrape_all_sources(self) -> Dict[str, List[ScrapingResult]]:
        """Scrape ALL tea industry sources comprehensively"""
        all_results = {}
        
        for group_name, source_keys in self.source_groups.items():
            logging.info(f"🌍 Starting {group_name.upper()} group ({len(source_keys)} sources)")
            group_results = []
            
            for source_key in source_keys:
                url = self.all_sources[source_key]
                
                try:
                    logging.info(f"📡 Scraping {source_key}: {url}")
                    result = await self.scrape_single_source(url, source_key, group_name)
                    
                    if result and result.success:
                        group_results.append(result)
                        self.data_warehouse.store_raw_data(result)
                        logging.info(f"✅ {source_key} successful")
                    else:
                        logging.warning(f"❌ {source_key} failed")
                        
                    # Human-like delays between sources
                    self.behavior_simulator.random_delay(20, 60)
                    
                except Exception as e:
                    logging.error(f"💥 {source_key} error: {e}")
                    
            all_results[group_name] = group_results
            
            # Longer delay between groups (regions)
            if group_name != list(self.source_groups.keys())[-1]:
                logging.info(f"⏳ Resting 5-10 minutes before next region...")
                await asyncio.sleep(300 + (300 * hash(group_name) % 300))
                
        return all_results
        
    async def scrape_single_source(self, url: str, source_key: str, group: str) -> Optional[ScrapingResult]:
        """Scrape individual source with intelligent method selection"""
        
        # Try requests first (faster), fallback to selenium for complex sites
        result = await self.scrape_with_requests(url, source_key, group)
        
        if not result or not result.success:
            # Fallback to selenium for complex sites
            result = await self.scrape_with_selenium(url, source_key, group)
            
        return result
        
    async def scrape_with_requests(self, url: str, source_key: str, group: str) -> Optional[ScrapingResult]:
        """Fast scraping with requests"""
        try:
            headers = {
                'User-Agent': self.behavior_simulator.get_random_user_agent(),
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1'
            }
            
            response = requests.get(url, headers=headers, timeout=30, allow_redirects=True)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            extracted_data = self.extract_comprehensive_data(soup, source_key, group)
            
            # Check if we got meaningful data
            if self.is_meaningful_data(extracted_data):
                return ScrapingResult(
                    source_url=url,
                    auction_center=f"{group.title()}_{source_key}",
                    data_type=self.classify_data_type(source_key),
                    timestamp=datetime.now(),
                    raw_data=extracted_data,
                    success=True,
                    metadata={'scraping_method': 'requests', 'group': group}
                )
            else:
                return None
                
        except Exception as e:
            logging.debug(f"Requests scraping failed for {url}: {e}")
            return None
            
    async def scrape_with_selenium(self, url: str, source_key: str, group: str) -> Optional[ScrapingResult]:
        """Advanced scraping with Selenium"""
        driver_manager = AdvancedWebDriver()
        
        try:
            driver = driver_manager.setup_driver()
            
            if not driver_manager.safe_get(url):
                return None
                
            # Wait for dynamic content to load
            await asyncio.sleep(5)
            
            # Handle interactive elements
            await self.handle_interactive_elements(driver, source_key)
            
            # Extract data
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            extracted_data = self.extract_comprehensive_data(soup, source_key, group)
            
            # Add selenium-specific extractions
            selenium_data = await self.extract_selenium_specific_data(driver, source_key)
            extracted_data.update(selenium_data)
            
            if self.is_meaningful_data(extracted_data):
                return ScrapingResult(
                    source_url=url,
                    auction_center=f"{group.title()}_{source_key}",
                    data_type=self.classify_data_type(source_key),
                    timestamp=datetime.now(),
                    raw_data=extracted_data,
                    success=True,
                    metadata={'scraping_method': 'selenium', 'group': group}
                )
            else:
                return None
                
        except Exception as e:
            logging.error(f"Selenium scraping failed for {url}: {e}")
            return None
        finally:
            driver_manager.close()
            
    async def handle_interactive_elements(self, driver, source_key: str):
        """Handle dropdowns, buttons, date selectors"""
        try:
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import Select, WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            
            # Handle date/week selectors
            date_selectors = ["select[name*='week']", "select[name*='date']", "select[name*='year']"]
            for selector in date_selectors:
                try:
                    dropdown = WebDriverWait(driver, 3).until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                    )
                    select = Select(dropdown)
                    
                    # Select most recent option
                    if len(select.options) > 1:
                        select.select_by_index(-1)  # Last option (usually most recent)
                        await asyncio.sleep(2)
                except:
                    continue
                    
            # Handle "View Report" or "Download" buttons
            button_texts = ['view', 'download', 'show', 'display', 'report']
            for text in button_texts:
                try:
                    buttons = driver.find_elements(By.XPATH, f"//button[contains(text(), '{text}') or contains(@value, '{text}')]")
                    if buttons:
                        driver.execute_script("arguments[0].click();", buttons[0])
                        await asyncio.sleep(3)
                        break
                except:
                    continue
                    
        except Exception as e:
            logging.debug(f"Interactive elements handling failed: {e}")
            
    def extract_comprehensive_data(self, soup: BeautifulSoup, source_key: str, group: str) -> Dict[str, Any]:
        """Comprehensive data extraction for any source"""
        data = {
            'source_key': source_key,
            'group': group,
            'page_title': soup.title.string if soup.title else '',
            'extraction_timestamp': datetime.now().isoformat(),
            'url_classification': self.classify_data_type(source_key)
        }
        
        # Extract all tables
        tables = soup.find_all('table')
        for i, table in enumerate(tables):
            table_data = self.extract_advanced_table(table)
            if table_data:
                data[f'table_{i}'] = table_data
                
        # Extract structured data based on source type
        if 'price' in source_key:
            data.update(self.extract_price_data(soup))
        elif 'report' in source_key:
            data.update(self.extract_report_data(soup))
        elif 'statistics' in source_key or 'crop' in source_key:
            data.update(self.extract_statistics_data(soup))
        elif 'news' in source_key:
            data.update(self.extract_news_data(soup))
        else:
            data.update(self.extract_general_market_data(soup))
            
        # Extract download links
        download_links = soup.find_all('a', href=re.compile(r'\.(pdf|xlsx?|docx?|csv)', re.I))
        for i, link in enumerate(download_links[:5]):  # Limit to 5 downloads
            data[f'download_link_{i}'] = {
                'url': link.get('href'),
                'text': link.get_text(strip=True),
                'type': link.get('href', '').split('.')[-1].lower()
            }
            
        return data
        
    def extract_advanced_table(self, table) -> List[Dict[str, Any]]:
        """Advanced table extraction with data type detection"""
        rows = []
        
        try:
            # Get headers with multiple strategies
            headers = []
            
            # Strategy 1: thead
            header_row = table.find('thead')
            if header_row:
                header_cells = header_row.find_all(['th', 'td'])
                headers = [cell.get_text(strip=True) for cell in header_cells]
            
            # Strategy 2: First row with th tags
            if not headers:
                first_row = table.find('tr')
                if first_row:
                    header_cells = first_row.find_all('th')
                    if header_cells:
                        headers = [cell.get_text(strip=True) for cell in header_cells]
            
            # Strategy 3: First row as headers if no th tags found
            if not headers:
                first_row = table.find('tr')
                if first_row:
                    cells = first_row.find_all(['td', 'th'])
                    if len(cells) > 1:
                        headers = [cell.get_text(strip=True) for cell in cells]
                        
            # Get data rows
            tbody = table.find('tbody') or table
            all_rows = tbody.find_all('tr')
            
            # Skip header row if we used first row as headers
            data_rows = all_rows[1:] if not table.find('thead') and headers else all_rows
            
            for row in data_rows:
                cells = row.find_all(['td', 'th'])
                if cells:
                    if headers and len(cells) >= len(headers):
                        row_data = {}
                        for i, header in enumerate(headers):
                            if i < len(cells):
                                cell_text = cells[i].get_text(strip=True)
                                # Try to detect and convert numeric data
                                row_data[header] = self.smart_convert_value(cell_text)
                        rows.append(row_data)
                    else:
                        # No headers or mismatch - use generic column names
                        row_data = {}
                        for i, cell in enumerate(cells):
                            cell_text = cell.get_text(strip=True)
                            row_data[f'column_{i}'] = self.smart_convert_value(cell_text)
                        rows.append(row_data)
                        
        except Exception as e:
            logging.debug(f"Advanced table extraction error: {e}")
            
        return rows
        
    def smart_convert_value(self, value: str) -> Any:
        """Intelligently convert string values to appropriate types"""
        if not value or value.lower() in ['n/a', 'na', '-', '']:
            return None
            
        # Try to convert to number
        clean_value = re.sub(r'[^\d\.\-]', '', value)
        if clean_value:
            try:
                if '.' in clean_value:
                    return float(clean_value)
                else:
                    return int(clean_value)
            except ValueError:
                pass
                
        # Keep as string but cleaned
        return value.strip()
        
    def extract_price_data(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract price-specific data"""
        price_data = {}
        
        text_content = soup.get_text()
        
        # Extract various price patterns
        price_patterns = {
            'usd_prices': r'\$(\d+(?:,\d{3})*(?:\.\d+)?)',
            'cents_prices': r'(\d+(?:,\d{3})*(?:\.\d+)?)\s*(?:cents?|c|¢)',
            'rupee_prices': r'(?:Rs\.?|₹|INR)\s*(\d+(?:,\d{3})*(?:\.\d+)?)',
            'lkr_prices': r'(?:LKR)\s*(\d+(?:,\d{3})*(?:\.\d+)?)',
            'per_kg_prices': r'(\d+(?:,\d{3})*(?:\.\d+)?)\s*(?:per|/)\s*kg'
        }
        
        for price_type, pattern in price_patterns.items():
            matches = re.findall(pattern, text_content, re.IGNORECASE)
            if matches:
                price_data[price_type] = [float(m.replace(',', '')) for m in matches[:20]]
                
        return price_data
        
    def extract_report_data(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract market report specific data"""
        report_data = {}
        
        # Look for report sections
        sections = soup.find_all(['div', 'section', 'article'], class_=re.compile(r'report|market|content', re.I))
        for i, section in enumerate(sections[:5]):
            section_text = section.get_text(strip=True)
            if len(section_text) > 100:  # Meaningful content
                report_data[f'report_section_{i}'] = section_text
                
        # Extract key metrics from text
        text_content = soup.get_text()
        
        # Volume patterns
        volume_patterns = {
            'kg_volumes': r'(\d+(?:,\d{3})*(?:\.\d+)?)\s*(?:kg|kgs|kilos|kilograms)',
            'ton_volumes': r'(\d+(?:,\d{3})*(?:\.\d+)?)\s*(?:tons?|tonnes?)',
            'package_volumes': r'(\d+(?:,\d{3})*(?:\.\d+)?)\s*(?:packages?|pkgs?)',
            'lot_volumes': r'(\d+(?:,\d{3})*(?:\.\d+)?)\s*(?:lots?)'
        }
        
        for volume_type, pattern in volume_patterns.items():
            matches = re.findall(pattern, text_content, re.IGNORECASE)
            if matches:
                report_data[volume_type] = [float(m.replace(',', '')) for m in matches[:10]]
                
        return report_data
        
    def extract_statistics_data(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract statistical data"""
        stats_data = {}
        
        # Look for charts and graphs
        charts = soup.find_all('img', src=re.compile(r'chart|graph|stats', re.I))
        for i, chart in enumerate(charts):
            stats_data[f'chart_{i}'] = {
                'src': chart.get('src'),
                'alt': chart.get('alt', ''),
                'title': chart.get('title', '')
            }
            
        # Extract percentage data
        text_content = soup.get_text()
        percentages = re.findall(r'(\d+(?:\.\d+)?)\s*%', text_content)
        if percentages:
            stats_data['percentages'] = [float(p) for p in percentages[:20]]
            
        return stats_data
        
    def extract_news_data(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract news and articles"""
        news_data = {}
        
        # Find article headlines
        headlines = soup.find_all(['h1', 'h2', 'h3'], string=re.compile(r'tea', re.I))
        for i, headline in enumerate(headlines[:10]):
            news_data[f'headline_{i}'] = headline.get_text(strip=True)
            
        # Find article summaries
        articles = soup.find_all(['article', 'div'], class_=re.compile(r'news|article|post', re.I))
        for i, article in enumerate(articles[:5]):
            article_text = article.get_text(strip=True)
            if len(article_text) > 50:
                news_data[f'article_{i}'] = article_text[:500]  # First 500 chars
                
        return news_data
        
    def extract_general_market_data(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """Extract general market data"""
        market_data = {}
        
        # Get main content
        main_content = soup.find('main') or soup.find('body')
        if main_content:
            content_text = main_content.get_text(strip=True)
            if len(content_text) > 200:
                market_data['main_content'] = content_text[:1000]  # First 1000 chars
                
        # Look for data lists
        lists = soup.find_all(['ul', 'ol'])
        for i, list_elem in enumerate(lists[:3]):
            items = [li.get_text(strip=True) for li in list_elem.find_all('li')]
            if items:
                market_data[f'data_list_{i}'] = items
                
        return market_data
        
    async def extract_selenium_specific_data(self, driver, source_key: str) -> Dict[str, Any]:
        """Extract data that requires Selenium"""
        selenium_data = {}
        
        try:
            from selenium.webdriver.common.by import By
            
            # Extract dynamic content
            dynamic_elements = driver.find_elements(By.CSS_SELECTOR, "[data-price], [data-volume], [data-stats]")
            for i, element in enumerate(dynamic_elements):
                selenium_data[f'dynamic_data_{i}'] = element.text
                
            # Check for JavaScript-generated content
            js_content = driver.execute_script("return document.body.innerText;")
            if len(js_content) > len(soup.get_text()) * 1.1:  # 10% more content suggests JS generation
                selenium_data['js_generated_content'] = True
                
        except Exception as e:
            logging.debug(f"Selenium-specific extraction failed: {e}")
            
        return selenium_data
        
    def classify_data_type(self, source_key: str) -> str:
        """Classify the type of data based on source key"""
        if 'price' in source_key:
            return 'auction_prices'
        elif 'report' in source_key:
            return 'market_report'
        elif 'statistic' in source_key or 'crop' in source_key:
            return 'statistics'
        elif 'news' in source_key:
            return 'news'
        elif 'export' in source_key:
            return 'export_data'
        elif 'production' in source_key:
            return 'production_data'
        else:
            return 'general_market_data'
            
    def is_meaningful_data(self, data: Dict[str, Any]) -> bool:
        """Check if extracted data is meaningful"""
        if not data:
            return False
            
        # Check for tables
        table_count = len([k for k in data.keys() if 'table' in k and data[k]])
        if table_count > 0:
            return True
            
        # Check for extracted numerical data
        numerical_fields = ['extracted_prices', 'extracted_volumes', 'percentages'] + \
                          [k for k in data.keys() if 'price' in k or 'volume' in k]
        
        for field in numerical_fields:
            if field in data and data[field]:
                return True
                
        # Check for substantial text content
        text_fields = ['main_content', 'page_content'] + \
                     [k for k in data.keys() if 'content' in k or 'section' in k]
        
        for field in text_fields:
            if field in data and isinstance(data[field], str) and len(data[field]) > 200:
                return True
                
        return False
        
# Usage function
async def run_comprehensive_scraping():
    """Run the comprehensive scraping system"""
    data_warehouse = DataWarehouse()
    scraper = ComprehensiveTeaScraper(data_warehouse)
    
    logging.info("🌟 Starting Comprehensive Tea Industry Scraping")
    logging.info(f"📊 Total sources: {len(scraper.all_sources)}")
    
    results = await scraper.scrape_all_sources()
    
    # Report results
    total_successful = sum(len([r for r in group_results if r.success]) 
                          for group_results in results.values())
    
    logging.info(f"✅ Comprehensive scraping completed!")
    logging.info(f"📈 Successfully scraped: {total_successful} sources")
    
    for group_name, group_results in results.items():
        successful = len([r for r in group_results if r.success])
        total = len(scraper.source_groups[group_name])
        logging.info(f"   {group_name.title()}: {successful}/{total} successful")
        
    return results

if __name__ == "__main__":
    asyncio.run(run_comprehensive_scraping())
