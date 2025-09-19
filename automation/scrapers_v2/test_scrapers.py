#!/usr/bin/env python3
"""Test script for advanced scrapers"""

import asyncio
import sys
import os

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from core.advanced_scraper import DataWarehouse
from sites.mombasa.atb_ltd_scraper import ATBLtdScraper

async def test_single_scraper():
    print("🧪 Testing ATB Ltd scraper...")
    
    data_warehouse = DataWarehouse()
    scraper = ATBLtdScraper(data_warehouse)
    
    try:
        # Test one endpoint
        result = await scraper.scrape_endpoint('/Docs/current_market_report', 'market_report')
        
        if result and result.success:
            print("✅ Test successful!")
            print(f"   Data extracted: {len(result.raw_data)} fields")
            print(f"   Tables found: {len([k for k in result.raw_data.keys() if 'table' in k])}")
            return True
        else:
            print("❌ Test failed - no data extracted")
            return False
            
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False

if __name__ == "__main__":
    asyncio.run(test_single_scraper())
