#!/usr/bin/env python3
"""Simplified Master Controller"""

import subprocess
import sys
from pathlib import Path
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('MASTER_CONTROLLER')

def run_scraper(scraper_path):
    """Run individual scraper"""
    try:
        logger.info(f"Running {scraper_path}")
        result = subprocess.run([sys.executable, str(scraper_path)], 
                              capture_output=True, text=True, timeout=300)
        
        if result.returncode == 0:
            logger.info(f"✅ {scraper_path.name} completed successfully")
            return True, ""
        else:
            logger.error(f"❌ {scraper_path.name} failed: {result.stderr}")
            return False, result.stderr
            
    except subprocess.TimeoutExpired:
        logger.error(f"❌ {scraper_path.name} timed out")
        return False, "Timeout"
    except Exception as e:
        logger.error(f"❌ {scraper_path.name} error: {e}")
        return False, str(e)

def main():
    logger.info("🚀 Starting Master Automation Controller")
    
    # Define scrapers
    scrapers = [
        Path(__file__).parent / 'scrapers' / 'jthomas' / 'scrape_jthomas_complete.py',
        Path(__file__).parent / 'scrapers' / 'ceylon' / 'scrape_ceylon_complete.py',
        Path(__file__).parent / 'scrapers' / 'forbes' / 'scrape_forbes_complete.py',
        Path(__file__).parent / 'scrapers' / 'tbea' / 'scrape_tbea_complete.py',
        Path(__file__).parent / 'scrapers' / 'atb' / 'scrape_atb_complete.py',
        Path(__file__).parent / 'scrapers' / 'news' / 'scrape_news_complete.py'
    ]
    
    successful = 0
    total = len(scrapers)
    
    for scraper in scrapers:
        if scraper.exists():
            success, error = run_scraper(scraper)
            if success:
                successful += 1
        else:
            logger.warning(f"⚠️ Scraper not found: {scraper}")
    
    logger.info(f"🎉 Automation complete: {successful}/{total} scrapers successful")
    
    return 0 if successful > 0 else 1

if __name__ == "__main__":
    exit(main())
