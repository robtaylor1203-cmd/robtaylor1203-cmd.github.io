#!/usr/bin/env python3
"""Complete Tea Trade Automation Runner"""

import subprocess
import sys
import time
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('FULL_AUTOMATION')

def run_command(command, description):
    """Run a command and return success status"""
    try:
        logger.info(f"🚀 {description}")
        result = subprocess.run(command, shell=True, capture_output=True, text=True, timeout=600)
        
        if result.returncode == 0:
            logger.info(f"✅ {description} completed successfully")
            return True
        else:
            logger.error(f"❌ {description} failed: {result.stderr}")
            return False
    except Exception as e:
        logger.error(f"❌ {description} error: {e}")
        return False

def main():
    logger.info("🎯 Starting COMPLETE Tea Trade Automation")
    
    commands = [
        ("python automation/scrapers/jthomas/scrape_jthomas_live.py", "J Thomas Live Scraper"),
        ("python automation/scrapers/ceylon/scrape_ceylon_complete.py", "Ceylon Tea Brokers"),
        ("python automation/scrapers/tbea/scrape_tbea_complete.py", "TBEA Kenya"),
        ("python automation/scrapers/atb/scrape_atb_complete.py", "ATB Kenya"),
        ("python automation/scrapers/news/scrape_news_complete.py", "Tea Industry News"),
    ]
    
    successful = 0
    
    for command, description in commands:
        if run_command(command, description):
            successful += 1
        time.sleep(5)  # Brief pause between scrapers
    
    logger.info(f"🎉 Automation complete: {successful}/{len(commands)} scrapers successful")
    
    # Show results
    run_command("""
    psql -h localhost -U tea_admin -d tea_trade_data -c "
    SELECT source, COUNT(*) as records, MAX(scrape_timestamp) as latest
    FROM auction_lots 
    GROUP BY source
    ORDER BY records DESC;
    "
    """, "Database Summary")
    
    return 0 if successful > 0 else 1

if __name__ == "__main__":
    exit(main())
