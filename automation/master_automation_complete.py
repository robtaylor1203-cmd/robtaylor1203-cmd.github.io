#!/usr/bin/env python3
"""
Master Tea Trade Automation Controller
Complete automation with Git integration and website updates
"""

import subprocess
import sys
import json
import time
from pathlib import Path
from datetime import datetime
from typing import Dict, List
import logging
import os

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('MASTER_AUTOMATION')

def get_db_connection():
    try:
        import psycopg2
        return psycopg2.connect(
            host='localhost',
            database='tea_trade_data',
            user='tea_admin',
            password='secure_password_123'
        )
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return None

def run_scraper(scraper_path: Path) -> Dict:
    """Run individual scraper and return results"""
    try:
        logger.info(f"Running {scraper_path.name}")
        start_time = time.time()
        
        result = subprocess.run(
            [sys.executable, str(scraper_path)], 
            capture_output=True, 
            text=True, 
            timeout=600  # 10 minutes max per scraper
        )
        
        duration = time.time() - start_time
        
        if result.returncode == 0:
            logger.info(f"✅ {scraper_path.name} completed successfully in {duration:.1f}s")
            return {
                'scraper': scraper_path.name,
                'status': 'success',
                'duration': duration,
                'output': result.stdout,
                'error': None
            }
        else:
            logger.error(f"❌ {scraper_path.name} failed: {result.stderr}")
            return {
                'scraper': scraper_path.name,
                'status': 'failed',
                'duration': duration,
                'output': result.stdout,
                'error': result.stderr
            }
            
    except subprocess.TimeoutExpired:
        logger.error(f"❌ {scraper_path.name} timed out")
        return {
            'scraper': scraper_path.name,
            'status': 'timeout',
            'duration': 600,
            'output': '',
            'error': 'Timeout after 10 minutes'
        }
    except Exception as e:
        logger.error(f"❌ {scraper_path.name} error: {e}")
        return {
            'scraper': scraper_path.name,
            'status': 'error',
            'duration': 0,
            'output': '',
            'error': str(e)
        }

def get_database_stats() -> Dict:
    """Get current database statistics"""
    try:
        conn = get_db_connection()
        if not conn:
            return {}
        
        cursor = conn.cursor()
        
        # Get auction lot counts by source
        cursor.execute("""
            SELECT 
                source, 
                COUNT(*) as total_lots,
                COUNT(CASE WHEN scrape_timestamp >= CURRENT_DATE THEN 1 END) as today_lots,
                AVG(price_per_kg) as avg_price,
                MAX(scrape_timestamp) as last_scrape
            FROM auction_lots 
            GROUP BY source
            ORDER BY total_lots DESC
        """)
        
        auction_stats = {}
        total_lots_today = 0
        total_lots_all = 0
        
        for row in cursor.fetchall():
            source, total, today, avg_price, last_scrape = row
            auction_stats[source] = {
                'total_lots': total,
                'today_lots': today,
                'avg_price': float(avg_price) if avg_price else 0,
                'last_scrape': last_scrape.isoformat() if last_scrape else None
            }
            total_lots_today += today
            total_lots_all += total
        
        # Get news count
        cursor.execute("""
            SELECT 
                COUNT(*) as total_articles,
                COUNT(CASE WHEN scrape_timestamp >= CURRENT_DATE THEN 1 END) as today_articles
            FROM news_articles
        """)
        news_total, news_today = cursor.fetchone()
        
        conn.close()
        
        return {
            'auction_stats': auction_stats,
            'summary': {
                'total_lots_all_time': total_lots_all,
                'total_lots_today': total_lots_today,
                'total_articles_all_time': news_total or 0,
                'total_articles_today': news_today or 0
            }
        }
        
    except Exception as e:
        logger.error(f"Error getting database stats: {e}")
        return {}

def generate_website_data() -> bool:
    """Generate data files for website"""
    try:
        logger.info("Generating website data files...")
        
        # Create data directory
        data_dir = Path("data/latest")
        data_dir.mkdir(parents=True, exist_ok=True)
        
        conn = get_db_connection()
        if not conn:
            return False
        
        cursor = conn.cursor()
        
        # 1. Market Summary
        cursor.execute("""
            SELECT 
                COUNT(*) as total_auctions,
                COUNT(DISTINCT garden_id) as active_gardens,
                MAX(scrape_timestamp) as last_updated,
                AVG(price_per_kg) as avg_price
            FROM auction_lots
            WHERE scrape_timestamp >= CURRENT_DATE - INTERVAL '7 days'
        """)
        stats = cursor.fetchone()
        
        cursor.execute("""
            SELECT source, COUNT(*) as lots, AVG(price_per_kg) as avg_price
            FROM auction_lots
            WHERE scrape_timestamp >= CURRENT_DATE - INTERVAL '7 days'
            GROUP BY source
        """)
        sources = [
            {
                'source': row[0],
                'lots': row[1],
                'avg_price': float(row[2]) if row[2] else 0
            }
            for row in cursor.fetchall()
        ]
        
        summary = {
            'last_updated': datetime.now().isoformat(),
            'total_auctions': stats[0] or 0,
            'active_gardens': stats[1] or 0,
            'avg_price': float(stats[3]) if stats[3] else 0,
            'sources': sources,
            'system_status': 'operational'
        }
        
        with open(data_dir / 'market_summary.json', 'w') as f:
            json.dump(summary, f, indent=2)
        
        # 2. Recent Auction Data (for charts)
        cursor.execute("""
            SELECT 
                al.source,
                COALESCE(ac.name, al.source) as auction_centre,
                COALESCE(g.name, 'Unknown') as garden,
                al.grade,
                al.quantity_kg,
                al.price_per_kg,
                al.auction_date
            FROM auction_lots al
            LEFT JOIN auction_centres ac ON al.auction_centre_id = ac.id
            LEFT JOIN gardens g ON al.garden_id = g.id
            WHERE al.auction_date >= CURRENT_DATE - INTERVAL '30 days'
            AND al.price_per_kg > 0
            ORDER BY al.auction_date DESC
            LIMIT 500
        """)
        
        auction_data = [
            {
                'source': row[0],
                'auction_centre': row[1],
                'garden': row[2],
                'grade': row[3] or 'Mixed',
                'quantity_kg': row[4] or 0,
                'price_per_kg': float(row[5]) if row[5] else 0,
                'auction_date': row[6].isoformat() if row[6] else None
            }
            for row in cursor.fetchall()
        ]
        
        with open(data_dir / 'auction_data.json', 'w') as f:
            json.dump(auction_data, f, indent=2)
        
        # 3. Price Trends (for charts)
        cursor.execute("""
            SELECT 
                DATE(auction_date) as date,
                source,
                AVG(price_per_kg) as avg_price,
                SUM(quantity_kg) as total_quantity,
                COUNT(*) as lots_count
            FROM auction_lots
            WHERE auction_date >= CURRENT_DATE - INTERVAL '30 days'
            AND price_per_kg > 0
            GROUP BY DATE(auction_date), source
            ORDER BY date DESC
        """)
        
        price_trends = [
            {
                'date': row[0].isoformat(),
                'source': row[1],
                'avg_price': float(row[2]),
                'total_quantity': row[3] or 0,
                'lots_count': row[4]
            }
            for row in cursor.fetchall()
        ]
        
        with open(data_dir / 'price_trends.json', 'w') as f:
            json.dump(price_trends, f, indent=2)
        
        # 4. Recent News
        cursor.execute("""
            SELECT title, source, url, publish_date, content
            FROM news_articles
            WHERE scrape_timestamp >= CURRENT_DATE - INTERVAL '7 days'
            ORDER BY publish_date DESC
            LIMIT 20
        """)
        
        news_data = [
            {
                'title': row[0],
                'source': row[1],
                'url': row[2],
                'publish_date': row[3].isoformat() if row[3] else None,
                'summary': row[4][:200] + '...' if row[4] and len(row[4]) > 200 else row[4]
            }
            for row in cursor.fetchall()
        ]
        
        with open(data_dir / 'news_data.json', 'w') as f:
            json.dump(news_data, f, indent=2)
        
        conn.close()
        logger.info(f"✅ Generated {len(os.listdir(data_dir))} data files")
        return True
        
    except Exception as e:
        logger.error(f"Error generating website data: {e}")
        return False

def update_website_timestamp():
    """Update website with latest automation timestamp"""
    try:
        index_file = Path("index.html")
        if not index_file.exists():
            logger.warning("index.html not found")
            return False
        
        # Read current content
        content = index_file.read_text()
        
        # Remove old timestamp comment
        import re
        content = re.sub(r'<!-- Last automated update:.*?-->', '', content)
        
        # Add new timestamp
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')
        timestamp_comment = f'    <!-- Last automated update: {timestamp} -->'
        
        # Insert before closing body tag
        content = content.replace('</body>', f'{timestamp_comment}\n</body>')
        
        # Write back
        index_file.write_text(content)
        
        logger.info(f"✅ Updated website timestamp: {timestamp}")
        return True
        
    except Exception as e:
        logger.error(f"Error updating website timestamp: {e}")
        return False

def commit_and_push_changes(automation_results: List[Dict], db_stats: Dict):
    """Commit and push all changes to Git"""
    try:
        logger.info("Committing and pushing changes to Git...")
        
        # Configure Git
        subprocess.run(['git', 'config', 'user.email', 'automation@teatrade.com'], check=True)
        subprocess.run(['git', 'config', 'user.name', 'Tea Trade Automation Bot'], check=True)
        
        # Add all changes
        subprocess.run(['git', 'add', '.'], check=True)
        
        # Check if there are changes
        result = subprocess.run(['git', 'diff', '--staged', '--quiet'], capture_output=True)
        
        if result.returncode == 0:
            logger.info("No changes to commit")
            return True
        
        # Generate commit message
        successful_scrapers = len([r for r in automation_results if r['status'] == 'success'])
        total_scrapers = len(automation_results)
        
        total_lots = db_stats.get('summary', {}).get('total_lots_today', 0)
        total_articles = db_stats.get('summary', {}).get('total_articles_today', 0)
        
        commit_message = f"""🤖 Automated data update {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}

🎯 Collection Summary:
- 📊 Scrapers: {successful_scrapers}/{total_scrapers} successful
- 🍃 Auction lots: {total_lots} collected today
- 📰 News articles: {total_articles} collected today
- 🕐 Next update: {(datetime.now()).strftime('%Y-%m-%d')} 06:00 UTC

✨ Your beautiful TeaTrade design enhanced with fresh market data
🚀 System Status: Operational
🔄 Automation: {successful_scrapers}/{total_scrapers} sources active"""

        # Commit changes
        subprocess.run(['git', 'commit', '-m', commit_message], check=True)
        
        # Push to main branch
        subprocess.run(['git', 'push', 'origin', 'main'], check=True)
        
        logger.info("✅ Successfully committed and pushed changes")
        return True
        
    except subprocess.CalledProcessError as e:
        logger.error(f"Git operation failed: {e}")
        return False
    except Exception as e:
        logger.error(f"Error in Git operations: {e}")
        return False

def main():
    """Main automation execution"""
    start_time = time.time()
    logger.info("🚀 Starting COMPLETE Tea Trade Automation")
    
    # Define all scrapers
    scrapers = [
        Path(__file__).parent / 'scrapers' / 'jthomas' / 'scrape_jthomas_complete.py',
        Path(__file__).parent / 'scrapers' / 'ceylon' / 'scrape_ceylon_complete.py',
        Path(__file__).parent / 'scrapers' / 'forbes' / 'scrape_forbes_complete.py',
        Path(__file__).parent / 'scrapers' / 'tbea' / 'scrape_tbea_complete.py',
        Path(__file__).parent / 'scrapers' / 'atb' / 'scrape_atb_complete.py',
        Path(__file__).parent / 'scrapers' / 'news' / 'scrape_news_complete.py'
    ]
    
    # Run all scrapers
    automation_results = []
    successful_scrapers = 0
    
    for scraper in scrapers:
        if scraper.exists():
            result = run_scraper(scraper)
            automation_results.append(result)
            
            if result['status'] == 'success':
                successful_scrapers += 1
                
            # Brief pause between scrapers
            time.sleep(3)
        else:
            logger.warning(f"⚠️ Scraper not found: {scraper}")
            automation_results.append({
                'scraper': scraper.name,
                'status': 'not_found',
                'duration': 0,
                'output': '',
                'error': 'File not found'
            })
    
    # Get database statistics
    logger.info("📊 Collecting database statistics...")
    db_stats = get_database_stats()
    
    # Generate website data
    logger.info("🌐 Generating website data...")
    generate_website_data()
    
    # Update website timestamp
    logger.info("🕐 Updating website timestamp...")
    update_website_timestamp()
    
    # Commit and push changes
    logger.info("📤 Committing and pushing to Git...")
    git_success = commit_and_push_changes(automation_results, db_stats)
    
    # Final summary
    total_duration = time.time() - start_time
    
    logger.info("="*60)
    logger.info("🎯 AUTOMATION EXECUTION SUMMARY")
    logger.info("="*60)
    logger.info(f"Status: {'SUCCESS' if successful_scrapers > 0 else 'FAILED'}")
    logger.info(f"Successful Scrapers: {successful_scrapers}/{len(scrapers)}")
    logger.info(f"Total Duration: {total_duration:.1f} seconds")
    logger.info(f"Git Push: {'SUCCESS' if git_success else 'FAILED'}")
    
    if db_stats.get('summary'):
        summary = db_stats['summary']
        logger.info(f"Data Collected Today:")
        logger.info(f"  - Auction lots: {summary.get('total_lots_today', 0)}")
        logger.info(f"  - News articles: {summary.get('total_articles_today', 0)}")
    
    logger.info("="*60)
    
    # Return success code
    return 0 if successful_scrapers > 0 and git_success else 1

if __name__ == "__main__":
    exit(main())
