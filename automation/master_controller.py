#!/usr/bin/env python3
"""
Master Tea Trade Automation Controller - Enterprise Grade
Orchestrates all scrapers with comprehensive error handling and monitoring
"""

import asyncio
import json
import subprocess
import sys
import time
import signal
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import logging
import psutil
import concurrent.futures
from dataclasses import dataclass

# Add utils to path
sys.path.append(str(Path(__file__).parent))
from utils.pipeline_utils import (
    setup_logging, get_db_connection, generate_weekly_reports
)

@dataclass
class ScraperConfig:
    """Configuration for individual scrapers"""
    name: str
    script_path: Path
    timeout: int = 1800  # 30 minutes default
    retries: int = 2
    parallel: bool = True
    dependencies: List[str] = None

class MasterAutomationController:
    """Enterprise-grade automation controller for all tea market scrapers"""
    
    def __init__(self):
        self.logger = setup_logging('MASTER_CONTROLLER')
        self.execution_report = {
            'start_time': datetime.now().isoformat(),
            'scrapers': {},
            'overall_status': 'running',
            'total_records': 0,
            'errors': []
        }
        
        # Configure all scrapers
        self.scrapers = {
            'jthomas': ScraperConfig(
                name='J Thomas India',
                script_path=Path(__file__).parent / 'scrapers' / 'jthomas' / 'scrape_jthomas_complete.py',
                timeout=1800,  # 30 minutes
                retries=2,
                parallel=True
            ),
            'ceylon': ScraperConfig(
                name='Ceylon Tea Brokers',
                script_path=Path(__file__).parent / 'scrapers' / 'ceylon' / 'scrape_ceylon_complete.py',
                timeout=600,  # 10 minutes
                retries=2,
                parallel=True
            ),
            'forbes': ScraperConfig(
                name='Forbes Tea',
                script_path=Path(__file__).parent / 'scrapers' / 'forbes' / 'scrape_forbes_complete.py',
                timeout=600,  # 10 minutes
                retries=2,
                parallel=True
            ),
            'tbea': ScraperConfig(
                name='TBEA Kenya',
                script_path=Path(__file__).parent / 'scrapers' / 'tbea' / 'scrape_tbea_complete.py',
                timeout=900,  # 15 minutes
                retries=2,
                parallel=True
            ),
            'atb': ScraperConfig(
                name='ATB Kenya',
                script_path=Path(__file__).parent / 'scrapers' / 'atb' / 'scrape_atb_complete.py',
                timeout=900,  # 15 minutes
                retries=2,
                parallel=True
            ),
            'news': ScraperConfig(
                name='Tea Industry News',
                script_path=Path(__file__).parent / 'scrapers' / 'news' / 'scrape_news_complete.py',
                timeout=600,  # 10 minutes
                retries=1,
                parallel=True
            )
        }
    
    def check_system_resources(self) -> Tuple[bool, str]:
        """Check system resources before starting automation"""
        
        try:
            # Check memory usage
            memory = psutil.virtual_memory()
            if memory.percent > 85:
                return False, f"High memory usage: {memory.percent}%"
            
            # Check disk space
            disk = psutil.disk_usage('/')
            free_gb = disk.free / (1024**3)
            if free_gb < 2:
                return False, f"Low disk space: {free_gb:.1f}GB free"
            
            # Check database connectivity
            conn = get_db_connection()
            if not conn:
                return False, "Database connection failed"
            
            conn.close()
            
            return True, f"Resources OK: {memory.percent}% memory, {free_gb:.1f}GB free"
            
        except Exception as e:
            return False, f"Resource check failed: {e}"
    
    def run_scraper(self, scraper_id: str, config: ScraperConfig) -> Dict:
        """Run individual scraper with comprehensive monitoring"""
        
        start_time = datetime.now()
        result = {
            'scraper_id': scraper_id,
            'name': config.name,
            'status': 'running',
            'start_time': start_time.isoformat(),
            'duration': 0,
            'records_collected': 0,
            'errors': [],
            'retries_used': 0
        }
        
        try:
            self.logger.info(f"🚀 Starting {config.name}")
            
            for attempt in range(config.retries + 1):
                try:
                    # Run scraper subprocess
                    process = subprocess.Popen(
                        [sys.executable, str(config.script_path)],
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        text=True
                    )
                    
                    # Wait for completion with timeout
                    stdout, stderr = process.communicate(timeout=config.timeout)
                    
                    if process.returncode == 0:
                        result['status'] = 'success'
                        result['records_collected'] = self.extract_record_count(stdout)
                        self.logger.info(f"✅ {config.name} completed successfully")
                        break
                    else:
                        error_msg = f"Exit code {process.returncode}: {stderr}"
                        result['errors'].append(error_msg)
                        
                        if attempt < config.retries:
                            result['retries_used'] += 1
                            self.logger.warning(f"⚠️ {config.name} failed, retrying ({attempt + 1}/{config.retries})")
                            time.sleep(30)  # Wait before retry
                        else:
                            result['status'] = 'failed'
                            self.logger.error(f"❌ {config.name} failed after {config.retries} retries")
                
                except subprocess.TimeoutExpired:
                    process.kill()
                    error_msg = f"Timeout after {config.timeout} seconds"
                    result['errors'].append(error_msg)
                    
                    if attempt < config.retries:
                        result['retries_used'] += 1
                        self.logger.warning(f"⏰ {config.name} timeout, retrying ({attempt + 1}/{config.retries})")
                        time.sleep(60)  # Longer wait after timeout
                    else:
                        result['status'] = 'timeout'
                        self.logger.error(f"❌ {config.name} timeout after {config.retries} retries")
                
                except Exception as e:
                    error_msg = f"Execution error: {str(e)}"
                    result['errors'].append(error_msg)
                    
                    if attempt < config.retries:
                        result['retries_used'] += 1
                        self.logger.warning(f"⚠️ {config.name} error, retrying ({attempt + 1}/{config.retries}): {e}")
                        time.sleep(30)
                    else:
                        result['status'] = 'error'
                        self.logger.error(f"❌ {config.name} error after {config.retries} retries: {e}")
        
        except Exception as e:
            result['status'] = 'error'
            result['errors'].append(f"Critical error: {str(e)}")
            self.logger.error(f"❌ Critical error in {config.name}: {e}")
        
        finally:
            end_time = datetime.now()
            result['end_time'] = end_time.isoformat()
            result['duration'] = (end_time - start_time).total_seconds()
        
        return result
    
    def extract_record_count(self, output: str) -> int:
        """Extract number of records from scraper output"""
        
        import re
        
        # Look for patterns like "123 records", "456 lots", "789 articles"
        patterns = [
            r'(\d+)\s+(?:records?|lots?|articles?)',
            r'(?:saved|collected|extracted)\s+(\d+)',
            r'total:\s*(\d+)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, output, re.IGNORECASE)
            if match:
                try:
                    return int(match.group(1))
                except:
                    continue
        
        return 0
    
    def run_parallel_scrapers(self, scrapers_to_run: List[str]) -> Dict[str, Dict]:
        """Run scrapers in parallel"""
        
        results = {}
        
        try:
            self.logger.info(f"🔄 Running {len(scrapers_to_run)} scrapers in parallel")
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
                # Submit all scraper tasks
                future_to_scraper = {
                    executor.submit(self.run_scraper, scraper_id, self.scrapers[scraper_id]): scraper_id
                    for scraper_id in scrapers_to_run
                }
                
                # Collect results as they complete
                for future in concurrent.futures.as_completed(future_to_scraper):
                    scraper_id = future_to_scraper[future]
                    try:
                        result = future.result()
                        results[scraper_id] = result
                        
                        status_emoji = "✅" if result['status'] == 'success' else "❌"
                        self.logger.info(f"{status_emoji} {result['name']}: {result['status']} ({result['records_collected']} records)")
                        
                    except Exception as e:
                        results[scraper_id] = {
                            'scraper_id': scraper_id,
                            'status': 'error',
                            'errors': [f"Parallel execution error: {str(e)}"]
                        }
                        self.logger.error(f"❌ Parallel execution error for {scraper_id}: {e}")
        
        except Exception as e:
            self.logger.error(f"❌ Parallel execution failed: {e}")
        
        return results
    
    def run_sequential_scrapers(self, scrapers_to_run: List[str]) -> Dict[str, Dict]:
        """Run scrapers sequentially"""
        
        results = {}
        
        try:
            self.logger.info(f"⏭️ Running {len(scrapers_to_run)} scrapers sequentially")
            
            for scraper_id in scrapers_to_run:
                result = self.run_scraper(scraper_id, self.scrapers[scraper_id])
                results[scraper_id] = result
                
                status_emoji = "✅" if result['status'] == 'success' else "❌"
                self.logger.info(f"{status_emoji} {result['name']}: {result['status']} ({result['records_collected']} records)")
                
                # Brief pause between scrapers
                time.sleep(10)
        
        except Exception as e:
            self.logger.error(f"❌ Sequential execution failed: {e}")
        
        return results
    
    def post_processing(self) -> bool:
        """Execute post-processing tasks"""
        
        try:
            self.logger.info("🔄 Starting post-processing")
            
            # Generate weekly reports
            if generate_weekly_reports():
                self.logger.info("✅ Weekly reports generated")
            else:
                self.logger.warning("⚠️ Weekly report generation failed")
            
            # Update data quality metrics
            self.update_data_quality_metrics()
            
            # Generate data exports for website
            self.generate_website_data_exports()
            
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Post-processing failed: {e}")
            return False
    
    def update_data_quality_metrics(self):
        """Update data quality tracking"""
        
        try:
            conn = get_db_connection()
            if not conn:
                return
            
            cursor = conn.cursor()
            
            # Calculate data quality scores for each source
            sources = ['J_THOMAS', 'CEYLON', 'FORBES', 'TBEA', 'ATB']
            
            for source in sources:
                cursor.execute("""
                    INSERT INTO data_quality_log (source, check_type, quality_score, issues_found)
                    SELECT 
                        %s,
                        'daily_completeness',
                        CASE 
                            WHEN COUNT(*) > 0 THEN 100.0
                            ELSE 0.0
                        END,
                        0
                    FROM auction_lots
                    WHERE source = %s
                    AND scrape_timestamp >= CURRENT_DATE
                """, (source, source))
            
            conn.commit()
            conn.close()
            
        except Exception as e:
            self.logger.warning(f"Data quality update error: {e}")
    
    def generate_website_data_exports(self):
        """Generate JSON exports for website"""
        
        try:
            conn = get_db_connection()
            if not conn:
                return
            
            cursor = conn.cursor()
            
            # Export summary data
            cursor.execute("""
                SELECT 
                    source,
                    COUNT(*) as total_lots,
                    SUM(quantity_kg) as total_quantity,
                    AVG(price_per_kg) as avg_price,
                    MAX(scrape_timestamp) as last_updated
                FROM auction_lots
                WHERE scrape_timestamp >= CURRENT_DATE - INTERVAL '7 days'
                GROUP BY source
            """)
            
            summary_data = []
            for row in cursor.fetchall():
                summary_data.append({
                    'source': row[0],
                    'total_lots': row[1],
                    'total_quantity': row[2],
                    'avg_price': float(row[3]) if row[3] else 0,
                    'last_updated': row[4].isoformat() if row[4] else None
                })
            
            # Save to data directory for website
            data_dir = Path(__file__).parent.parent / 'data' / 'latest'
            data_dir.mkdir(parents=True, exist_ok=True)
            
            with open(data_dir / 'summary.json', 'w') as f:
                json.dump({
                    'last_updated': datetime.now().isoformat(),
                    'total_sources': len(summary_data),
                    'sources': summary_data,
                    'system_status': 'operational'
                }, f, indent=2)
            
            conn.close()
            self.logger.info("✅ Website data exports generated")
            
        except Exception as e:
            self.logger.warning(f"Website export error: {e}")
    
    def run_complete_automation(self, parallel: bool = True, scrapers: List[str] = None) -> Dict:
        """Execute complete automation workflow"""
        
        try:
            self.logger.info("🚀 Starting COMPLETE Tea Trade Automation")
            
            # Check system resources
            resource_ok, resource_msg = self.check_system_resources()
            if not resource_ok:
                self.execution_report['overall_status'] = 'failed'
                self.execution_report['errors'].append(f"Resource check failed: {resource_msg}")
                return self.execution_report
            
            self.logger.info(f"✅ {resource_msg}")
            
            # Determine which scrapers to run
            scrapers_to_run = scrapers or list(self.scrapers.keys())
            
            # Run scrapers
            if parallel:
                scraper_results = self.run_parallel_scrapers(scrapers_to_run)
            else:
                scraper_results = self.run_sequential_scrapers(scrapers_to_run)
            
            # Update execution report
            self.execution_report['scrapers'] = scraper_results
            
            # Calculate totals
            successful_scrapers = 0
            total_records = 0
            
            for result in scraper_results.values():
                if result.get('status') == 'success':
                    successful_scrapers += 1
                    total_records += result.get('records_collected', 0)
                
                if result.get('errors'):
                    self.execution_report['errors'].extend(result['errors'])
            
            self.execution_report['total_records'] = total_records
            self.execution_report['successful_scrapers'] = successful_scrapers
            self.execution_report['total_scrapers'] = len(scrapers_to_run)
            
            # Determine overall status
            if successful_scrapers == 0:
                self.execution_report['overall_status'] = 'failed'
            elif successful_scrapers < len(scrapers_to_run):
                self.execution_report['overall_status'] = 'partial_success'
            else:
                self.execution_report['overall_status'] = 'success'
            
            # Post-processing
            if successful_scrapers > 0:
                post_processing_success = self.post_processing()
                if not post_processing_success:
                    self.execution_report['errors'].append("Post-processing failed")
            
            # Final logging
            end_time = datetime.now()
            self.execution_report['end_time'] = end_time.isoformat()
            self.execution_report['total_duration'] = (
                end_time - datetime.fromisoformat(self.execution_report['start_time'])
            ).total_seconds()
            
            self.logger.info(f"🎉 Automation complete: {self.execution_report['overall_status']}")
            self.logger.info(f"📊 Total records: {total_records}")
            self.logger.info(f"⏱️ Duration: {self.execution_report['total_duration']:.1f} seconds")
            
            return self.execution_report
            
        except Exception as e:
            self.execution_report['overall_status'] = 'error'
            self.execution_report['errors'].append(f"Critical automation error: {str(e)}")
            self.logger.error(f"❌ Critical automation error: {e}")
            return self.execution_report

def main():
    """Main execution function"""
    
    import argparse
    
    parser = argparse.ArgumentParser(description='Tea Trade Master Automation Controller')
    parser.add_argument('--parallel', action='store_true', default=True, help='Run scrapers in parallel')
    parser.add_argument('--sequential', action='store_true', help='Run scrapers sequentially')
    parser.add_argument('--scrapers', nargs='+', help='Specific scrapers to run', 
                        choices=['jthomas', 'ceylon', 'forbes', 'tbea', 'atb', 'news'])
    
    args = parser.parse_args()
    
    # Create controller
    controller = MasterAutomationController()
    
    # Run automation
    parallel_mode = not args.sequential
    result = controller.run_complete_automation(
        parallel=parallel_mode,
        scrapers=args.scrapers
    )
    
    # Print summary
    print(f"\n{'='*60}")
    print("🎯 AUTOMATION EXECUTION SUMMARY")
    print(f"{'='*60}")
    print(f"Status: {result['overall_status'].upper()}")
    print(f"Total Records: {result['total_records']}")
    print(f"Successful Scrapers: {result.get('successful_scrapers', 0)}/{result.get('total_scrapers', 0)}")
    print(f"Duration: {result.get('total_duration', 0):.1f} seconds")
    
    if result.get('errors'):
        print(f"\n⚠️ Errors ({len(result['errors'])}):")
        for error in result['errors']:
            print(f"  - {error}")
    
    # Return appropriate exit code
    if result['overall_status'] in ['success', 'partial_success']:
        return 0
    else:
        return 1

if __name__ == "__main__":
    exit(main())
