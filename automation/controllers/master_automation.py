#!/usr/bin/env python3
"""
Master Tea Trade Automation Controller
Enterprise-grade orchestration with comprehensive error handling
"""

import asyncio
import json
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import logging
import psutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

# Setup comprehensive logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(name)s] %(message)s',
    handlers=[
        logging.FileHandler('automation/logs/master_automation.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

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
    """Enterprise-grade automation controller"""
    
    def __init__(self):
        self.base_dir = Path(__file__).parent.parent
        self.scrapers_dir = self.base_dir / "scrapers"
        self.output_dir = self.base_dir.parent / "data" / "latest"
        self.logs_dir = self.base_dir / "logs"
        
        # Create directories
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        
        # Initialize metrics
        self.execution_start = datetime.now()
        self.scraper_results = {}
        self.system_metrics = {}
        
        logger.info("Master Automation Controller initialized")
    
    def discover_scrapers(self) -> List[ScraperConfig]:
        """Discover and configure all available scrapers"""
        scrapers = []
        
        # Define scraper configurations
        scraper_configs = {
            'jthomas': {'timeout': 2400, 'retries': 3},  # 40 minutes for complex scraping
            'atb': {'timeout': 1800, 'retries': 2},
            'tbea': {'timeout': 1800, 'retries': 2}, 
            'forbes': {'timeout': 1200, 'retries': 2},
            'ceylon': {'timeout': 1500, 'retries': 2},
            'news': {'timeout': 900, 'retries': 1}
        }
        
        # Discover scraper files
        for scraper_file in self.scrapers_dir.rglob("*.py"):
            if scraper_file.name.startswith('scraper_') or 'scraper' in scraper_file.name:
                scraper_name = scraper_file.stem
                
                # Apply specific config if available
                config = scraper_configs.get(
                    next((k for k in scraper_configs.keys() if k in scraper_name.lower()), 'default'),
                    {'timeout': 1200, 'retries': 2}
                )
                
                scraper_config = ScraperConfig(
                    name=scraper_name,
                    script_path=scraper_file,
                    timeout=config['timeout'],
                    retries=config['retries']
                )
                scrapers.append(scraper_config)
        
        logger.info(f"Discovered {len(scrapers)} scrapers for execution")
        return scrapers
    
    async def execute_scraper(self, config: ScraperConfig) -> Dict:
        """Execute a single scraper with comprehensive error handling"""
        scraper_start = time.time()
        
        for attempt in range(config.retries + 1):
            try:
                logger.info(f"Executing {config.name} (attempt {attempt + 1}/{config.retries + 1})")
                
                # Setup environment
                env = os.environ.copy()
                env['PYTHONPATH'] = str(self.base_dir)
                env['SCRAPER_OUTPUT_DIR'] = str(self.output_dir)
                
                # Execute scraper
                process = await asyncio.create_subprocess_exec(
                    sys.executable, str(config.script_path),
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    env=env,
                    cwd=config.script_path.parent
                )
                
                stdout, stderr = await asyncio.wait_for(
                    process.communicate(),
                    timeout=config.timeout
                )
                
                execution_time = time.time() - scraper_start
                
                if process.returncode == 0:
                    # Success - look for output files
                    output_files = self.find_scraper_outputs(config.name)
                    
                    result = {
                        'status': 'success',
                        'execution_time': execution_time,
                        'attempt': attempt + 1,
                        'output_files': output_files,
                        'stdout': stdout.decode()[:1000] if stdout else '',
                        'data_size': sum(f.stat().st_size for f in output_files if f.exists())
                    }
                    
                    logger.info(f"{config.name} completed successfully in {execution_time:.2f}s")
                    return result
                else:
                    error_msg = stderr.decode() if stderr else 'Unknown error'
                    logger.warning(f"{config.name} failed (attempt {attempt + 1}): {error_msg[:200]}")
                    
                    if attempt < config.retries:
                        await asyncio.sleep(30 * (attempt + 1))  # Exponential backoff
                        continue
                    
            except asyncio.TimeoutError:
                logger.error(f"{config.name} timed out after {config.timeout}s")
                if attempt < config.retries:
                    await asyncio.sleep(60)
                    continue
            except Exception as e:
                logger.error(f"Unexpected error in {config.name}: {e}")
                if attempt < config.retries:
                    await asyncio.sleep(30)
                    continue
        
        # All attempts failed
        return {
            'status': 'failed',
            'execution_time': time.time() - scraper_start,
            'attempts': config.retries + 1,
            'error': 'All retry attempts exhausted'
        }
    
    def find_scraper_outputs(self, scraper_name: str) -> List[Path]:
        """Find output files generated by a scraper"""
        output_files = []
        
        # Common output patterns
        patterns = [
            f"{scraper_name}_*.json",
            f"{scraper_name}_data.json",
            "output.json",
            "data.json",
            "*_auction_data.json",
            "*_market_report.json"
        ]
        
        # Search in scraper directory and output directory
        search_dirs = [self.output_dir, self.scrapers_dir]
        
        for search_dir in search_dirs:
            for pattern in patterns:
                output_files.extend(search_dir.rglob(pattern))
        
        return list(set(output_files))  # Remove duplicates
    
    async def run_parallel_execution(self, scrapers: List[ScraperConfig]) -> Dict:
        """Execute scrapers in parallel with resource management"""
        logger.info(f"Starting parallel execution of {len(scrapers)} scrapers")
        
        # Limit concurrent scrapers based on system resources
        max_concurrent = min(4, len(scrapers))  # Max 4 concurrent for stability
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def execute_with_semaphore(config):
            async with semaphore:
                return await self.execute_scraper(config)
        
        # Execute all scrapers
        tasks = [execute_with_semaphore(config) for config in scrapers]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        for i, (scraper, result) in enumerate(zip(scrapers, results)):
            if isinstance(result, Exception):
                self.scraper_results[scraper.name] = {
                    'status': 'error',
                    'error': str(result)
                }
            else:
                self.scraper_results[scraper.name] = result
        
        return self.scraper_results
    
    def generate_execution_summary(self) -> Dict:
        """Generate comprehensive execution summary"""
        execution_time = (datetime.now() - self.execution_start).total_seconds()
        
        successful = sum(1 for r in self.scraper_results.values() if r.get('status') == 'success')
        failed = len(self.scraper_results) - successful
        
        summary = {
            'execution_start': self.execution_start.isoformat(),
            'execution_end': datetime.now().isoformat(),
            'total_execution_time': execution_time,
            'scrapers_executed': len(self.scraper_results),
            'successful_scrapers': successful,
            'failed_scrapers': failed,
            'success_rate': (successful / len(self.scraper_results)) * 100 if self.scraper_results else 0,
            'scraper_details': self.scraper_results,
            'system_metrics': {
                'memory_usage': psutil.Process().memory_info().rss / 1024 / 1024,  # MB
                'cpu_percent': psutil.cpu_percent(interval=1)
            }
        }
        
        # Save summary
        summary_file = self.logs_dir / f"execution_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(summary_file, 'w') as f:
            json.dump(summary, f, indent=2)
        
        logger.info(f"Execution summary: {successful}/{len(self.scraper_results)} scrapers successful")
        return summary

async def main():
    """Main execution function"""
    import os
    
    try:
        controller = MasterAutomationController()
        
        # Discover scrapers
        scrapers = controller.discover_scrapers()
        if not scrapers:
            logger.warning("No scrapers discovered")
            return False
        
        # Execute scrapers
        await controller.run_parallel_execution(scrapers)
        
        # Generate summary
        summary = controller.generate_execution_summary()
        
        # Import and run data consolidation pipeline
        sys.path.append(str(Path(__file__).parent.parent))
        from pipelines.enhanced_consolidation import EnhancedConsolidationPipeline
        
        pipeline = EnhancedConsolidationPipeline(controller.base_dir.parent)
        consolidation_result = await pipeline.run_consolidation()
        
        logger.info("Master automation completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Master automation failed: {e}")
        return False

if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
