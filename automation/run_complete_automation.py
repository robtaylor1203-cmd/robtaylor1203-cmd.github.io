#!/usr/bin/env python3
"""
Complete TeaTrade Automation System
Uses existing scrapers for real data collection
"""

import os
import sys
import json
import asyncio
import logging
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
import glob

# Add utils to path
sys.path.append(str(Path(__file__).parent))
from utils.scraper_utils import setup_logging, save_scraper_output, get_output_filename

class TeaTradeAutomation:
    """Complete automation using existing scrapers"""
    
    def __init__(self):
        self.logger = setup_logging('TEATRADE_AUTOMATION')
        self.github_actions = os.getenv('GITHUB_ACTIONS', 'false').lower() == 'true'
        self.run_mode = os.getenv('RUN_MODE', 'full')
        
        self.base_dir = Path(__file__).parent
        self.scrapers_dir = self.base_dir / "scrapers"
        self.output_dir = self.base_dir.parent / "data" / "latest"
        self.logs_dir = self.base_dir / "logs"
        
        # Create directories
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        
        self.logger.info(f"🚀 TeaTrade Automation initialized")
        self.logger.info(f"📊 Mode: {self.run_mode}")
        self.logger.info(f"🔧 GitHub Actions: {self.github_actions}")
        
        # Track scraper results
        self.scraper_results = {}
    
    def discover_scrapers(self):
        """Discover all available scrapers"""
        scrapers = []
        
        # Find all Python files in scrapers directory
        for scraper_file in self.scrapers_dir.rglob("*.py"):
            if scraper_file.name.startswith('scraper_') or 'scraper' in scraper_file.name:
                relative_path = scraper_file.relative_to(self.scrapers_dir)
                scraper_info = {
                    'name': scraper_file.stem,
                    'path': scraper_file,
                    'location': relative_path.parent.name if relative_path.parent.name != '.' else 'root',
                    'type': self.classify_scraper(scraper_file.name)
                }
                scrapers.append(scraper_info)
        
        self.logger.info(f"🔍 Discovered {len(scrapers)} scrapers:")
        for scraper in scrapers:
            self.logger.info(f"  📄 {scraper['name']} ({scraper['location']}) - {scraper['type']}")
        
        return scrapers
    
    def classify_scraper(self, filename):
        """Classify scraper type based on filename"""
        filename_lower = filename.lower()
        
        if 'jthomas' in filename_lower or 'kolkata' in filename_lower:
            return 'india_auctions'
        elif 'atb' in filename_lower or 'tbea' in filename_lower or 'mombasa' in filename_lower:
            return 'kenya_auctions'
        elif 'forbes' in filename_lower or 'colombo' in filename_lower:
            return 'sri_lanka_auctions'
        elif 'ceylon' in filename_lower:
            return 'ceylon_reports'
        elif 'news' in filename_lower:
            return 'news'
        else:
            return 'general'
    
    async def run_scraper(self, scraper_info):
        """Run a single scraper"""
        scraper_name = scraper_info['name']
        scraper_path = scraper_info['path']
        
        try:
            self.logger.info(f"🌐 Running {scraper_name}...")
            
            # Run the scraper as a subprocess
            env = os.environ.copy()
            env['PYTHONPATH'] = str(self.base_dir)
            
            cmd = [sys.executable, str(scraper_path)]
            
            # Add output directory argument if scraper supports it
            output_file = self.output_dir / f"{scraper_name}_output.json"
            
            # Try to run with timeout
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env,
                cwd=scraper_path.parent
            )
            
            try:
                stdout, stderr = await asyncio.wait_for(
                    process.communicate(),
                    timeout=300  # 5 minutes timeout
                )
                
                if process.returncode == 0:
                    self.logger.info(f"✅ {scraper_name} completed successfully")
                    
                    # Try to find output files
                    possible_outputs = [
                        scraper_path.parent / f"{scraper_name}_data.json",
                        scraper_path.parent / "output.json",
                        scraper_path.parent / "data.json",
                    ]
                    
                    output_found = False
                    for possible_output in possible_outputs:
                        if possible_output.exists():
                            # Copy to standard location
                            import shutil
                            shutil.copy2(possible_output, output_file)
                            output_found = True
                            break
                    
                    if not output_found and stdout:
                        # Try to parse stdout as JSON
                        try:
                            output_data = json.loads(stdout.decode())
                            with open(output_file, 'w') as f:
                                json.dump(output_data, f, indent=2)
                            output_found = True
                        except:
                            pass
                    
                    self.scraper_results[scraper_name] = {
                        'status': 'success',
                        'output_file': str(output_file) if output_found else None,
                        'stdout': stdout.decode()[:500] if stdout else '',
                        'type': scraper_info['type']
                    }
                    
                else:
                    error_msg = stderr.decode() if stderr else 'Unknown error'
                    self.logger.warning(f"⚠️ {scraper_name} failed: {error_msg[:200]}")
                    
                    self.scraper_results[scraper_name] = {
                        'status': 'failed',
                        'error': error_msg[:500],
                        'type': scraper_info['type']
                    }
                    
            except asyncio.TimeoutError:
                process.kill()
                self.logger.warning(f"⏰ {scraper_name} timed out after 5 minutes")
                
                self.scraper_results[scraper_name] = {
                    'status': 'timeout',
                    'error': 'Scraper execution timed out',
                    'type': scraper_info['type']
                }
                
        except Exception as e:
            self.logger.error(f"❌ Error running {scraper_name}: {e}")
            
            self.scraper_results[scraper_name] = {
                'status': 'error',
                'error': str(e),
                'type': scraper_info['type']
            }
    
    async def run_all_scrapers(self):
        """Run all discovered scrapers"""
        scrapers = self.discover_scrapers()
        
        if not scrapers:
            self.logger.warning("⚠️ No scrapers found, generating sample data...")
            await self.generate_sample_data()
            return
        
        # Run scrapers based on mode
        if self.run_mode == 'test-only':
            # Run only first 2 scrapers for testing
            scrapers = scrapers[:2]
            self.logger.info(f"🧪 Test mode: running {len(scrapers)} scrapers")
        
        # Run scrapers in parallel (with concurrency limit)
        semaphore = asyncio.Semaphore(3)  # Max 3 concurrent scrapers
        
        async def run_with_semaphore(scraper_info):
            async with semaphore:
                await self.run_scraper(scraper_info)
        
        tasks = [run_with_semaphore(scraper) for scraper in scrapers]
        await asyncio.gather(*tasks, return_exceptions=True)
        
        # Generate consolidated data
        await self.consolidate_data()
    
    async def generate_sample_data(self):
        """Generate sample data if no scrapers available"""
        from datetime import datetime
        import random
        
        # Generate basic sample data
        summary = {
            "last_updated": datetime.now().isoformat() + "Z",
            "total_auctions": random.randint(150, 200),
            "total_news": random.randint(15, 25),
            "active_centers": ["Kolkata", "Guwahati", "Colombo", "Kandy", "Mombasa"],
            "status": "sample_data",
            "run_mode": self.run_mode,
            "scrapers_available": len(self.discover_scrapers())
        }
        
        with open(self.output_dir / "summary.json", "w") as f:
            json.dump(summary, f, indent=2)
        
        self.logger.info("📊 Sample data generated")
    
    async def consolidate_data(self):
        """Consolidate all scraper outputs"""
        self.logger.info("📊 Consolidating scraper outputs...")
        
        # Collect all successful outputs
        all_auctions = []
        all_news = []
        
        successful_scrapers = 0
        failed_scrapers = 0
        
        for scraper_name, result in self.scraper_results.items():
            if result['status'] == 'success' and result.get('output_file'):
                successful_scrapers += 1
                
                try:
                    with open(result['output_file'], 'r') as f:
                        scraper_data = json.load(f)
                    
                    # Process based on scraper type
                    if result['type'] in ['india_auctions', 'kenya_auctions', 'sri_lanka_auctions']:
                        if isinstance(scraper_data, list):
                            all_auctions.extend(scraper_data)
                        elif isinstance(scraper_data, dict) and 'auctions' in scraper_data:
                            all_auctions.extend(scraper_data['auctions'])
                    
                    elif result['type'] == 'news':
                        if isinstance(scraper_data, list):
                            all_news.extend(scraper_data)
                        elif isinstance(scraper_data, dict) and 'news' in scraper_data:
                            all_news.extend(scraper_data['news'])
                
                except Exception as e:
                    self.logger.warning(f"⚠️ Error processing {scraper_name} output: {e}")
                    failed_scrapers += 1
            else:
                failed_scrapers += 1
        
        # Generate summary
        summary = {
            "last_updated": datetime.now().isoformat() + "Z",
            "total_auctions": len(all_auctions),
            "total_news": len(all_news),
            "successful_scrapers": successful_scrapers,
            "failed_scrapers": failed_scrapers,
            "scraper_results": self.scraper_results,
            "status": "real_data" if successful_scrapers > 0 else "no_data",
            "run_mode": self.run_mode,
            "collection_time": datetime.now().strftime("%Y-%m-%d %H:%M UTC")
        }
        
        # Save consolidated data
        with open(self.output_dir / "summary.json", "w") as f:
            json.dump(summary, f, indent=2)
        
        with open(self.output_dir / "auction_data.json", "w") as f:
            json.dump(all_auctions, f, indent=2)
        
        with open(self.output_dir / "news_data.json", "w") as f:
            json.dump(all_news, f, indent=2)
        
        # Save detailed scraper log
        with open(self.logs_dir / f"automation_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json", "w") as f:
            json.dump({
                'execution_time': datetime.now().isoformat(),
                'scraper_results': self.scraper_results,
                'summary': summary
            }, f, indent=2)
        
        self.logger.info(f"✅ Consolidation complete: {len(all_auctions)} auctions, {len(all_news)} news items")

async def main():
    """Main execution function"""
    try:
        automation = TeaTradeAutomation()
        await automation.run_all_scrapers()
        
        automation.logger.info("🎉 TeaTrade automation completed successfully!")
        return True
        
    except Exception as e:
        logging.error(f"❌ Automation failed: {e}")
        return False

if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)
