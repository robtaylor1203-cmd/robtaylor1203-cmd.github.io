#!/usr/bin/env python3
"""Simple runner for TeaTrade advanced scrapers"""

import asyncio
import sys
import os

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from master_controller import MasterController

async def main():
    print("🚀 Starting TeaTrade Advanced Scraper System")
    print("=" * 50)
    
    controller = MasterController()
    
    try:
        # Run all scrapers
        results = await controller.run_all_scrapers()
        
        # Show summary
        total_success = sum(len([r for r in scraper_results if r.success]) for scraper_results in results.values())
        total_failed = sum(len([r for r in scraper_results if not r.success]) for scraper_results in results.values())
        
        print(f"\n📊 RESULTS SUMMARY:")
        print(f"   Successful: {total_success}")
        print(f"   Failed: {total_failed}")
        
        # Integrate with TeaTrade
        if total_success > 0:
            print("\n🔄 Integrating with TeaTrade system...")
            success = await controller.integrate_with_teatrade(results)
            
            if success:
                print("✅ Integration completed successfully!")
                print("\nYour TeaTrade system now has fresh data!")
                print("Check Data/Consolidated/ for new files")
            else:
                print("❌ Integration failed, but raw data is saved in data warehouse")
        else:
            print("❌ No successful scraping results to integrate")
            
    except Exception as e:
        print(f"❌ System failed: {e}")
        
    print("\n🏁 Advanced scraper run completed")

if __name__ == "__main__":
    asyncio.run(main())
