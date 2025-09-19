#!/usr/bin/env python3
"""Master runner for complete TeaTrade automation system"""

import asyncio
import argparse

from complete_automation import main

async def run_system():
    print("🚀 TeaTrade Complete Automation System")
    print("======================================")
    print()
    print("This will:")
    print("✅ Scrape ALL tea industry sources from your URL list")
    print("✅ Extract comprehensive market data using advanced techniques")
    print("✅ Store data in centralized warehouse")
    print("✅ Integrate with your existing TeaTrade system")
    print("✅ Update consolidated files and market library")
    print("✅ Push everything to GitHub automatically")
    print()
    
    confirm = input("Ready to start complete automation? [Y/n]: ")
    if confirm.lower() in ['y', 'yes', '']:
        return await main()
    else:
        print("❌ Automation cancelled")
        return False

if __name__ == "__main__":
    asyncio.run(run_system())
