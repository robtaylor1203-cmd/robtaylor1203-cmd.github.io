#!/usr/bin/env python3
"""Check system status"""

import os
from pathlib import Path
from datetime import datetime

def check_system_status():
    print("🔍 TeaTrade Advanced Scraper System Status")
    print("=" * 45)
    
    # Check directory structure
    base_path = Path("automation/scrapers_v2")
    required_dirs = [
        "core", "sites", "data_warehouse", "logs",
        "sites/mombasa", "sites/kolkata", "sites/colombo",
        "data_warehouse/raw", "data_warehouse/processed", "data_warehouse/analysis"
    ]
    
    print("\n📁 Directory Structure:")
    for dir_path in required_dirs:
        full_path = base_path / dir_path
        status = "✅" if full_path.exists() else "❌"
        print(f"   {status} {dir_path}")
    
    # Check core files
    print("\n📄 Core Files:")
    core_files = [
        "core/advanced_scraper.py",
        "sites/mombasa/atb_ltd_scraper.py", 
        "sites/kolkata/j_thomas_scraper.py",
        "sites/colombo/sri_lankan_scrapers.py",
        "master_controller.py",
        "run_advanced_scrapers.py"
    ]
    
    for file_path in core_files:
        full_path = base_path / file_path
        status = "✅" if full_path.exists() else "❌"
        size = f"({full_path.stat().st_size // 1024}KB)" if full_path.exists() else ""
        print(f"   {status} {file_path} {size}")
    
    # Check data warehouse
    print("\n💾 Data Warehouse:")
    warehouse_path = base_path / "data_warehouse"
    
    if warehouse_path.exists():
        raw_files = len(list((warehouse_path / "raw").glob("*.json")))
        processed_files = len(list((warehouse_path / "processed").glob("*.json")))
        analysis_files = len(list((warehouse_path / "analysis").glob("*.csv")))
        
        print(f"   Raw data files: {raw_files}")
        print(f"   Processed files: {processed_files}")
        print(f"   Analysis files: {analysis_files}")
    else:
        print("   ❌ Data warehouse not found")
    
    # Check integration
    print("\n🔗 TeaTrade Integration:")
    consolidated_path = Path("Data/Consolidated")
    if consolidated_path.exists():
        consolidated_files = len(list(consolidated_path.glob("*_consolidated.json")))
        print(f"   ✅ Consolidated files: {consolidated_files}")
    else:
        print("   ❌ Consolidated directory not found")
    
    # Check logs
    print("\n📋 Recent Logs:")
    logs_path = base_path / "logs"
    if logs_path.exists():
        log_files = list(logs_path.glob("*.log"))
        recent_logs = sorted(log_files, key=lambda x: x.stat().st_mtime, reverse=True)[:3]
        
        for log_file in recent_logs:
            mod_time = datetime.fromtimestamp(log_file.stat().st_mtime)
            print(f"   📄 {log_file.name} (modified: {mod_time.strftime('%Y-%m-%d %H:%M')})")
    else:
        print("   ❌ No logs directory found")
    
    print(f"\n✅ System check completed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

if __name__ == "__main__":
    check_system_status()
