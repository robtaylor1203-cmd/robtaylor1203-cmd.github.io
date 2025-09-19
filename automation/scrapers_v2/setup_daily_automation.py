#!/usr/bin/env python3
"""Setup daily automation via cron"""

import os
import subprocess
from pathlib import Path

def setup_daily_cron():
    """Setup cron job for daily automation"""
    
    # Create cron job script
    cron_script_path = Path.home() / "teatrade_daily_automation.sh"
    
    script_content = f'''#!/bin/bash
# TeaTrade Daily Automation Script
cd {Path.cwd()}
source venv/bin/activate
python3 automation/scrapers_v2/complete_automation.py >> automation/scrapers_v2/logs/daily_cron.log 2>&1
'''
    
    with open(cron_script_path, 'w') as f:
        f.write(script_content)
        
    # Make executable
    os.chmod(cron_script_path, 0o755)
    
    # Add to cron (runs daily at 6 AM)
    cron_entry = f"0 6 * * * {cron_script_path}"
    
    try:
        # Get current cron jobs
        current_cron = subprocess.run(['crontab', '-l'], capture_output=True, text=True)
        
        if cron_entry not in current_cron.stdout:
            # Add new cron job
            new_cron = current_cron.stdout + cron_entry + "\n"
            
            # Write to temp file
            temp_cron = Path("/tmp/teatrade_cron")
            with open(temp_cron, 'w') as f:
                f.write(new_cron)
                
            # Install new cron
            subprocess.run(['crontab', str(temp_cron)], check=True)
            
            print("✅ Daily automation cron job installed!")
            print("   Runs every day at 6:00 AM")
            print(f"   Script location: {cron_script_path}")
            print("   Logs: automation/scrapers_v2/logs/daily_cron.log")
        else:
            print("ℹ️ Cron job already exists")
            
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to setup cron: {e}")
        print("You can manually add this line to your cron:")
        print(f"   crontab -e")
        print(f"   Add: {cron_entry}")

if __name__ == "__main__":
    setup_daily_cron()
