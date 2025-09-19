#!/usr/bin/env python3
"""Git Automation for TeaTrade System"""

import subprocess
import logging
import os
from datetime import datetime
from pathlib import Path

class GitAutomation:
    def __init__(self, repo_path: str = "/home/robtaylor1203/robtaylor1203-cmd.github.io"):
        self.repo_path = Path(repo_path)
        
    def safe_git_push(self) -> bool:
        """Safely push changes to GitHub with conflict resolution"""
        try:
            os.chdir(self.repo_path)
            
            # Step 1: Pull latest changes first
            logging.info("📥 Pulling latest changes from GitHub...")
            pull_result = subprocess.run(
                ['git', 'pull', 'origin', 'main', '--allow-unrelated-histories'],
                capture_output=True, text=True
            )
            
            if pull_result.returncode != 0:
                logging.warning(f"Pull had issues: {pull_result.stderr}")
                # Try merge strategy
                subprocess.run(['git', 'merge', '--strategy=ours', 'origin/main'], 
                             capture_output=True)
            
            # Step 2: Add all new data files
            logging.info("📁 Adding new data files...")
            subprocess.run(['git', 'add', 'Data/Consolidated/'], check=True)
            subprocess.run(['git', 'add', 'automation/scrapers_v2/data_warehouse/'], check=True)
            subprocess.run(['git', 'add', 'market-reports-library.json'], check=True)
            
            # Step 3: Check if there are changes to commit
            status_result = subprocess.run(
                ['git', 'status', '--porcelain'],
                capture_output=True, text=True
            )
            
            if not status_result.stdout.strip():
                logging.info("ℹ️ No changes to commit")
                return True
                
            # Step 4: Commit changes
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M")
            commit_msg = f"🤖 Automated scraper update - {timestamp}"
            
            logging.info("💾 Committing changes...")
            subprocess.run(['git', 'commit', '-m', commit_msg], check=True)
            
            # Step 5: Push changes
            logging.info("🚀 Pushing to GitHub...")
            push_result = subprocess.run(
                ['git', 'push', 'origin', 'main'],
                capture_output=True, text=True
            )
            
            if push_result.returncode != 0:
                logging.error(f"Push failed: {push_result.stderr}")
                return False
                
            logging.info("✅ Successfully pushed changes to GitHub")
            return True
            
        except Exception as e:
            logging.error(f"Git automation failed: {e}")
            return False
            
    def create_backup_branch(self) -> bool:
        """Create backup branch before major operations"""
        try:
            branch_name = f"backup-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
            subprocess.run(['git', 'checkout', '-b', branch_name], check=True)
            subprocess.run(['git', 'push', 'origin', branch_name], check=True)
            subprocess.run(['git', 'checkout', 'main'], check=True)
            
            logging.info(f"✅ Created backup branch: {branch_name}")
            return True
            
        except Exception as e:
            logging.error(f"Backup branch creation failed: {e}")
            return False

if __name__ == "__main__":
    git_auto = GitAutomation()
    git_auto.safe_git_push()
