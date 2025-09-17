#!/bin/bash

# Tea Trade Automation Startup Script
# Simple script to run complete automation

set -e

echo "🍃 Starting Tea Trade Automation..."
echo "=================================="

# Check if we're in the right directory
if [ ! -f "automation/master_automation_complete.py" ]; then
    echo "❌ Error: automation/master_automation_complete.py not found"
    echo "Please run this script from the root of your tea trade project"
    exit 1
fi

# Check if virtual environment exists
if [ ! -d "automation_venv" ]; then
    echo "❌ Error: automation_venv not found"
    echo "Please create virtual environment first"
    exit 1
fi

# Activate virtual environment
echo "🐍 Activating virtual environment..."
source automation_venv/bin/activate

# Check if PostgreSQL is running
echo "🗄️ Checking database connection..."
if ! psql -h localhost -U tea_admin -d tea_trade_data -c "SELECT 1;" > /dev/null 2>&1; then
    echo "❌ Error: Cannot connect to PostgreSQL database"
    echo "Please ensure PostgreSQL is running and database is set up"
    exit 1
fi

# Run the automation
echo "🚀 Running complete automation..."
cd automation
python master_automation_complete.py

# Check if automation was successful
if [ $? -eq 0 ]; then
    echo ""
    echo "✅ Automation completed successfully!"
    echo ""
    echo "📊 Latest data summary:"
    psql -h localhost -U tea_admin -d tea_trade_data -c "
    SELECT 
        source, 
        COUNT(*) as total_lots,
        COUNT(CASE WHEN scrape_timestamp >= CURRENT_DATE THEN 1 END) as today_lots
    FROM auction_lots 
    GROUP BY source
    ORDER BY total_lots DESC;
    "
    echo ""
    echo "🌐 Website data has been updated with latest information"
    echo "🔄 Changes have been committed and pushed to Git"
else
    echo "❌ Automation failed"
    exit 1
fi
