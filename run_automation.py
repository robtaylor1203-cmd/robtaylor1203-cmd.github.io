#!/usr/bin/env python3
"""
TeaTrade Automation Backend
Separate from your beautiful website design
"""

import json
import os
from datetime import datetime
from pathlib import Path

def create_sample_data():
    """Create sample data for testing your enhanced website"""
    
    # Create data directory
    os.makedirs('data/latest', exist_ok=True)
    
    # Sample summary data
    summary = {
        "total_auctions": 245,
        "total_news": 18,
        "active_gardens": 127,
        "last_updated": datetime.now().isoformat(),
        "data_quality": "High",
        "system_status": "Operational"
    }
    
    # Sample news data
    news = [
        {
            "title": "Tea Prices Rise 15% in Global Markets",
            "summary": "International tea auction prices show significant increase across major growing regions...",
            "source": "Tea Industry Times",
            "url": "#",
            "publish_date": datetime.now().isoformat()
        },
        {
            "title": "Sustainable Tea Farming Initiative Launched",
            "summary": "New environmental standards adopted by leading tea gardens in India and Sri Lanka...",
            "source": "Sustainable Agriculture News",
            "url": "#",
            "publish_date": datetime.now().isoformat()
        }
    ]
    
    # Sample auction data
    auctions = [
        {
            "location": "Kolkata",
            "garden": "Makaibari Estate",
            "grade": "FTGFOP1",
            "price": 285.50,
            "quantity": 1250,
            "date": datetime.now().isoformat()
        },
        {
            "location": "Colombo", 
            "garden": "Kenilworth Estate",
            "grade": "Pekoe",
            "price": 195.75,
            "quantity": 2100,
            "date": datetime.now().isoformat()
        }
    ]
    
    # Write data files
    with open('data/latest/summary.json', 'w') as f:
        json.dump(summary, f, indent=2)
    
    with open('data/latest/news_data.json', 'w') as f:
        json.dump(news, f, indent=2)
    
    with open('data/latest/auction_data.json', 'w') as f:
        json.dump(auctions, f, indent=2)
    
    print("✅ Sample data created for your enhanced website")
    print("🌐 Your beautiful design now has live data features!")

if __name__ == "__main__":
    create_sample_data()
