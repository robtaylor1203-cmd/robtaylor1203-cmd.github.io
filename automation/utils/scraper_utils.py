"""
Utility functions for scraper integration
"""
import os
import sys
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
import importlib.util

def setup_logging(name):
    """Setup logging for scrapers"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(name)

def load_scraper_module(scraper_path):
    """Dynamically load a scraper module"""
    try:
        spec = importlib.util.spec_from_file_location("scraper", scraper_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    except Exception as e:
        logging.error(f"Failed to load scraper {scraper_path}: {e}")
        return None

def save_scraper_output(data, output_file):
    """Save scraper output in standardized format"""
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Add metadata
    if isinstance(data, dict):
        data['_metadata'] = {
            'scraped_at': datetime.now().isoformat(),
            'scraper_version': '2.0.0'
        }
    elif isinstance(data, list) and data:
        # Add metadata to first item if it's a list
        metadata = {
            '_metadata': {
                'scraped_at': datetime.now().isoformat(),
                'scraper_version': '2.0.0',
                'total_records': len(data)
            }
        }
        data = [metadata] + data
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    return output_path

def get_output_filename(scraper_name, data_type='data'):
    """Generate standardized output filename"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    return f"{scraper_name}_{data_type}_{timestamp}.json"
