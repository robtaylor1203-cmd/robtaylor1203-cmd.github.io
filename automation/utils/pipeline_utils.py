#!/usr/bin/env python3
"""
Complete Pipeline Utilities for Tea Trade Automation
Enterprise-grade data processing, validation, and storage
"""

import json
import logging
import psycopg2
import pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
import re
import hashlib

# Database Configuration
DB_CONFIG = {
    'host': 'localhost',
    'database': 'tea_trade_data',
    'user': 'tea_admin',
    'password': 'secure_password_123',
    'port': '5432'
}

def setup_logging(component_name: str) -> logging.Logger:
    """Setup comprehensive logging for each component"""
    
    # Create logs directory
    log_dir = Path(__file__).parent.parent.parent / "logs"
    log_dir.mkdir(exist_ok=True)
    
    # Configure logger
    logger = logging.getLogger(component_name)
    logger.setLevel(logging.INFO)
    
    # Clear existing handlers
    logger.handlers.clear()
    
    # File handler
    file_handler = logging.FileHandler(
        log_dir / f"{component_name.lower()}.log",
        encoding='utf-8'
    )
    file_handler.setLevel(logging.INFO)
    
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # Formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

def get_db_connection():
    """Get database connection with error handling"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
        return None

def standardize_data_format(raw_data: Dict, source: str) -> Dict:
    """Standardize data format across all sources"""
    
    standardized = {
        'source': source.upper(),
        'scrape_timestamp': datetime.now().isoformat(),
        'data_quality_score': calculate_quality_score(raw_data),
        'processed_data': {},
        'raw_data': raw_data
    }
    
    # Source-specific standardization
    if source.upper() == 'J_THOMAS':
        standardized['processed_data'] = standardize_jthomas_data(raw_data)
    elif source.upper() == 'CEYLON':
        standardized['processed_data'] = standardize_ceylon_data(raw_data)
    elif source.upper() == 'FORBES':
        standardized['processed_data'] = standardize_forbes_data(raw_data)
    elif source.upper() == 'TBEA':
        standardized['processed_data'] = standardize_tbea_data(raw_data)
    elif source.upper() == 'ATB':
        standardized['processed_data'] = standardize_atb_data(raw_data)
    elif source.upper() == 'NEWS':
        standardized['processed_data'] = standardize_news_data(raw_data)
    
    return standardized

def calculate_quality_score(data: Dict) -> float:
    """Calculate data quality score (0-100)"""
    score = 0
    total_checks = 0
    
    # Check for required fields
    required_fields = ['auction_date', 'price', 'quantity']
    for field in required_fields:
        total_checks += 1
        if any(field in str(k).lower() for k in data.keys()):
            score += 1
    
    # Check for completeness
    total_checks += 1
    non_empty_fields = sum(1 for v in data.values() if v and str(v).strip())
    if non_empty_fields / len(data) > 0.7:
        score += 1
    
    return (score / total_checks) * 100 if total_checks > 0 else 0

def save_to_database(data: List[Dict], table: str, source: str) -> bool:
    """Save standardized data to database"""
    
    if not data:
        logging.warning(f"No data to save for {source}")
        return False
    
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        if table == 'auction_lots':
            return save_auction_lots(cursor, data, source)
        elif table == 'market_reports':
            return save_market_reports(cursor, data, source)
        elif table == 'news_articles':
            return save_news_articles(cursor, data, source)
        
        conn.commit()
        logging.info(f"✅ Saved {len(data)} records to {table}")
        return True
        
    except Exception as e:
        logging.error(f"❌ Database save failed: {e}")
        conn.rollback()
        return False
        
    finally:
        conn.close()

def generate_weekly_reports() -> bool:
    """Generate weekly analytics reports"""
    
    conn = get_db_connection()
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        # Generate weekly price analytics
        cursor.execute("""
            INSERT INTO weekly_price_analytics (
                source, auction_centre_id, tea_grade, week_start_date, 
                week_number, year, avg_price, min_price, max_price, 
                total_quantity, total_lots, unique_gardens, currency
            )
            SELECT 
                al.source,
                al.auction_centre_id,
                al.grade as tea_grade,
                DATE_TRUNC('week', al.auction_date) as week_start_date,
                EXTRACT(WEEK FROM al.auction_date) as week_number,
                EXTRACT(YEAR FROM al.auction_date) as year,
                ROUND(AVG(al.price_per_kg), 2) as avg_price,
                MIN(al.price_per_kg) as min_price,
                MAX(al.price_per_kg) as max_price,
                SUM(al.quantity_kg) as total_quantity,
                COUNT(*) as total_lots,
                COUNT(DISTINCT al.garden_id) as unique_gardens,
                al.currency
            FROM auction_lots al
            WHERE al.auction_date >= CURRENT_DATE - INTERVAL '7 days'
            AND NOT EXISTS (
                SELECT 1 FROM weekly_price_analytics wpa 
                WHERE wpa.source = al.source 
                AND wpa.auction_centre_id = al.auction_centre_id
                AND wpa.tea_grade = al.grade
                AND wpa.week_start_date = DATE_TRUNC('week', al.auction_date)
            )
            GROUP BY al.source, al.auction_centre_id, al.grade, 
                     DATE_TRUNC('week', al.auction_date), 
                     EXTRACT(WEEK FROM al.auction_date),
                     EXTRACT(YEAR FROM al.auction_date), al.currency
            HAVING COUNT(*) > 0
        """)
        
        conn.commit()
        logging.info("✅ Weekly analytics generated")
        return True
        
    except Exception as e:
        logging.error(f"❌ Weekly report generation failed: {e}")
        return False
        
    finally:
        conn.close()

# Utility functions
def safe_int(value: Any) -> Optional[int]:
    """Safely convert value to integer"""
    try:
        if value is None or value == '':
            return None
        return int(float(str(value).replace(',', '')))
    except:
        return None

def safe_float(value: Any) -> Optional[float]:
    """Safely convert value to float"""
    try:
        if value is None or value == '':
            return None
        return float(str(value).replace(',', ''))
    except:
        return None

def clean_text(text: str) -> str:
    """Clean and standardize text"""
    if not text:
        return ""
    return re.sub(r'\s+', ' ', str(text).strip())

# Data standardization functions for each source
def standardize_jthomas_data(data: Dict) -> Dict:
    """Standardize J Thomas auction data"""
    return {
        'auction_centre': clean_text(data.get('location', '')),
        'garden': clean_text(data.get('garden', '')),
        'grade': clean_text(data.get('grade', '')),
        'quantity_kg': safe_int(data.get('quantity')),
        'price_per_kg': safe_float(data.get('price')),
        'auction_date': data.get('auction_date'),
        'sale_number': safe_int(data.get('sale_no')),
        'lot_number': safe_int(data.get('lot_no')),
        'currency': 'INR'
    }

def standardize_ceylon_data(data: Dict) -> Dict:
    """Standardize Ceylon Tea Brokers data"""
    return {
        'auction_centre': 'Colombo',
        'garden': clean_text(data.get('estate', '')),
        'grade': clean_text(data.get('grade', '')),
        'quantity_kg': safe_int(data.get('quantity')),
        'price_per_kg': safe_float(data.get('price')),
        'auction_date': data.get('sale_date'),
        'sale_number': safe_int(data.get('sale_no')),
        'lot_number': safe_int(data.get('lot_no')),
        'currency': 'LKR'
    }

def standardize_forbes_data(data: Dict) -> Dict:
    """Standardize Forbes Tea data"""
    return {
        'auction_centre': 'Colombo',
        'garden': clean_text(data.get('garden', '')),
        'grade': clean_text(data.get('grade', '')),
        'quantity_kg': safe_int(data.get('quantity')),
        'price_per_kg': safe_float(data.get('price')),
        'auction_date': data.get('auction_date'),
        'currency': 'LKR'
    }

def standardize_tbea_data(data: Dict) -> Dict:
    """Standardize TBEA Kenya data"""
    return {
        'auction_centre': 'Mombasa',
        'garden': clean_text(data.get('garden', '')),
        'grade': clean_text(data.get('grade', '')),
        'quantity_kg': safe_int(data.get('quantity')),
        'price_per_kg': safe_float(data.get('price')),
        'auction_date': data.get('auction_date'),
        'currency': 'USD'
    }

def standardize_atb_data(data: Dict) -> Dict:
    """Standardize ATB Kenya data"""
    return {
        'auction_centre': 'Nairobi',
        'garden': clean_text(data.get('garden', '')),
        'grade': clean_text(data.get('grade', '')),
        'quantity_kg': safe_int(data.get('quantity')),
        'price_per_kg': safe_float(data.get('price')),
        'auction_date': data.get('auction_date'),
        'currency': 'USD'
    }

def standardize_news_data(data: Dict) -> Dict:
    """Standardize news article data"""
    return {
        'title': clean_text(data.get('title', '')),
        'content': clean_text(data.get('content', '')),
        'source': clean_text(data.get('source', '')),
        'url': data.get('url', ''),
        'publish_date': data.get('publish_date'),
        'image_url': data.get('image_url', ''),
        'categories': data.get('categories', [])
    }

# Database helper functions
def save_auction_lots(cursor, data: List[Dict], source: str) -> bool:
    """Save auction lots to database"""
    
    for record in data:
        try:
            # Get or create auction centre
            cursor.execute("""
                SELECT id FROM auction_centres 
                WHERE name = %s
            """, (record['auction_centre'],))
            
            centre_result = cursor.fetchone()
            if not centre_result:
                continue
            
            centre_id = centre_result[0]
            
            # Get or create garden
            cursor.execute("""
                INSERT INTO gardens (name, country) 
                VALUES (%s, %s) 
                ON CONFLICT (name, country) DO NOTHING
                RETURNING id
            """, (record['garden'], 'India' if source == 'J_THOMAS' else 'Sri Lanka'))
            
            garden_result = cursor.fetchone()
            if not garden_result:
                cursor.execute("""
                    SELECT id FROM gardens 
                    WHERE name = %s
                """, (record['garden'],))
                garden_result = cursor.fetchone()
            
            if not garden_result:
                continue
            
            garden_id = garden_result[0]
            
            # Insert auction lot
            cursor.execute("""
                INSERT INTO auction_lots (
                    source, auction_centre_id, garden_id, sale_number, 
                    lot_number, grade, quantity_kg, price_per_kg, 
                    currency, auction_date, raw_data
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (source, auction_centre_id, sale_number, lot_number, auction_date) 
                DO NOTHING
            """, (
                source, centre_id, garden_id, record.get('sale_number'),
                record.get('lot_number'), record['grade'], record['quantity_kg'],
                record['price_per_kg'], record['currency'], record['auction_date'],
                json.dumps(record)
            ))
            
        except Exception as e:
            logging.error(f"Error saving auction lot: {e}")
            continue
    
    return True

def save_market_reports(cursor, data: List[Dict], source: str) -> bool:
    """Save market reports to database"""
    
    for record in data:
        try:
            cursor.execute("""
                INSERT INTO market_reports (
                    source, title, content, report_date, week_number, 
                    year, key_metrics, raw_data
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
            """, (
                source, record['title'], record.get('content'),
                record.get('report_date'), record.get('week_number'),
                record.get('year'), json.dumps(record.get('metrics', {})),
                json.dumps(record)
            ))
            
        except Exception as e:
            logging.error(f"Error saving market report: {e}")
            continue
    
    return True

def save_news_articles(cursor, data: List[Dict], source: str) -> bool:
    """Save news articles to database"""
    
    for record in data:
        try:
            cursor.execute("""
                INSERT INTO news_articles (
                    title, content, source, url, publish_date, 
                    image_url, categories
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (title, source, publish_date) DO NOTHING
            """, (
                record['title'], record.get('content'), record['source'],
                record.get('url'), record.get('publish_date'),
                record.get('image_url'), json.dumps(record.get('categories', []))
            ))
            
        except Exception as e:
            logging.error(f"Error saving news article: {e}")
            continue
    
    return True
