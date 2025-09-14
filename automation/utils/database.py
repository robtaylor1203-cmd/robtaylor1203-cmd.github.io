#!/usr/bin/env python3
"""
Database utilities for PostgreSQL data warehouse
"""

import os
import psycopg2
import psycopg2.extras
from psycopg2.pool import SimpleConnectionPool
import json
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime
import pandas as pd

logger = logging.getLogger(__name__)

class TeaTradeDatabase:
    """PostgreSQL database manager for tea trade data"""
    
    def __init__(self):
        # Check if running in GitHub Actions (use PostgreSQL service)
        if os.getenv('GITHUB_ACTIONS'):
            self.db_config = {
                'host': 'localhost',
                'port': 5432,
                'user': 'tea_admin',
                'password': 'secure_password_123',
                'database': 'tea_trade_data'
            }
        else:
            # Local development configuration
            self.db_config = {
                'host': os.getenv('DB_HOST', 'localhost'),
                'port': int(os.getenv('DB_PORT', 5432)),
                'user': os.getenv('DB_USER', 'tea_admin'),
                'password': os.getenv('DB_PASSWORD', 'secure_password_123'),
                'database': os.getenv('DB_NAME', 'tea_trade_data')
            }
        
        self.connection_pool = None
        self._initialize_pool()
    
    def _initialize_pool(self):
        """Initialize connection pool"""
        try:
            self.connection_pool = SimpleConnectionPool(
                minconn=1,
                maxconn=10,
                **self.db_config
            )
            logger.info("Database connection pool initialized")
        except Exception as e:
            logger.error(f"Failed to initialize database pool: {e}")
            self.connection_pool = None
    
    def get_connection(self):
        """Get database connection from pool"""
        if self.connection_pool:
            return self.connection_pool.getconn()
        else:
            # Fallback to direct connection
            try:
                return psycopg2.connect(**self.db_config)
            except Exception as e:
                logger.error(f"Failed to get database connection: {e}")
                return None
    
    def return_connection(self, conn):
        """Return connection to pool"""
        if self.connection_pool:
            self.connection_pool.putconn(conn)
        else:
            conn.close()
    
    def execute_query(self, query: str, params: tuple = None) -> List[Dict]:
        """Execute query and return results"""
        conn = self.get_connection()
        if not conn:
            return []
        
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(query, params)
                if cursor.description:
                    return [dict(row) for row in cursor.fetchall()]
                return []
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            return []
        finally:
            self.return_connection(conn)
    
    def insert_auction_lot(self, lot_data: Dict) -> bool:
        """Insert auction lot data"""
        conn = self.get_connection()
        if not conn:
            return False
        
        try:
            with conn.cursor() as cursor:
                # Get centre_id
                cursor.execute(
                    "SELECT id FROM auction_centres WHERE centre_name = %s",
                    (lot_data.get('location', 'Unknown'),)
                )
                centre_result = cursor.fetchone()
                centre_id = centre_result[0] if centre_result else None
                
                # Insert auction lot
                cursor.execute("""
                    INSERT INTO auction_lots 
                    (lot_no, centre_id, garden_name, grade, quantity, price, price_usd, 
                     currency, auction_date, broker, warehouse, quality_notes, source_file)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (lot_no, centre_id, auction_date) DO UPDATE SET
                        garden_name = EXCLUDED.garden_name,
                        grade = EXCLUDED.grade,
                        quantity = EXCLUDED.quantity,
                        price = EXCLUDED.price,
                        price_usd = EXCLUDED.price_usd,
                        broker = EXCLUDED.broker,
                        warehouse = EXCLUDED.warehouse,
                        quality_notes = EXCLUDED.quality_notes,
                        source_file = EXCLUDED.source_file
                """, (
                    lot_data.get('lot_no'),
                    centre_id,
                    lot_data.get('garden_name'),
                    lot_data.get('grade'),
                    lot_data.get('quantity'),
                    lot_data.get('price'),
                    lot_data.get('price_usd'),
                    lot_data.get('currency'),
                    lot_data.get('auction_date'),
                    lot_data.get('broker'),
                    lot_data.get('warehouse'),
                    lot_data.get('quality_notes'),
                    lot_data.get('source_file')
                ))
            
            conn.commit()
            return True
            
        except Exception as e:
            logger.error(f"Failed to insert auction lot: {e}")
            conn.rollback()
            return False
        finally:
            self.return_connection(conn)
    
    def insert_news_article(self, article_data: Dict) -> bool:
        """Insert news article data"""
        conn = self.get_connection()
        if not conn:
            return False
        
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO news_articles 
                    (title, source, url, summary, content, publish_date, category, importance, region)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (title, source) DO UPDATE SET
                        url = EXCLUDED.url,
                        summary = EXCLUDED.summary,
                        content = EXCLUDED.content,
                        publish_date = EXCLUDED.publish_date,
                        category = EXCLUDED.category,
                        importance = EXCLUDED.importance,
                        region = EXCLUDED.region
                """, (
                    article_data.get('title'),
                    article_data.get('source'),
                    article_data.get('url'),
                    article_data.get('summary'),
                    article_data.get('content'),
                    article_data.get('publish_date'),
                    article_data.get('category'),
                    article_data.get('importance'),
                    article_data.get('region')
                ))
            
            conn.commit()
            return True
            
        except Exception as e:
            logger.error(f"Failed to insert news article: {e}")
            conn.rollback()
            return False
        finally:
            self.return_connection(conn)
    
    def get_daily_summary(self, days: int = 7) -> Dict:
        """Get daily market summary"""
        query = """
        SELECT * FROM daily_market_summary 
        WHERE auction_date >= CURRENT_DATE - INTERVAL '%s days'
        ORDER BY auction_date DESC, centre_name
        """
        
        results = self.execute_query(query, (days,))
        
        return {
            'summary_data': results,
            'total_centres': len(set(r['centre_name'] for r in results)),
            'date_range': f"Last {days} days",
            'generated_at': datetime.now().isoformat()
        }
    
    def get_price_trends(self, centre: str = None, grade: str = None) -> List[Dict]:
        """Get price trends with optional filtering"""
        query = "SELECT * FROM price_trends_monthly WHERE 1=1"
        params = []
        
        if centre:
            query += " AND centre_name = %s"
            params.append(centre)
        
        if grade:
            query += " AND grade = %s"
            params.append(grade)
        
        query += " ORDER BY week_start DESC"
        
        return self.execute_query(query, tuple(params))
    
    def get_top_gardens(self, limit: int = 20) -> List[Dict]:
        """Get top performing gardens"""
        query = "SELECT * FROM top_gardens_weekly ORDER BY avg_price_usd DESC LIMIT %s"
        return self.execute_query(query, (limit,))
    
    def store_data_quality_metrics(self, metrics: Dict) -> bool:
        """Store data quality metrics"""
        conn = self.get_connection()
        if not conn:
            return False
        
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO data_quality_metrics 
                    (metric_date, total_records, completeness_score, accuracy_score, 
                     freshness_hours, validation_errors, scraper_success_rate)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (
                    datetime.now().date(),
                    metrics.get('total_records'),
                    metrics.get('completeness_score'),
                    metrics.get('accuracy_score'),
                    metrics.get('freshness_hours'),
                    json.dumps(metrics.get('validation_errors', [])),
                    metrics.get('scraper_success_rate', 0)
                ))
            
            conn.commit()
            return True
            
        except Exception as e:
            logger.error(f"Failed to store quality metrics: {e}")
            conn.rollback()
            return False
        finally:
            self.return_connection(conn)

# Global database instance
db = TeaTradeDatabase()
