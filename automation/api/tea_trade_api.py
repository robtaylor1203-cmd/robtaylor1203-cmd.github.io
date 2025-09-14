#!/usr/bin/env python3
"""
Tea Trade API - Complete Working Version
RESTful API for tea market data access
"""

import json
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import logging
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import psycopg2
from pydantic import BaseModel

# Initialize API
app = FastAPI(
    title="Tea Trade API",
    description="Complete API for global tea market data",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# Enable CORS for your website
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify your domain
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('TEA_API')

# Database connection function
def get_db_connection():
    """Get database connection"""
    try:
        return psycopg2.connect(
            host='localhost',
            database='tea_trade_data',
            user='tea_admin',
            password='secure_password_123'
        )
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        return None

# Pydantic models for API responses
class AuctionLot(BaseModel):
    id: int
    source: str
    auction_centre: str
    garden: str
    grade: str
    quantity_kg: int
    price_per_kg: float
    currency: str
    auction_date: str
    scrape_timestamp: str

class NewsArticle(BaseModel):
    id: int
    title: str
    content: Optional[str]
    source: str
    url: Optional[str]
    publish_date: str

class MarketSummary(BaseModel):
    total_auctions: int
    total_news: int
    active_gardens: int
    last_updated: str
    sources: List[Dict[str, Any]]
    system_status: str

# Health check endpoint
@app.get("/health")
async def health_check():
    """API health check"""
    try:
        conn = get_db_connection()
        if conn:
            conn.close()
            return {"status": "healthy", "timestamp": datetime.now().isoformat()}
        else:
            raise HTTPException(status_code=503, detail="Database unavailable")
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Health check failed: {str(e)}")

# Market summary endpoint
@app.get("/api/v1/market-summary", response_model=MarketSummary)
async def get_market_summary():
    """Get comprehensive market summary"""
    try:
        conn = get_db_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        cursor = conn.cursor()
        
        # Get auction statistics
        cursor.execute("""
            SELECT 
                COUNT(*) as total_auctions,
                COUNT(DISTINCT garden_id) as active_gardens,
                MAX(scrape_timestamp) as last_updated
            FROM auction_lots
            WHERE scrape_timestamp >= CURRENT_DATE - INTERVAL '7 days'
        """)
        auction_stats = cursor.fetchone()
        
        # Get news statistics
        cursor.execute("""
            SELECT COUNT(*) 
            FROM news_articles
            WHERE scrape_timestamp >= CURRENT_DATE - INTERVAL '7 days'
        """)
        news_count = cursor.fetchone()[0]
        
        # Get sources breakdown
        cursor.execute("""
            SELECT 
                source,
                COUNT(*) as lots,
                AVG(price_per_kg) as avg_price,
                MAX(scrape_timestamp) as last_updated
            FROM auction_lots
            WHERE scrape_timestamp >= CURRENT_DATE - INTERVAL '7 days'
            GROUP BY source
            ORDER BY lots DESC
        """)
        sources = []
        for row in cursor.fetchall():
            sources.append({
                'source': row[0],
                'lots': row[1],
                'avg_price': float(row[2]) if row[2] else 0,
                'last_updated': row[3].isoformat() if row[3] else None
            })
        
        conn.close()
        
        return MarketSummary(
            total_auctions=auction_stats[0] or 0,
            total_news=news_count,
            active_gardens=auction_stats[1] or 0,
            last_updated=auction_stats[2].isoformat() if auction_stats[2] else datetime.now().isoformat(),
            sources=sources,
            system_status="operational"
        )
        
    except Exception as e:
        logger.error(f"Market summary error: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve market summary")

# Auction lots endpoint with filtering
@app.get("/api/v1/auction-lots", response_model=List[AuctionLot])
async def get_auction_lots(
    source: Optional[str] = Query(None, description="Filter by source"),
    auction_centre: Optional[str] = Query(None, description="Filter by auction centre"),
    limit: int = Query(100, le=1000, description="Maximum results")
):
    """Get auction lots with filtering options"""
    try:
        conn = get_db_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        cursor = conn.cursor()
        
        # Build dynamic query
        query = """
            SELECT 
                al.id, al.source, COALESCE(ac.name, 'Unknown') as auction_centre, 
                COALESCE(g.name, 'Unknown Garden') as garden,
                al.grade, al.quantity_kg, al.price_per_kg, al.currency,
                al.auction_date, al.scrape_timestamp
            FROM auction_lots al
            LEFT JOIN auction_centres ac ON al.auction_centre_id = ac.id
            LEFT JOIN gardens g ON al.garden_id = g.id
            WHERE 1=1
        """
        params = []
        
        if source:
            query += " AND al.source = %s"
            params.append(source)
        
        if auction_centre:
            query += " AND ac.name ILIKE %s"
            params.append(f"%{auction_centre}%")
        
        query += " ORDER BY al.auction_date DESC, al.scrape_timestamp DESC LIMIT %s"
        params.append(limit)
        
        cursor.execute(query, params)
        
        lots = []
        for row in cursor.fetchall():
            lots.append(AuctionLot(
                id=row[0],
                source=row[1],
                auction_centre=row[2],
                garden=row[3],
                grade=row[4] or 'Mixed',
                quantity_kg=row[5] or 0,
                price_per_kg=float(row[6]) if row[6] else 0.0,
                currency=row[7] or 'INR',
                auction_date=row[8].isoformat() if row[8] else datetime.now().date().isoformat(),
                scrape_timestamp=row[9].isoformat() if row[9] else datetime.now().isoformat()
            ))
        
        conn.close()
        return lots
        
    except Exception as e:
        logger.error(f"Auction lots error: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve auction lots")

# News articles endpoint
@app.get("/api/v1/news", response_model=List[NewsArticle])
async def get_news_articles(
    source: Optional[str] = Query(None, description="Filter by news source"),
    limit: int = Query(50, le=200, description="Maximum results")
):
    """Get tea industry news articles"""
    try:
        conn = get_db_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        cursor = conn.cursor()
        
        query = """
            SELECT 
                id, title, content, source, url, publish_date
            FROM news_articles
            WHERE 1=1
        """
        params = []
        
        if source:
            query += " AND source ILIKE %s"
            params.append(f"%{source}%")
        
        query += " ORDER BY publish_date DESC LIMIT %s"
        params.append(limit)
        
        cursor.execute(query, params)
        
        articles = []
        for row in cursor.fetchall():
            articles.append(NewsArticle(
                id=row[0],
                title=row[1] or 'Untitled',
                content=row[2][:500] if row[2] else None,  # Limit content length
                source=row[3] or 'Unknown',
                url=row[4],
                publish_date=row[5].isoformat() if row[5] else datetime.now().isoformat()
            ))
        
        conn.close()
        return articles
        
    except Exception as e:
        logger.error(f"News articles error: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve news articles")

# Price trends endpoint
@app.get("/api/v1/price-trends")
async def get_price_trends(
    days: int = Query(30, le=365, description="Number of days"),
    source: Optional[str] = Query(None, description="Filter by source")
):
    """Get price trends over time"""
    try:
        conn = get_db_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        cursor = conn.cursor()
        
        query = """
            SELECT 
                DATE(auction_date) as date,
                AVG(price_per_kg) as avg_price,
                MIN(price_per_kg) as min_price,
                MAX(price_per_kg) as max_price,
                COUNT(*) as lots_count,
                SUM(quantity_kg) as total_quantity
            FROM auction_lots
            WHERE auction_date >= CURRENT_DATE - INTERVAL '%s days'
            AND price_per_kg > 0
        """
        params = [days]
        
        if source:
            query += " AND source = %s"
            params.append(source)
        
        query += """
            GROUP BY DATE(auction_date)
            ORDER BY DATE(auction_date)
        """
        
        cursor.execute(query, params)
        
        trends = []
        for row in cursor.fetchall():
            trends.append({
                'date': row[0].isoformat(),
                'avg_price': float(row[1]) if row[1] else 0,
                'min_price': float(row[2]) if row[2] else 0,
                'max_price': float(row[3]) if row[3] else 0,
                'lots_count': row[4],
                'total_quantity': row[5] or 0
            })
        
        conn.close()
        return {"trends": trends, "period_days": days}
        
    except Exception as e:
        logger.error(f"Price trends error: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve price trends")

# Export data endpoint for website
@app.get("/api/v1/export/website-data")
async def export_website_data():
    """Export optimized data for website consumption"""
    try:
        conn = get_db_connection()
        if not conn:
            raise HTTPException(status_code=500, detail="Database connection failed")
        
        cursor = conn.cursor()
        
        # Get latest auction data for charts
        cursor.execute("""
            SELECT 
                COALESCE(ac.name, al.source) as auction_centre,
                DATE(al.auction_date) as date,
                AVG(al.price_per_kg) as avg_price,
                SUM(al.quantity_kg) as total_quantity,
                COUNT(*) as lots_count
            FROM auction_lots al
            LEFT JOIN auction_centres ac ON al.auction_centre_id = ac.id
            WHERE al.auction_date >= CURRENT_DATE - INTERVAL '30 days'
            AND al.price_per_kg > 0
            GROUP BY COALESCE(ac.name, al.source), DATE(al.auction_date)
            ORDER BY DATE(al.auction_date) DESC
            LIMIT 200
        """)
        
        chart_data = []
        for row in cursor.fetchall():
            chart_data.append({
                'auction_centre': row[0],
                'date': row[1].isoformat(),
                'avg_price': float(row[2]),
                'total_quantity': row[3] or 0,
                'lots_count': row[4]
            })
        
        # Get recent news
        cursor.execute("""
            SELECT title, source, url, publish_date
            FROM news_articles
            WHERE publish_date >= CURRENT_DATE - INTERVAL '7 days'
            ORDER BY publish_date DESC
            LIMIT 10
        """)
        
        recent_news = []
        for row in cursor.fetchall():
            recent_news.append({
                'title': row[0],
                'source': row[1],
                'url': row[2],
                'publish_date': row[3].isoformat() if row[3] else None
            })
        
        conn.close()
        
        return {
            'export_timestamp': datetime.now().isoformat(),
            'chart_data': chart_data,
            'recent_news': recent_news,
            'data_freshness': 'last_30_days'
        }
        
    except Exception as e:
        logger.error(f"Website export error: {e}")
        raise HTTPException(status_code=500, detail="Failed to export website data")

# Run API server
def start_api_server(host: str = "0.0.0.0", port: int = 8000):
    """Start the API server"""
    logger.info(f"Starting Tea Trade API server on {host}:{port}")
    uvicorn.run(app, host=host, port=port, log_level="info")

if __name__ == "__main__":
    start_api_server()
