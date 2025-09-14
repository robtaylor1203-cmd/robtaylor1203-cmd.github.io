#!/usr/bin/env python3
"""
Tea Trade RESTful API
Provides data access endpoints for the web application
"""

from flask import Flask, jsonify, request, render_template_string
from flask_cors import CORS
import logging
from datetime import datetime, timedelta
import sys
from pathlib import Path

# Add utils to path
sys.path.append(str(Path(__file__).parent.parent))
from utils.database import db

app = Flask(__name__)
CORS(app)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@app.route('/api/health')
def health_check():
    """API health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now().isoformat(),
        'version': '2.0.0',
        'database': 'connected' if db.get_connection() else 'disconnected'
    })

@app.route('/api/summary')
def get_summary():
    """Get market summary data"""
    days = request.args.get('days', 7, type=int)
    summary = db.get_daily_summary(days)
    
    return jsonify({
        'status': 'success',
        'data': summary,
        'meta': {
            'endpoint': 'summary',
            'days_requested': days,
            'generated_at': datetime.now().isoformat()
        }
    })

@app.route('/api/auctions')
def get_auctions():
    """Get auction data with filtering"""
    centre = request.args.get('centre')
    grade = request.args.get('grade')
    days = request.args.get('days', 7, type=int)
    limit = request.args.get('limit', 100, type=int)
    
    query = """
    SELECT al.*, ac.centre_name, ac.country 
    FROM auction_lots al 
    JOIN auction_centres ac ON al.centre_id = ac.id 
    WHERE al.auction_date >= CURRENT_DATE - INTERVAL '%s days'
    """
    params = [days]
    
    if centre:
        query += " AND ac.centre_name = %s"
        params.append(centre)
    
    if grade:
        query += " AND al.grade = %s"
        params.append(grade)
    
    query += " ORDER BY al.auction_date DESC, al.price_usd DESC LIMIT %s"
    params.append(limit)
    
    auctions = db.execute_query(query, tuple(params))
    
    return jsonify({
        'status': 'success',
        'data': auctions,
        'meta': {
            'total_records': len(auctions),
            'filters': {
                'centre': centre,
                'grade': grade,
                'days': days
            },
            'generated_at': datetime.now().isoformat()
        }
    })

@app.route('/api/news')
def get_news():
    """Get news articles"""
    category = request.args.get('category')
    days = request.args.get('days', 7, type=int)
    limit = request.args.get('limit', 50, type=int)
    
    query = """
    SELECT * FROM news_articles 
    WHERE publish_date >= CURRENT_DATE - INTERVAL '%s days'
    """
    params = [days]
    
    if category:
        query += " AND category = %s"
        params.append(category)
    
    query += " ORDER BY publish_date DESC LIMIT %s"
    params.append(limit)
    
    news = db.execute_query(query, tuple(params))
    
    return jsonify({
        'status': 'success',
        'data': news,
        'meta': {
            'total_articles': len(news),
            'category_filter': category,
            'days': days,
            'generated_at': datetime.now().isoformat()
        }
    })

@app.route('/api/trends')
def get_trends():
    """Get price trends analysis"""
    centre = request.args.get('centre')
    grade = request.args.get('grade')
    
    trends = db.get_price_trends(centre, grade)
    
    return jsonify({
        'status': 'success',
        'data': trends,
        'meta': {
            'filters': {
                'centre': centre,
                'grade': grade
            },
            'generated_at': datetime.now().isoformat()
        }
    })

@app.route('/api/gardens/top')
def get_top_gardens():
    """Get top performing gardens"""
    limit = request.args.get('limit', 20, type=int)
    gardens = db.get_top_gardens(limit)
    
    return jsonify({
        'status': 'success',
        'data': gardens,
        'meta': {
            'limit': limit,
            'generated_at': datetime.now().isoformat()
        }
    })

@app.route('/api/centres')
def get_centres():
    """Get all auction centres"""
    centres = db.execute_query("SELECT * FROM auction_centres ORDER BY centre_name")
    
    return jsonify({
        'status': 'success',
        'data': centres,
        'meta': {
            'total_centres': len(centres),
            'generated_at': datetime.now().isoformat()
        }
    })

@app.route('/api/analytics/dashboard')
def get_dashboard_data():
    """Get comprehensive dashboard data"""
    # Get summary stats
    summary_stats = db.execute_query("""
        SELECT 
            COUNT(DISTINCT al.centre_id) as active_centres,
            COUNT(*) as total_lots_week,
            SUM(al.quantity) as total_quantity_week,
            AVG(al.price_usd) as avg_price_usd,
            MAX(al.auction_date) as latest_auction
        FROM auction_lots al 
        WHERE al.auction_date >= CURRENT_DATE - INTERVAL '7 days'
    """)
    
    # Get top centres by volume
    top_centres = db.execute_query("""
        SELECT 
            ac.centre_name,
            ac.country,
            COUNT(*) as lots_count,
            SUM(al.quantity) as total_quantity,
            AVG(al.price_usd) as avg_price_usd
        FROM auction_lots al
        JOIN auction_centres ac ON al.centre_id = ac.id
        WHERE al.auction_date >= CURRENT_DATE - INTERVAL '7 days'
        GROUP BY ac.centre_name, ac.country
        ORDER BY total_quantity DESC
        LIMIT 10
    """)
    
    # Get recent news
    recent_news = db.execute_query("""
        SELECT title, source, category, publish_date
        FROM news_articles 
        WHERE publish_date >= CURRENT_DATE - INTERVAL '3 days'
        ORDER BY publish_date DESC 
        LIMIT 10
    """)
    
    return jsonify({
        'status': 'success',
        'data': {
            'summary_stats': summary_stats[0] if summary_stats else {},
            'top_centres': top_centres,
            'recent_news': recent_news,
            'last_updated': datetime.now().isoformat()
        }
    })

@app.route('/')
def api_documentation():
    """Simple API documentation page"""
    doc_html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>TeaTrade API Documentation</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 40px; }
            .endpoint { background: #f5f5f5; padding: 15px; margin: 10px 0; border-radius: 5px; }
            .method { color: #007bff; font-weight: bold; }
            .url { color: #28a745; font-family: monospace; }
        </style>
    </head>
    <body>
        <h1>TeaTrade API Documentation</h1>
        <p><strong>Base URL:</strong> <code>http://localhost:5000</code></p>
        
        <div class="endpoint">
            <h3><span class="method">GET</span> <span class="url">/api/health</span></h3>
            <p>Check API health status</p>
        </div>
        
        <div class="endpoint">
            <h3><span class="method">GET</span> <span class="url">/api/summary</span></h3>
            <p>Get market summary data</p>
            <p><strong>Parameters:</strong> days (optional, default: 7)</p>
        </div>
        
        <div class="endpoint">
            <h3><span class="method">GET</span> <span class="url">/api/auctions</span></h3>
            <p>Get auction lot data</p>
            <p><strong>Parameters:</strong> centre, grade, days (default: 7), limit (default: 100)</p>
        </div>
        
        <div class="endpoint">
            <h3><span class="method">GET</span> <span class="url">/api/news</span></h3>
            <p>Get news articles</p>
            <p><strong>Parameters:</strong> category, days (default: 7), limit (default: 50)</p>
        </div>
        
        <div class="endpoint">
            <h3><span class="method">GET</span> <span class="url">/api/trends</span></h3>
            <p>Get price trends analysis</p>
            <p><strong>Parameters:</strong> centre, grade</p>
        </div>
        
        <div class="endpoint">
            <h3><span class="method">GET</span> <span class="url">/api/gardens/top</span></h3>
            <p>Get top performing gardens</p>
            <p><strong>Parameters:</strong> limit (default: 20)</p>
        </div>
        
        <div class="endpoint">
            <h3><span class="method">GET</span> <span class="url">/api/analytics/dashboard</span></h3>
            <p>Get comprehensive dashboard data</p>
        </div>
    </body>
    </html>
    """
    return render_template_string(doc_html)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
