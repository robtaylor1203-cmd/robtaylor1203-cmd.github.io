-- Tea Trade Data Warehouse Schema
-- Complete PostgreSQL setup for enterprise system

-- Enable extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- Auction Centers lookup table
CREATE TABLE IF NOT EXISTS auction_centres (
    id SERIAL PRIMARY KEY,
    centre_name VARCHAR(100) NOT NULL UNIQUE,
    country VARCHAR(50) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    timezone VARCHAR(50) DEFAULT 'UTC',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert standard auction centres
INSERT INTO auction_centres (centre_name, country, currency, timezone) VALUES
('Kolkata', 'India', 'INR', 'Asia/Kolkata'),
('Guwahati', 'India', 'INR', 'Asia/Kolkata'),
('Kochi', 'India', 'INR', 'Asia/Kolkata'),
('Colombo', 'Sri Lanka', 'LKR', 'Asia/Colombo'),
('Kandy', 'Sri Lanka', 'LKR', 'Asia/Colombo'),
('Mombasa', 'Kenya', 'KES', 'Africa/Nairobi'),
('Limuru', 'Kenya', 'KES', 'Africa/Nairobi')
ON CONFLICT (centre_name) DO NOTHING;

-- Auction lots table
CREATE TABLE IF NOT EXISTS auction_lots (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    lot_no VARCHAR(50) NOT NULL,
    centre_id INTEGER REFERENCES auction_centres(id),
    garden_name VARCHAR(200),
    grade VARCHAR(20),
    quantity INTEGER,
    price DECIMAL(10,2),
    price_usd DECIMAL(10,2),
    currency VARCHAR(3),
    auction_date DATE,
    broker VARCHAR(100),
    warehouse VARCHAR(100),
    quality_notes TEXT,
    source_file VARCHAR(200),
    scrape_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(lot_no, centre_id, auction_date)
);

-- News articles table
CREATE TABLE IF NOT EXISTS news_articles (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    title VARCHAR(500) NOT NULL,
    source VARCHAR(200),
    url TEXT,
    summary TEXT,
    content TEXT,
    publish_date TIMESTAMP,
    category VARCHAR(50),
    importance VARCHAR(20),
    region VARCHAR(100),
    scrape_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(title, source)
);

-- Market reports table
CREATE TABLE IF NOT EXISTS market_reports (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    report_date DATE NOT NULL,
    centre_id INTEGER REFERENCES auction_centres(id),
    total_lots INTEGER,
    total_quantity INTEGER,
    avg_price DECIMAL(10,2),
    max_price DECIMAL(10,2),
    min_price DECIMAL(10,2),
    top_gardens JSONB,
    price_trends JSONB,
    summary TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(report_date, centre_id)
);

-- Data quality metrics table
CREATE TABLE IF NOT EXISTS data_quality_metrics (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    metric_date DATE NOT NULL,
    total_records INTEGER,
    completeness_score DECIMAL(5,2),
    accuracy_score DECIMAL(5,2),
    freshness_hours DECIMAL(8,2),
    validation_errors JSONB,
    scraper_success_rate DECIMAL(5,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_auction_lots_date ON auction_lots(auction_date DESC);
CREATE INDEX IF NOT EXISTS idx_auction_lots_centre ON auction_lots(centre_id);
CREATE INDEX IF NOT EXISTS idx_auction_lots_garden ON auction_lots(garden_name);
CREATE INDEX IF NOT EXISTS idx_auction_lots_grade ON auction_lots(grade);
CREATE INDEX IF NOT EXISTS idx_news_date ON news_articles(publish_date DESC);
CREATE INDEX IF NOT EXISTS idx_news_category ON news_articles(category);
CREATE INDEX IF NOT EXISTS idx_market_reports_date ON market_reports(report_date DESC);

-- Analytics views
CREATE OR REPLACE VIEW daily_market_summary AS
SELECT 
    al.auction_date,
    ac.centre_name,
    ac.country,
    COUNT(*) as total_lots,
    SUM(al.quantity) as total_quantity,
    AVG(al.price_usd) as avg_price_usd,
    MAX(al.price_usd) as max_price_usd,
    MIN(al.price_usd) as min_price_usd,
    STRING_AGG(DISTINCT al.grade, ', ') as grades_sold
FROM auction_lots al
JOIN auction_centres ac ON al.centre_id = ac.id
WHERE al.auction_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY al.auction_date, ac.centre_name, ac.country
ORDER BY al.auction_date DESC, ac.centre_name;

CREATE OR REPLACE VIEW top_gardens_weekly AS
SELECT 
    al.garden_name,
    ac.centre_name,
    COUNT(*) as lots_sold,
    SUM(al.quantity) as total_quantity,
    AVG(al.price_usd) as avg_price_usd,
    MAX(al.auction_date) as last_sale_date
FROM auction_lots al
JOIN auction_centres ac ON al.centre_id = ac.id
WHERE al.auction_date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY al.garden_name, ac.centre_name
HAVING COUNT(*) >= 2
ORDER BY avg_price_usd DESC, total_quantity DESC
LIMIT 50;

CREATE OR REPLACE VIEW price_trends_monthly AS
SELECT 
    DATE_TRUNC('week', al.auction_date) as week_start,
    ac.centre_name,
    al.grade,
    COUNT(*) as lots_count,
    AVG(al.price_usd) as avg_price_usd,
    STDDEV(al.price_usd) as price_volatility
FROM auction_lots al
JOIN auction_centres ac ON al.centre_id = ac.id
WHERE al.auction_date >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY DATE_TRUNC('week', al.auction_date), ac.centre_name, al.grade
HAVING COUNT(*) >= 3
ORDER BY week_start DESC, ac.centre_name, al.grade;

-- Stored procedures for data processing
CREATE OR REPLACE FUNCTION generate_daily_market_report(report_date DATE)
RETURNS VOID AS $$
BEGIN
    INSERT INTO market_reports (report_date, centre_id, total_lots, total_quantity, avg_price, max_price, min_price, top_gardens, price_trends)
    SELECT 
        report_date,
        ac.id,
        COUNT(al.*),
        SUM(al.quantity),
        AVG(al.price),
        MAX(al.price),
        MIN(al.price),
        jsonb_agg(
            jsonb_build_object(
                'garden', al.garden_name,
                'avg_price', al.price,
                'quantity', al.quantity
            ) ORDER BY al.price DESC
        ) FILTER (WHERE al.garden_name IS NOT NULL),
        jsonb_build_object(
            'avg_price_trend', AVG(al.price),
            'price_range', MAX(al.price) - MIN(al.price),
            'dominant_grades', STRING_AGG(DISTINCT al.grade, ', ')
        )
    FROM auction_centres ac
    LEFT JOIN auction_lots al ON ac.id = al.centre_id AND al.auction_date = report_date
    GROUP BY ac.id
    ON CONFLICT (report_date, centre_id) DO UPDATE SET
        total_lots = EXCLUDED.total_lots,
        total_quantity = EXCLUDED.total_quantity,
        avg_price = EXCLUDED.avg_price,
        max_price = EXCLUDED.max_price,
        min_price = EXCLUDED.min_price,
        top_gardens = EXCLUDED.top_gardens,
        price_trends = EXCLUDED.price_trends;
END;
$$ LANGUAGE plpgsql;
