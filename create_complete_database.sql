-- Complete Tea Trade Data Warehouse Schema
-- Optimized for enterprise-scale tea market analysis

-- Auction Centres
CREATE TABLE auction_centres (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    city VARCHAR(100),
    country VARCHAR(50),
    timezone VARCHAR(50),
    active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(name, country)
);

-- Gardens/Estates
CREATE TABLE gardens (
    id SERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    region VARCHAR(100),
    country VARCHAR(50),
    elevation_m INTEGER,
    area_hectares DECIMAL(10,2),
    established_year INTEGER,
    tea_type VARCHAR(50), -- 'Black', 'Green', 'White', 'Oolong'
    organic BOOLEAN DEFAULT false,
    active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(name, country)
);

-- Complete Auction Lots (All Sources)
CREATE TABLE auction_lots (
    id SERIAL PRIMARY KEY,
    source VARCHAR(50) NOT NULL, -- 'J_THOMAS', 'CEYLON', 'FORBES', 'TBEA', 'ATB'
    auction_centre_id INTEGER REFERENCES auction_centres(id),
    garden_id INTEGER REFERENCES gardens(id),
    sale_number INTEGER NOT NULL,
    lot_number INTEGER NOT NULL,
    grade VARCHAR(50),
    quantity_kg INTEGER,
    price_per_kg DECIMAL(10,2),
    currency VARCHAR(10),
    auction_date DATE,
    sale_type VARCHAR(50), -- 'Normal', 'CTC', 'Orthodox'
    quality_score INTEGER, -- 1-10 scale
    buyer VARCHAR(200),
    seller VARCHAR(200),
    scrape_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    raw_data JSONB, -- Store original scraped data
    UNIQUE(source, auction_centre_id, sale_number, lot_number, auction_date)
);

-- Weekly Price Analytics
CREATE TABLE weekly_price_analytics (
    id SERIAL PRIMARY KEY,
    source VARCHAR(50) NOT NULL,
    auction_centre_id INTEGER REFERENCES auction_centres(id),
    tea_grade VARCHAR(50),
    week_start_date DATE,
    week_number INTEGER,
    year INTEGER,
    avg_price DECIMAL(10,2),
    min_price DECIMAL(10,2),
    max_price DECIMAL(10,2),
    total_quantity INTEGER,
    total_lots INTEGER,
    unique_gardens INTEGER,
    currency VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source, auction_centre_id, tea_grade, week_start_date)
);

-- Market Reports (TBEA, ATB, Analysis)
CREATE TABLE market_reports (
    id SERIAL PRIMARY KEY,
    source VARCHAR(50) NOT NULL, -- 'TBEA', 'ATB', 'ANALYSIS'
    title VARCHAR(300) NOT NULL,
    report_type VARCHAR(100), -- 'Weekly', 'Monthly', 'Annual', 'Special'
    country VARCHAR(50),
    region VARCHAR(100),
    auction_centre_id INTEGER REFERENCES auction_centres(id),
    report_date DATE,
    week_number INTEGER,
    year INTEGER,
    content TEXT,
    summary TEXT,
    key_metrics JSONB, -- Store structured data like prices, volumes
    insights JSONB, -- Store market insights and trends
    file_path VARCHAR(500),
    scrape_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed BOOLEAN DEFAULT false,
    published BOOLEAN DEFAULT false
);

-- News Articles
CREATE TABLE news_articles (
    id SERIAL PRIMARY KEY,
    title VARCHAR(500) NOT NULL,
    content TEXT,
    summary TEXT,
    source VARCHAR(200),
    author VARCHAR(200),
    url VARCHAR(1000),
    image_url VARCHAR(1000),
    publish_date TIMESTAMP,
    categories JSONB, -- Store categories like ['market', 'prices', 'industry']
    sentiment_score DECIMAL(3,2), -- -1 to 1 sentiment analysis
    relevance_score DECIMAL(3,2), -- 0 to 1 relevance to tea industry
    scrape_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(title, source, publish_date)
);

-- System Monitoring
CREATE TABLE system_health (
    id SERIAL PRIMARY KEY,
    component VARCHAR(100) NOT NULL, -- 'scraper', 'database', 'api'
    status VARCHAR(20) NOT NULL, -- 'healthy', 'warning', 'error'
    message TEXT,
    metrics JSONB, -- Performance metrics
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Data Quality Tracking
CREATE TABLE data_quality_log (
    id SERIAL PRIMARY KEY,
    source VARCHAR(50) NOT NULL,
    check_type VARCHAR(100), -- 'completeness', 'accuracy', 'freshness'
    quality_score DECIMAL(5,2), -- 0-100 quality score
    issues_found INTEGER,
    details JSONB,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert initial auction centres
INSERT INTO auction_centres (name, city, country, timezone) VALUES
('Kolkata', 'Kolkata', 'India', 'Asia/Kolkata'),
('Guwahati', 'Guwahati', 'India', 'Asia/Kolkata'),
('Siliguri', 'Siliguri', 'India', 'Asia/Kolkata'),
('Coimbatore', 'Coimbatore', 'India', 'Asia/Kolkata'),
('Kochi', 'Kochi', 'India', 'Asia/Kolkata'),
('Cochin', 'Cochin', 'India', 'Asia/Kolkata'),
('Colombo', 'Colombo', 'Sri Lanka', 'Asia/Colombo'),
('Mombasa', 'Mombasa', 'Kenya', 'Africa/Nairobi'),
('Nairobi', 'Nairobi', 'Kenya', 'Africa/Nairobi');

-- Create analytics views
CREATE VIEW v_weekly_market_summary AS
SELECT 
    ac.name as auction_centre,
    ac.country,
    wp.week_start_date,
    wp.week_number,
    wp.year,
    SUM(wp.total_quantity) as total_quantity_kg,
    SUM(wp.total_lots) as total_lots,
    AVG(wp.avg_price) as avg_price,
    MIN(wp.min_price) as min_price,
    MAX(wp.max_price) as max_price,
    COUNT(DISTINCT wp.tea_grade) as unique_grades,
    SUM(wp.unique_gardens) as total_gardens
FROM weekly_price_analytics wp
JOIN auction_centres ac ON wp.auction_centre_id = ac.id
GROUP BY ac.name, ac.country, wp.week_start_date, wp.week_number, wp.year
ORDER BY wp.year DESC, wp.week_number DESC;

-- Create indexes for performance
CREATE INDEX idx_auction_lots_date ON auction_lots(auction_date);
CREATE INDEX idx_auction_lots_source ON auction_lots(source);
CREATE INDEX idx_auction_lots_centre ON auction_lots(auction_centre_id);
CREATE INDEX idx_weekly_analytics_date ON weekly_price_analytics(week_start_date);
CREATE INDEX idx_market_reports_date ON market_reports(report_date);
CREATE INDEX idx_news_publish_date ON news_articles(publish_date);
