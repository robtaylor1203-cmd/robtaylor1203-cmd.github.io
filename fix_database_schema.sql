-- Complete Tea Trade Database Schema
-- Drop existing tables if they exist to start fresh
DROP TABLE IF EXISTS auction_lots CASCADE;
DROP TABLE IF EXISTS auction_centres CASCADE;
DROP TABLE IF EXISTS gardens CASCADE;
DROP TABLE IF EXISTS weekly_price_analytics CASCADE;
DROP TABLE IF EXISTS market_reports CASCADE;
DROP TABLE IF EXISTS news_articles CASCADE;
DROP TABLE IF EXISTS system_health CASCADE;
DROP TABLE IF EXISTS data_quality_log CASCADE;

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
    tea_type VARCHAR(50),
    organic BOOLEAN DEFAULT false,
    active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(name, country)
);

-- Auction Lots (Main data table)
CREATE TABLE auction_lots (
    id SERIAL PRIMARY KEY,
    source VARCHAR(50) NOT NULL,
    auction_centre_id INTEGER REFERENCES auction_centres(id),
    garden_id INTEGER REFERENCES gardens(id),
    sale_number INTEGER,
    lot_number INTEGER,
    grade VARCHAR(50),
    quantity_kg INTEGER,
    price_per_kg DECIMAL(10,2),
    currency VARCHAR(10),
    auction_date DATE,
    sale_type VARCHAR(50),
    quality_score INTEGER,
    buyer VARCHAR(200),
    seller VARCHAR(200),
    scrape_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    raw_data JSONB
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
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Market Reports
CREATE TABLE market_reports (
    id SERIAL PRIMARY KEY,
    source VARCHAR(50) NOT NULL,
    title VARCHAR(300) NOT NULL,
    report_type VARCHAR(100),
    country VARCHAR(50),
    region VARCHAR(100),
    auction_centre_id INTEGER REFERENCES auction_centres(id),
    report_date DATE,
    week_number INTEGER,
    year INTEGER,
    content TEXT,
    summary TEXT,
    key_metrics JSONB,
    insights JSONB,
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
    categories JSONB,
    sentiment_score DECIMAL(3,2),
    relevance_score DECIMAL(3,2),
    scrape_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- System Health Monitoring
CREATE TABLE system_health (
    id SERIAL PRIMARY KEY,
    component VARCHAR(100) NOT NULL,
    status VARCHAR(20) NOT NULL,
    message TEXT,
    metrics JSONB,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Data Quality Tracking
CREATE TABLE data_quality_log (
    id SERIAL PRIMARY KEY,
    source VARCHAR(50) NOT NULL,
    check_type VARCHAR(100),
    quality_score DECIMAL(5,2),
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

-- Create indexes for performance
CREATE INDEX idx_auction_lots_date ON auction_lots(auction_date);
CREATE INDEX idx_auction_lots_source ON auction_lots(source);
CREATE INDEX idx_auction_lots_scrape_timestamp ON auction_lots(scrape_timestamp);
CREATE INDEX idx_news_publish_date ON news_articles(publish_date);
CREATE INDEX idx_news_scrape_timestamp ON news_articles(scrape_timestamp);
