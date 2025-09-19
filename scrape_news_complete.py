#!/usr/bin/env python3
"""
TeaTrade Refined News Scraper v2.1
Focused on reliable sources with minimal anti-bot issues
Feeds news.html automatically via news_data.json
"""

import requests
from bs4 import BeautifulSoup
import json
import hashlib
import time
import os
import logging
from datetime import datetime, timedelta
from urllib.parse import urljoin, urlparse
import re
from typing import List, Dict, Set
import random
import feedparser

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('automation/logs/news_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class RefinedTeaNewsAggregator:
    def __init__(self):
        self.session = requests.Session()
        # Rotate user agents
        self.user_agents = [
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15'
        ]
        
        # Data storage
        self.news_database_file = 'data/latest/news.json'
        self.website_news_file = 'news.json'  # This feeds news.html
        self.duplicate_tracker_file = 'data/latest/news_duplicates.json'
        
        # Ensure directories exist
        os.makedirs('data/latest', exist_ok=True)
        os.makedirs('automation/logs', exist_ok=True)
        
        # Load existing data
        self.existing_articles = self.load_existing_articles()
        self.seen_hashes = self.load_duplicate_tracker()
        
        # Enhanced tea and beverage keywords
        self.tea_keywords = {
            'primary': [
                'tea', 'teas', 'chai', 'matcha', 'oolong', 'darjeeling', 'assam', 
                'earl grey', 'jasmine tea', 'green tea', 'black tea', 'white tea',
                'herbal tea', 'tea plantation', 'tea garden', 'tea estate',
                'tea auction', 'tea trade', 'tea export', 'tea import',
                'tea production', 'tea industry', 'tea market', 'tea price',
                'tea workers', 'tea farmers', 'bubble tea', 'milk tea', 'iced tea',
                'tea ceremony', 'afternoon tea', 'tea culture', 'tea leaves'
            ],
            'beverage': [
                'kombucha', 'yerba mate', 'rooibos', 'chamomile', 'peppermint tea',
                'ginger tea', 'lemon tea', 'honey tea', 'turmeric tea', 'detox tea',
                'wellness tea', 'organic tea', 'fair trade tea', 'premium tea'
            ],
            'industry': [
                'beverage industry', 'drink market', 'hot beverages', 'tea sales',
                'tea consumption', 'tea revenue', 'tea manufacturing', 'tea packaging',
                'tea distribution', 'tea retail', 'tea brands', 'tea companies'
            ],
            'health': [
                'antioxidants', 'caffeine', 'health benefits', 'wellness', 'nutrition',
                'immunity', 'metabolism', 'weight loss', 'stress relief', 'energy'
            ],
            'regional': [
                'ceylon tea', 'indian tea', 'chinese tea', 'japanese tea', 'kenyan tea',
                'sri lankan tea', 'assam tea', 'darjeeling tea', 'nilgiri tea'
            ]
        }

        # REFINED NEWS SOURCES - Focus on reliable, accessible sources
        self.news_sources = {
            
            # RSS FEEDS (Most Reliable)
            'bbc_business_rss': {
                'url': 'http://feeds.bbci.co.uk/news/business/rss.xml',
                'type': 'rss',
                'category': 'BBC Business',
                'filter_required': True
            },
            
            'guardian_business_rss': {
                'url': 'https://www.theguardian.com/business/rss',
                'type': 'rss',
                'category': 'Guardian Business',
                'filter_required': True
            },
            
            'reuters_business_rss': {
                'url': 'https://feeds.reuters.com/reuters/businessNews',
                'type': 'rss',
                'category': 'Reuters Business',
                'filter_required': True
            },
            
            'ft_companies_rss': {
                'url': 'https://www.ft.com/companies?format=rss',
                'type': 'rss',
                'category': 'Financial Times',
                'filter_required': True
            },
            
            'economist_business_rss': {
                'url': 'https://www.economist.com/business/rss.xml',
                'type': 'rss',
                'category': 'The Economist',
                'filter_required': True
            },
            
            # NEWS AGGREGATORS & OPEN SOURCES
            'google_news_tea': {
                'url': 'https://news.google.com/rss/search?q=tea%20industry&hl=en-US&gl=US&ceid=US:en',
                'type': 'rss',
                'category': 'Google News',
                'filter_required': False  # Already tea-focused
            },
            
            'google_news_beverages': {
                'url': 'https://news.google.com/rss/search?q=beverage%20industry&hl=en-US&gl=US&ceid=US:en',
                'type': 'rss',
                'category': 'Google News',
                'filter_required': True
            },
            
            # WORKING SPECIALIZED SOURCES FROM TEST
            'times_of_india_tea': {
                'url': 'https://timesofindia.indiatimes.com/topic/tea',
                'selectors': {
                    'articles': '.uwU81, .fHv_i, .story-card',
                    'title': 'h2, h3, .title',
                    'link': 'a',
                    'date': '.timestamp, .date',
                    'summary': 'p, .summary'
                },
                'category': 'Times of India',
                'filter_required': False,
                'delay': (2, 4)
            },
            
            # INDUSTRY PUBLICATIONS - TEA FOCUSED
            'tea_coffee_trade': {
                'url': 'https://www.teaandcoffee.net/news/',
                'selectors': {
                    'articles': 'article, .post, .news-item',
                    'title': 'h2, h3, .entry-title',
                    'link': 'a',
                    'date': '.date, time',
                    'summary': '.excerpt, p'
                },
                'category': 'Tea & Coffee Trade',
                'filter_required': False,
                'delay': (1, 3)
            },
            
            'world_tea_news': {
                'url': 'https://worldteanews.com/category/news/',
                'selectors': {
                    'articles': '.post, article',
                    'title': '.entry-title, h2',
                    'link': 'a',
                    'date': '.entry-date, time',
                    'summary': '.entry-summary, p'
                },
                'category': 'World Tea News',
                'filter_required': False,
                'delay': (1, 2)
            },
            
            'tea_journey_news': {
                'url': 'https://www.teajourney.pub/category/news/',
                'selectors': {
                    'articles': '.post, article',
                    'title': 'h2, .title',
                    'link': 'a',
                    'date': '.date, time',
                    'summary': '.excerpt, p'
                },
                'category': 'Tea Journey',
                'filter_required': False,
                'delay': (1, 2)
            },
            
            # FOOD & BEVERAGE INDUSTRY SOURCES
            'food_business_magazine': {
                'url': 'https://www.foodbusinessmagazine.com/topics/2/beverages',
                'selectors': {
                    'articles': '.article, .content-item',
                    'title': 'h2, h3, .headline',
                    'link': 'a',
                    'date': '.date, time',
                    'summary': '.summary, p'
                },
                'category': 'Food Business',
                'filter_required': True,
                'delay': (2, 4)
            },
            
            'beverage_industry_news': {
                'url': 'https://www.bevindustry.com/topics/3106-tea',
                'selectors': {
                    'articles': '.teaser, .article-teaser',
                    'title': '.headline, h2',
                    'link': 'a',
                    'date': '.date, time',
                    'summary': '.deck, p'
                },
                'category': 'Beverage Industry',
                'filter_required': False,
                'delay': (2, 3)
            },
            
            'food_dive': {
                'url': 'https://www.fooddive.com/search/?q=tea',
                'selectors': {
                    'articles': '.feed__item, article',
                    'title': '.feed__title, h2',
                    'link': 'a',
                    'date': '.feed__date, time',
                    'summary': '.feed__summary, p'
                },
                'category': 'Food Dive',
                'filter_required': False,
                'delay': (2, 4)
            },
            
            'grocery_dive': {
                'url': 'https://www.grocerydive.com/search/?q=tea',
                'selectors': {
                    'articles': '.feed__item, article',
                    'title': '.feed__title, h2',
                    'link': 'a',
                    'date': '.feed__date, time',
                    'summary': '.feed__summary, p'
                },
                'category': 'Grocery Dive',
                'filter_required': False,
                'delay': (2, 4)
            },
            
            # AGRICULTURE & COMMODITY SOURCES
            'agweb_news': {
                'url': 'https://www.agweb.com/news',
                'selectors': {
                    'articles': '.teaser, .news-item',
                    'title': '.headline, h2',
                    'link': 'a',
                    'date': '.date, time',
                    'summary': '.summary, p'
                },
                'category': 'AgWeb',
                'filter_required': True,
                'delay': (2, 4)
            },
            
            'farm_journal': {
                'url': 'https://www.farmjournal.com/crops',
                'selectors': {
                    'articles': '.article-card, .teaser',
                    'title': '.headline, h2',
                    'link': 'a',
                    'date': '.date, time',
                    'summary': '.deck, p'
                },
                'category': 'Farm Journal',
                'filter_required': True,
                'delay': (2, 3)
            },
            
            # HEALTH & WELLNESS SOURCES
            'nutraingredients': {
                'url': 'https://www.nutraingredients.com/search?keys=tea',
                'selectors': {
                    'articles': '.teaser, .search-result',
                    'title': '.headline, h2',
                    'link': 'a',
                    'date': '.date, time',
                    'summary': '.standfirst, p'
                },
                'category': 'Nutra Ingredients',
                'filter_required': False,
                'delay': (2, 4)
            },
            
            'natural_products_insider': {
                'url': 'https://www.naturalproductsinsider.com/search?keys=tea',
                'selectors': {
                    'articles': '.teaser, .search-item',
                    'title': '.headline, h2',
                    'link': 'a',
                    'date': '.date, time',
                    'summary': '.standfirst, p'
                },
                'category': 'Natural Products',
                'filter_required': False,
                'delay': (2, 4)
            },
            
            # RETAIL & CONSUMER SOURCES
            'retail_dive': {
                'url': 'https://www.retaildive.com/search/?q=beverage',
                'selectors': {
                    'articles': '.feed__item, article',
                    'title': '.feed__title, h2',
                    'link': 'a',
                    'date': '.feed__date, time',
                    'summary': '.feed__summary, p'
                },
                'category': 'Retail Dive',
                'filter_required': True,
                'delay': (2, 4)
            },
            
            'convenience_store_news': {
                'url': 'https://csnews.com/category/beverages',
                'selectors': {
                    'articles': '.post, article',
                    'title': '.entry-title, h2',
                    'link': 'a',
                    'date': '.entry-date, time',
                    'summary': '.entry-excerpt, p'
                },
                'category': 'CSNews',
                'filter_required': True,
                'delay': (2, 3)
            },
            
            # SUPPLY CHAIN SOURCES
            'supply_chain_dive': {
                'url': 'https://www.supplychaindive.com/search/?q=tea',
                'selectors': {
                    'articles': '.feed__item, article',
                    'title': '.feed__title, h2',
                    'link': 'a',
                    'date': '.feed__date, time',
                    'summary': '.feed__summary, p'
                },
                'category': 'Supply Chain',
                'filter_required': False,
                'delay': (2, 4)
            },
            
            # INTERNATIONAL SOURCES
            'fresh_plaza_tea': {
                'url': 'https://www.freshplaza.com/search/?q=tea',
                'selectors': {
                    'articles': '.news-item, .article',
                    'title': '.title, h2',
                    'link': 'a',
                    'date': '.date, time',
                    'summary': '.intro, p'
                },
                'category': 'Fresh Plaza',
                'filter_required': False,
                'delay': (2, 3)
            },
            
            'asia_one': {
                'url': 'https://www.asiaone.com/search?query=tea',
                'selectors': {
                    'articles': '.story-card, .article-item',
                    'title': '.headline, h2',
                    'link': 'a',
                    'date': '.date, time',
                    'summary': '.summary, p'
                },
                'category': 'Asia One',
                'filter_required': True,
                'delay': (2, 4)
            },
            
            # ALTERNATIVE RSS SOURCES
            'yahoo_finance_rss': {
                'url': 'https://feeds.finance.yahoo.com/rss/2.0/headline',
                'type': 'rss',
                'category': 'Yahoo Finance',
                'filter_required': True
            },
            
            'cnbc_rss': {
                'url': 'https://www.cnbc.com/id/100003114/device/rss/rss.html',
                'type': 'rss',
                'category': 'CNBC',
                'filter_required': True
            },
            
            # TRADE PUBLICATIONS RSS
            'food_engineering_rss': {
                'url': 'https://www.foodengineeringmag.com/rss/all',
                'type': 'rss',
                'category': 'Food Engineering',
                'filter_required': True
            },
            
            'packaging_world_rss': {
                'url': 'https://www.packworld.com/rss.xml',
                'type': 'rss',
                'category': 'Packaging World',
                'filter_required': True
            }
        }

    def load_existing_articles(self) -> Dict:
        """Load existing articles database"""
        try:
            if os.path.exists(self.news_database_file):
                with open(self.news_database_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            return {"articles": [], "last_updated": None}
        except Exception as e:
            logger.error(f"Error loading existing articles: {e}")
            return {"articles": [], "last_updated": None}
    
    def load_duplicate_tracker(self) -> Set[str]:
        """Load hash tracker for duplicate detection"""
        try:
            if os.path.exists(self.duplicate_tracker_file):
                with open(self.duplicate_tracker_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return set(data.get('hashes', []))
            return set()
        except Exception as e:
            logger.error(f"Error loading duplicate tracker: {e}")
            return set()
    
    def save_duplicate_tracker(self):
        """Save hash tracker"""
        try:
            with open(self.duplicate_tracker_file, 'w', encoding='utf-8') as f:
                json.dump({
                    'hashes': list(self.seen_hashes),
                    'last_updated': datetime.now().isoformat()
                }, f)
        except Exception as e:
            logger.error(f"Error saving duplicate tracker: {e}")
    
    def generate_article_hash(self, title: str, url: str) -> str:
        """Generate unique hash for article"""
        content = f"{title.lower().strip()}{urlparse(url).path}"
        return hashlib.md5(content.encode('utf-8')).hexdigest()
    
    def is_tea_relevant(self, title: str, summary: str) -> bool:
        """Enhanced relevance checking"""
        text = f"{title} {summary}".lower()
        
        # Primary keywords (immediate relevance)
        primary_matches = sum(1 for keyword in self.tea_keywords['primary'] 
                             if keyword in text)
        if primary_matches >= 1:
            return True
        
        # Beverage keywords
        beverage_matches = sum(1 for keyword in self.tea_keywords['beverage'] 
                              if keyword in text)
        if beverage_matches >= 1:
            return True
        
        # Industry keywords (need multiple matches)
        industry_matches = sum(1 for keyword in self.tea_keywords['industry'] 
                              if keyword in text)
        if industry_matches >= 2:
            return True
        
        # Health + beverage context
        health_matches = sum(1 for keyword in self.tea_keywords['health'] 
                            if keyword in text)
        if health_matches >= 1 and ('drink' in text or 'beverage' in text):
            return True
        
        # Regional tea terms
        regional_matches = sum(1 for keyword in self.tea_keywords['regional'] 
                              if keyword in text)
        if regional_matches >= 1:
            return True
        
        return False
    
    def clean_text(self, text: str) -> str:
        """Clean text content"""
        if not text:
            return ""
        
        text = ' '.join(text.split())
        unwanted = ['Read more', 'Continue reading', 'Click here', 'Subscribe', 'Newsletter']
        for phrase in unwanted:
            text = text.replace(phrase, '')
        
        return text.strip()
    
    def get_random_headers(self):
        """Get randomized headers"""
        return {
            'User-Agent': random.choice(self.user_agents),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        }
    
    def parse_rss_feed(self, url: str, source_name: str, category: str, filter_required: bool = True) -> List[Dict]:
        """Parse RSS feed using feedparser for better reliability"""
        articles = []
        try:
            # Use feedparser which handles RSS better than BeautifulSoup
            feed = feedparser.parse(url)
            
            if not feed.entries:
                logger.warning(f"No entries found in RSS feed: {source_name}")
                return articles
            
            logger.info(f"Found {len(feed.entries)} RSS items from {source_name}")
            
            for entry in feed.entries[:15]:  # Limit RSS items
                try:
                    title = entry.get('title', '').strip()
                    link = entry.get('link', '').strip()
                    summary = entry.get('summary', '') or entry.get('description', '')
                    published = entry.get('published', '')
                    
                    if not title or not link or len(title) < 10:
                        continue
                    
                    # Clean summary
                    if summary:
                        # Remove HTML tags from RSS summary
                        summary = re.sub(r'<[^>]+>', '', summary)
                        summary = self.clean_text(summary)
                    
                    # Check for duplicates
                    article_hash = self.generate_article_hash(title, link)
                    if article_hash in self.seen_hashes:
                        continue
                    
                    # Check relevance
                    if filter_required and not self.is_tea_relevant(title, summary):
                        continue
                    
                    article_data = {
                        'title': title,
                        'url': link,
                        'summary': summary[:300] + '...' if len(summary) > 300 else summary,
                        'source': source_name,
                        'category': category,
                        'scraped_date': datetime.now().isoformat(),
                        'published_date': published,
                        'hash': article_hash
                    }
                    
                    articles.append(article_data)
                    self.seen_hashes.add(article_hash)
                    
                except Exception as e:
                    logger.error(f"Error parsing RSS item from {source_name}: {e}")
                    continue
            
        except Exception as e:
            logger.error(f"Error parsing RSS feed {source_name}: {e}")
        
        return articles
    
    def extract_article_data(self, article_element, source_config: Dict, source_name: str, base_url: str) -> Dict:
        """Extract article data from HTML element"""
        try:
            # Extract title
            title_element = article_element.select_one(source_config['selectors']['title'])
            title = self.clean_text(title_element.get_text() if title_element else "")
            
            # Extract link
            link_element = article_element.select_one(source_config['selectors']['link'])
            link = ""
            if link_element:
                href = link_element.get('href', '')
                if href:
                    link = urljoin(base_url, href)
            
            # Skip if essential data missing
            if not title or not link or len(title) < 10:
                return None
            
            # Check duplicates
            article_hash = self.generate_article_hash(title, link)
            if article_hash in self.seen_hashes:
                return None
            
            # Extract other fields
            date_element = article_element.select_one(source_config['selectors']['date'])
            date_text = ""
            if date_element:
                date_text = date_element.get('datetime', '') or self.clean_text(date_element.get_text())
            
            summary_element = article_element.select_one(source_config['selectors']['summary'])
            summary = ""
            if summary_element:
                summary = self.clean_text(summary_element.get_text())
            
            # Check relevance
            if source_config.get('filter_required', False):
                if not self.is_tea_relevant(title, summary):
                    return None
            
            article_data = {
                'title': title,
                'url': link,
                'summary': summary[:300] + '...' if len(summary) > 300 else summary,
                'source': source_name,
                'category': source_config.get('category', 'News'),
                'scraped_date': datetime.now().isoformat(),
                'published_date': date_text,
                'hash': article_hash
            }
            
            self.seen_hashes.add(article_hash)
            return article_data
            
        except Exception as e:
            logger.error(f"Error extracting from {source_name}: {e}")
            return None
    
    def scrape_source(self, source_name: str, source_config: Dict) -> List[Dict]:
        """Scrape individual source"""
        articles = []
        
        try:
            logger.info(f"Scraping {source_name}...")
            
            # Handle RSS feeds
            if source_config.get('type') == 'rss':
                return self.parse_rss_feed(
                    source_config['url'], 
                    source_name, 
                    source_config.get('category', 'RSS'),
                    source_config.get('filter_required', True)
                )
            
            # Handle regular web scraping
            headers = self.get_random_headers()
            
            try:
                response = requests.get(
                    source_config['url'], 
                    headers=headers, 
                    timeout=25,
                    allow_redirects=True
                )
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                logger.warning(f"Failed to fetch {source_name}: {e}")
                return articles
            
            soup = BeautifulSoup(response.content, 'html.parser')
            article_elements = soup.select(source_config['selectors']['articles'])
            
            logger.info(f"Found {len(article_elements)} potential articles from {source_name}")
            
            for element in article_elements[:12]:  # Limit per source
                article_data = self.extract_article_data(
                    element, source_config, source_name, source_config['url']
                )
                if article_data:
                    articles.append(article_data)
            
            logger.info(f"✓ {source_name}: {len(articles)} articles")
            
            # Respectful delay
            delay_range = source_config.get('delay', (1, 3))
            time.sleep(random.uniform(delay_range[0], delay_range[1]))
            
        except Exception as e:
            logger.error(f"✗ Error scraping {source_name}: {e}")
        
        return articles
    
    def save_news_database(self, all_articles: List[Dict]):
        """Save articles to database"""
        try:
            existing_articles = self.existing_articles.get('articles', [])
            combined_articles = existing_articles + all_articles
            
            # Remove duplicates
            unique_articles = []
            seen_hashes = set()
            
            for article in combined_articles:
                article_hash = article.get('hash')
                if article_hash and article_hash not in seen_hashes:
                    unique_articles.append(article)
                    seen_hashes.add(article_hash)
            
            # Sort by date
            unique_articles.sort(key=lambda x: x.get('scraped_date', ''), reverse=True)
            
            database = {
                'articles': unique_articles,
                'total_articles': len(unique_articles),
                'new_articles': len(all_articles),
                'last_updated': datetime.now().isoformat(),
                'sources_count': len(self.news_sources)
            }
            
            with open(self.news_database_file, 'w', encoding='utf-8') as f:
                json.dump(database, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Saved {len(unique_articles)} total articles to database")
            
        except Exception as e:
            logger.error(f"Error saving database: {e}")
    
    def generate_website_news_data(self, all_articles: List[Dict]):
        """Generate news_data.json for news.html page"""
        try:
            # Get recent articles for website
            recent_articles = all_articles[:50]
            
            # Convert to format expected by news.html
            formatted_articles = []
            for article in recent_articles:
                formatted_article = {
                    'source': article.get('source', 'Unknown'),
                    'headline': article.get('title', 'No Title'),
                    'snippet': article.get('summary', 'No summary available'),
                    'link': article.get('url', '#'),
                    'image': 'images/news-general.svg'  # Default image
                }
                
                # Choose image based on category/source
                category = article.get('category', '').lower()
                if 'tea' in category or 'industry' in article.get('source', '').lower():
                    formatted_article['image'] = 'images/news-industry.svg'
                elif any(word in category for word in ['international', 'asia', 'global']):
                    formatted_article['image'] = 'images/news-international.svg'
                
                formatted_articles.append(formatted_article)
            
            # Save in format expected by news.html
            with open(self.website_news_file, 'w', encoding='utf-8') as f:
                json.dump(formatted_articles, f, indent=2, ensure_ascii=False)
            
            logger.info(f"Generated news_data.json with {len(formatted_articles)} articles for website")
            
        except Exception as e:
            logger.error(f"Error generating website data: {e}")
    
    def run_aggregation(self):
        """Run complete news aggregation"""
        logger.info("Starting Refined TeaTrade News Aggregation...")
        start_time = datetime.now()
        
        all_new_articles = []
        successful_sources = 0
        failed_sources = []
        
        # Randomize order to distribute load
        sources_list = list(self.news_sources.items())
        random.shuffle(sources_list)
        
        for source_name, source_config in sources_list:
            try:
                articles = self.scrape_source(source_name, source_config)
                all_new_articles.extend(articles)
                if articles:
                    successful_sources += 1
                else:
                    logger.info(f"○ {source_name}: 0 articles")
                    
            except Exception as e:
                logger.error(f"✗ {source_name}: {e}")
                failed_sources.append(source_name)
        
        logger.info(f"Collected {len(all_new_articles)} new articles from {successful_sources}/{len(self.news_sources)} sources")
        
        if all_new_articles:
            # Save to database
            self.save_news_database(all_new_articles)
            
            # Generate website data (this feeds news.html)
            all_articles = self.existing_articles.get('articles', []) + all_new_articles
            self.generate_website_news_data(all_articles)
            
            # Save duplicate tracker
            self.save_duplicate_tracker()
        
        duration = (datetime.now() - start_time).total_seconds()
        
        logger.info(f"Refined news aggregation completed in {duration:.2f} seconds")
        
        return {
            'success': True,
            'new_articles': len(all_new_articles),
            'sources_successful': successful_sources,
            'sources_failed': len(failed_sources),
            'total_sources': len(self.news_sources),
            'duration': duration
        }

def main():
    """Main execution"""
    try:
        aggregator = RefinedTeaNewsAggregator()
        result = aggregator.run_aggregation()
        
        print(f"\n=== REFINED TEA NEWS AGGREGATION COMPLETE ===")
        print(f"New articles found: {result['new_articles']}")
        print(f"Sources successful: {result['sources_successful']}/{result['total_sources']}")
        print(f"Sources failed: {result['sources_failed']}")
        print(f"Duration: {result['duration']:.2f} seconds")
        print(f"Website feed: news_data.json")
        print(f"Database: data/latest/news_database.json")
        
        return True
        
    except Exception as e:
        logger.error(f"Error in main: {e}")
        return False

if __name__ == "__main__":
    main()
