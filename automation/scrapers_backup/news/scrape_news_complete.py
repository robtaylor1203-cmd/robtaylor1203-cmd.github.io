#!/usr/bin/env python3
"""
Complete Tea Industry News Scraper - Enterprise Grade
Multi-source tea industry news aggregation
"""

import json
import sys
import time
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import requests
from bs4 import BeautifulSoup
import re
import feedparser

# Add utils to path
sys.path.append(str(Path(__file__).parent.parent.parent))
from utils.pipeline_utils import (
    setup_logging, standardize_data_format, save_to_database,
    clean_text
)

class CompleteNewsScaper:
    """Enterprise tea industry news scraper"""
    
    def __init__(self):
        self.logger = setup_logging('NEWS')
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36'
        })
        self.scraped_articles = []
        
        # News sources configuration
        self.news_sources = {
            'tea_industry_sites': [
                {
                    'name': 'Tea Industry Times',
                    'url': 'https://www.teaindustrytimes.com',
                    'rss': 'https://www.teaindustrytimes.com/feed'
                },
                {
                    'name': 'World Tea News',
                    'url': 'https://worldteanews.com',
                    'rss': 'https://worldteanews.com/feed'
                },
                {
                    'name': 'Tea Journey',
                    'url': 'https://teajourney.pub',
                    'selector': 'article'
                }
            ],
            'general_news_with_tea': [
                {
                    'name': 'Reuters Agriculture',
                    'search_url': 'https://www.reuters.com/search/news?blob=tea+auction',
                    'selector': '.search-result-more-info'
                },
                {
                    'name': 'Bloomberg Agriculture',
                    'search_url': 'https://www.bloomberg.com/search?query=tea+auction+prices',
                    'selector': '.result-item'
                }
            ],
            'regional_sources': [
                {
                    'name': 'Ceylon Today',
                    'url': 'https://ceylontoday.lk',
                    'search_terms': ['tea', 'auction', 'export']
                },
                {
                    'name': 'The Hindu Business',
                    'url': 'https://www.thehindu.com/business',
                    'search_terms': ['tea', 'auction', 'darjeeling', 'assam']
                },
                {
                    'name': 'Kenya Tea Board',
                    'url': 'https://teaboard.or.ke/news',
                    'selector': '.news-item'
                }
            ]
        }
    
    def scrape_rss_feeds(self) -> List[Dict]:
        """Scrape news from RSS feeds"""
        
        articles = []
        
        try:
            self.logger.info("📡 Scraping RSS feeds")
            
            for source in self.news_sources['tea_industry_sites']:
                if 'rss' in source:
                    try:
                        self.logger.info(f"📰 Fetching RSS: {source['name']}")
                        
                        feed = feedparser.parse(source['rss'])
                        
                        for entry in feed.entries[:10]:  # Limit per feed
                            article = {
                                'title': clean_text(entry.get('title', '')),
                                'content': clean_text(entry.get('description', '')),
                                'url': entry.get('link', ''),
                                'source': source['name'],
                                'publish_date': self.parse_date(entry.get('published', '')),
                                'categories': ['tea', 'industry']
                            }
                            
                            if article['title'] and article['url']:
                                articles.append(article)
                        
                        time.sleep(2)  # Rate limiting
                    
                    except Exception as rss_error:
                        self.logger.warning(f"RSS error for {source['name']}: {rss_error}")
                        continue
            
            self.logger.info(f"📊 RSS feeds: {len(articles)} articles")
            
        except Exception as e:
            self.logger.error(f"❌ RSS scraping failed: {e}")
        
        return articles
    
    def scrape_website_news(self, source: Dict) -> List[Dict]:
        """Scrape news from website"""
        
        articles = []
        
        try:
            self.logger.info(f"🌐 Scraping website: {source['name']}")
            
            # Try different URL patterns
            urls_to_try = [
                source.get('url', ''),
                source.get('search_url', ''),
                f"{source.get('url', '')}/news",
                f"{source.get('url', '')}/articles"
            ]
            
            for url in urls_to_try:
                if not url:
                    continue
                
                try:
                    response = self.session.get(url, timeout=30)
                    response.raise_for_status()
                    
                    soup = BeautifulSoup(response.content, 'html.parser')
                    
                    # Extract articles using selector
                    selector = source.get('selector', 'article')
                    article_elements = soup.select(selector)
                    
                    if not article_elements:
                        # Fallback selectors
                        fallback_selectors = [
                            '.article', '.news-item', '.post', 
                            '.entry', '[class*="article"]', 
                            '[class*="news"]', 'h2 a', 'h3 a'
                        ]
                        
                        for fallback in fallback_selectors:
                            article_elements = soup.select(fallback)
                            if article_elements:
                                break
                    
                    # Extract article data
                    for element in article_elements[:15]:  # Limit per page
                        article = self.extract_article_data(element, source, url)
                        if article:
                            articles.append(article)
                    
                    if articles:
                        break  # Found articles, no need to try other URLs
                
                except Exception as url_error:
                    continue
            
            self.logger.info(f"📊 {source['name']}: {len(articles)} articles")
            
        except Exception as e:
            self.logger.warning(f"Website scraping error for {source['name']}: {e}")
        
        return articles
    
    def extract_article_data(self, element, source: Dict, base_url: str) -> Optional[Dict]:
        """Extract individual article data from HTML element"""
        
        try:
            # Extract title
            title_elem = element.find(['h1', 'h2', 'h3', 'h4', 'a'])
            title = clean_text(title_elem.get_text()) if title_elem else ""
            
            # Extract URL
            url = ""
            link_elem = element.find('a', href=True)
            if link_elem:
                href = link_elem.get('href')
                if href.startswith('http'):
                    url = href
                elif href.startswith('/'):
                    url = f"{base_url.rstrip('/')}{href}"
                else:
                    url = f"{base_url.rstrip('/')}/{href}"
            
            # Extract content/summary
            content = ""
            content_elem = element.find(['p', '.excerpt', '.summary', '.description'])
            if content_elem:
                content = clean_text(content_elem.get_text())
            else:
                # Use all text from element
                content = clean_text(element.get_text())
            
            # Extract image
            image_url = ""
            img_elem = element.find('img', src=True)
            if img_elem:
                src = img_elem.get('src')
                if src.startswith('http'):
                    image_url = src
                elif src.startswith('/'):
                    image_url = f"{base_url.rstrip('/')}{src}"
            
            # Extract date
            date_text = ""
            date_elem = element.find(['time', '.date', '.published'])
            if date_elem:
                date_text = date_elem.get('datetime') or date_elem.get_text()
            
            # Filter for tea-related content
            tea_keywords = ['tea', 'auction', 'estate', 'garden', 'plantation', 'leaves', 'brew']
            combined_text = f"{title} {content}".lower()
            
            if any(keyword in combined_text for keyword in tea_keywords):
                return {
                    'title': title,
                    'content': content[:500],  # Limit content length
                    'url': url,
                    'source': source['name'],
                    'image_url': image_url,
                    'publish_date': self.parse_date(date_text),
                    'categories': ['tea', 'industry']
                }
            
        except Exception as e:
            self.logger.warning(f"Article extraction error: {e}")
        
        return None
    
    def search_tea_news(self) -> List[Dict]:
        """Search for tea-related news using search engines"""
        
        articles = []
        
        try:
            self.logger.info("🔍 Searching for tea industry news")
            
            search_queries = [
                "tea auction prices today",
                "tea industry news 2024",
                "Ceylon tea auction results", 
                "Darjeeling tea garden news",
                "Kenya tea auction report",
                "tea market analysis"
            ]
            
            for query in search_queries[:3]:  # Limit searches
                try:
                    # Use DuckDuckGo search (more permissive)
                    search_url = f"https://duckduckgo.com/html/?q={query.replace(' ', '+')}"
                    
                    response = self.session.get(search_url, timeout=30)
                    soup = BeautifulSoup(response.content, 'html.parser')
                    
                    # Extract search results
                    for result in soup.find_all('a', class_='result__a')[:5]:
                        href = result.get('href')
                        title = clean_text(result.get_text())
                        
                        if href and title:
                            articles.append({
                                'title': title,
                                'url': href,
                                'source': 'Search Result',
                                'content': f"Found via search: {query}",
                                'publish_date': datetime.now().strftime('%Y-%m-%d'),
                                'categories': ['tea', 'search']
                            })
                    
                    time.sleep(3)  # Rate limiting
                
                except Exception as search_error:
                    continue
            
            self.logger.info(f"🔍 Search results: {len(articles)} articles")
            
        except Exception as e:
            self.logger.error(f"❌ Search scraping failed: {e}")
        
        return articles
    
    def parse_date(self, date_string: str) -> str:
        """Parse date from various formats"""
        
        if not date_string:
            return datetime.now().strftime('%Y-%m-%d')
        
        # Common date patterns
        date_patterns = [
            r'(\d{4})-(\d{2})-(\d{2})',
            r'(\d{2})/(\d{2})/(\d{4})',
            r'(\d{1,2})\s*(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s*(\d{4})'
        ]
        
        for pattern in date_patterns:
            match = re.search(pattern, date_string, re.IGNORECASE)
            if match:
                try:
                    groups = match.groups()
                    if len(groups) == 3:
                        if groups[1].isalpha():  # Month name
                            month_map = {
                                'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4,
                                'may': 5, 'jun': 6, 'jul': 7, 'aug': 8,
                                'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
                            }
                            month = month_map.get(groups[1][:3].lower())
                            if month:
                                return f"{groups[2]}-{month:02d}-{int(groups[0]):02d}"
                        else:
                            # Determine format
                            if len(groups[0]) == 4:  # YYYY-MM-DD
                                return f"{groups[0]}-{groups[1]}-{groups[2]}"
                            else:  # DD/MM/YYYY
                                return f"{groups[2]}-{int(groups[1]):02d}-{int(groups[0]):02d}"
                except:
                    continue
        
        return datetime.now().strftime('%Y-%m-%d')
    
    def run_complete_scrape(self) -> bool:
        """Execute complete news scraping"""
        
        try:
            self.logger.info("🚀 Starting COMPLETE tea industry news scraping")
            
            all_articles = []
            
            # 1. Scrape RSS feeds
            rss_articles = self.scrape_rss_feeds()
            all_articles.extend(rss_articles)
            
            # 2. Scrape website news
            for source in self.news_sources['tea_industry_sites']:
                website_articles = self.scrape_website_news(source)
                all_articles.extend(website_articles)
                time.sleep(2)
            
            for source in self.news_sources['regional_sources']:
                website_articles = self.scrape_website_news(source)
                all_articles.extend(website_articles)
                time.sleep(2)
            
            # 3. Search for tea news
            search_articles = self.search_tea_news()
            all_articles.extend(search_articles)
            
            # 4. Remove duplicates and validate
            unique_articles = self.remove_duplicate_articles(all_articles)
            
            if unique_articles:
                # Standardize data format
                standardized_data = [
                    standardize_data_format(article, 'NEWS')['processed_data'] 
                    for article in unique_articles
                ]
                
                # Save to database
                if save_to_database(standardized_data, 'news_articles', 'NEWS'):
                    self.scraped_articles = standardized_data
                    
                    self.logger.info(f"✅ News complete: {len(standardized_data)} articles saved")
                    return True
            
            self.logger.warning("⚠️ No news articles found")
            return False
            
        except Exception as e:
            self.logger.error(f"❌ Complete news scrape failed: {e}")
            return False
    
    def remove_duplicate_articles(self, articles: List[Dict]) -> List[Dict]:
        """Remove duplicate articles based on title similarity"""
        
        try:
            unique_articles = []
            seen_titles = set()
            
            for article in articles:
                title = article.get('title', '').lower().strip()
                
                # Simple duplicate check based on title
                if title and title not in seen_titles and len(title) > 10:
                    unique_articles.append(article)
                    seen_titles.add(title)
            
            removed_count = len(articles) - len(unique_articles)
            if removed_count > 0:
                self.logger.info(f"🧹 Removed {removed_count} duplicate articles")
            
            return unique_articles
            
        except Exception as e:
            self.logger.error(f"❌ Error removing duplicates: {e}")
            return articles

def main():
    """Main execution function"""
    scraper = CompleteNewsScaper()
    success = scraper.run_complete_scrape()
    
    if success:
        print("✅ Complete tea news scraping finished successfully")
        return 0
    else:
        print("❌ Complete tea news scraping failed")
        return 1

if __name__ == "__main__":
    exit(main())
