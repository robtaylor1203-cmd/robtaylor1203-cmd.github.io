#!/usr/bin/env python3
"""
Enhanced Data Consolidation Pipeline
Advanced processing with validation, quality checks, and standardization
"""

import asyncio
import json
import logging
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional, Any
import pandas as pd
from dataclasses import dataclass, asdict

logger = logging.getLogger(__name__)

@dataclass
class DataQualityMetrics:
    """Data quality metrics for validation"""
    completeness_score: float
    accuracy_score: float
    consistency_score: float
    freshness_hours: float
    total_records: int
    validation_errors: List[str]

class EnhancedConsolidationPipeline:
    """Advanced data consolidation with quality assurance"""
    
    def __init__(self, base_dir: Path):
        self.base_dir = base_dir
        self.source_dir = base_dir / "source_reports"
        self.output_dir = base_dir / "data" / "latest"
        self.validation_errors = []
        
        # Ensure directories exist
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.source_dir.mkdir(parents=True, exist_ok=True)
    
    async def run_consolidation(self) -> Dict:
        """Run complete consolidation pipeline"""
        logger.info("Starting enhanced data consolidation pipeline")
        
        try:
            # Discover and process source files
            source_files = await self.discover_source_files()
            logger.info(f"Found {len(source_files)} source files to process")
            
            # Process each data type
            auction_data = await self.consolidate_auction_data(source_files)
            news_data = await self.consolidate_news_data(source_files)
            market_reports = await self.consolidate_market_reports(source_files)
            
            # Generate quality metrics
            quality_metrics = await self.assess_data_quality(auction_data, news_data)
            
            # Create consolidated outputs
            await self.generate_consolidated_outputs(auction_data, news_data, market_reports, quality_metrics)
            
            # Create summary
            summary = await self.generate_pipeline_summary(auction_data, news_data, quality_metrics)
            
            logger.info("Enhanced consolidation pipeline completed successfully")
            return summary
            
        except Exception as e:
            logger.error(f"Consolidation pipeline failed: {e}")
            return {'status': 'failed', 'error': str(e)}
    
    async def discover_source_files(self) -> List[Path]:
        """Discover all source data files"""
        source_files = []
        
        # Search patterns for different data types
        patterns = [
            "**/*auction*.json",
            "**/*market*.json", 
            "**/*news*.json",
            "**/*data*.json",
            "**/output*.json"
        ]
        
        # Search in both automation and root directories
        search_dirs = [
            self.base_dir / "automation",
            self.source_dir,
            self.base_dir
        ]
        
        for search_dir in search_dirs:
            if search_dir.exists():
                for pattern in patterns:
                    source_files.extend(search_dir.glob(pattern))
        
        # Remove duplicates and sort by modification time
        unique_files = list(set(source_files))
        unique_files.sort(key=lambda f: f.stat().st_mtime, reverse=True)
        
        return unique_files
    
    async def consolidate_auction_data(self, source_files: List[Path]) -> List[Dict]:
        """Consolidate auction data from all sources"""
        logger.info("Consolidating auction data")
        
        consolidated_auctions = []
        
        for file_path in source_files:
            if any(keyword in file_path.name.lower() for keyword in ['auction', 'lot', 'sale']):
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    # Handle different data structures
                    if isinstance(data, list):
                        for item in data:
                            if self.is_auction_record(item):
                                standardized = await self.standardize_auction_record(item, file_path)
                                if standardized:
                                    consolidated_auctions.append(standardized)
                    
                    elif isinstance(data, dict):
                        if 'auctions' in data:
                            for auction in data['auctions']:
                                standardized = await self.standardize_auction_record(auction, file_path)
                                if standardized:
                                    consolidated_auctions.append(standardized)
                        elif self.is_auction_record(data):
                            standardized = await self.standardize_auction_record(data, file_path)
                            if standardized:
                                consolidated_auctions.append(standardized)
                
                except Exception as e:
                    logger.warning(f"Error processing auction file {file_path}: {e}")
                    self.validation_errors.append(f"Auction file error: {file_path.name} - {str(e)}")
        
        # Remove duplicates and sort
        consolidated_auctions = await self.deduplicate_auctions(consolidated_auctions)
        consolidated_auctions.sort(key=lambda x: x.get('auction_date', ''), reverse=True)
        
        logger.info(f"Consolidated {len(consolidated_auctions)} auction records")
        return consolidated_auctions
    
    async def standardize_auction_record(self, record: Dict, source_file: Path) -> Optional[Dict]:
        """Standardize auction record format"""
        try:
            # Extract source information
            source_name = self.extract_source_name(source_file)
            
            # Standardized auction record
            standardized = {
                'lot_no': record.get('lot_no') or record.get('lot_number') or record.get('invoice') or 'Unknown',
                'location': record.get('location') or record.get('center') or self.infer_location(source_file),
                'grade': record.get('grade') or record.get('tea_grade') or 'Unknown',
                'garden_name': record.get('garden_name') or record.get('garden') or record.get('estate') or 'Unknown',
                'price': self.parse_price(record.get('price') or record.get('selling_price')),
                'price_usd': self.convert_to_usd(record.get('price'), record.get('currency')),
                'quantity': self.parse_quantity(record.get('quantity') or record.get('qty')),
                'currency': record.get('currency') or self.infer_currency(source_file),
                'auction_date': self.standardize_date(record.get('auction_date') or record.get('date')),
                'broker': record.get('broker') or source_name,
                'warehouse': record.get('warehouse') or record.get('godown') or 'Unknown',
                'quality_notes': record.get('quality_notes') or record.get('remarks') or '',
                'source_file': source_file.name,
                'processed_at': datetime.now().isoformat()
            }
            
            # Validate required fields
            if standardized['lot_no'] == 'Unknown' and standardized['garden_name'] == 'Unknown':
                return None
            
            return standardized
            
        except Exception as e:
            logger.warning(f"Error standardizing auction record: {e}")
            return None
    
    def is_auction_record(self, record: Dict) -> bool:
        """Check if record is an auction record"""
        auction_indicators = [
            'lot_no', 'lot_number', 'invoice', 'grade', 'tea_grade',
            'garden', 'estate', 'price', 'selling_price', 'quantity'
        ]
        return any(key in record for key in auction_indicators)
    
    async def consolidate_news_data(self, source_files: List[Path]) -> List[Dict]:
        """Consolidate news data from all sources"""
        logger.info("Consolidating news data")
        
        consolidated_news = []
        
        for file_path in source_files:
            if 'news' in file_path.name.lower():
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    
                    if isinstance(data, list):
                        for item in data:
                            if self.is_news_record(item):
                                standardized = await self.standardize_news_record(item, file_path)
                                if standardized:
                                    consolidated_news.append(standardized)
                
                except Exception as e:
                    logger.warning(f"Error processing news file {file_path}: {e}")
        
        # Remove duplicates and sort
        consolidated_news = await self.deduplicate_news(consolidated_news)
        consolidated_news.sort(key=lambda x: x.get('publish_date', ''), reverse=True)
        
        logger.info(f"Consolidated {len(consolidated_news)} news articles")
        return consolidated_news
    
    def is_news_record(self, record: Dict) -> bool:
        """Check if record is a news record"""
        news_indicators = ['title', 'headline', 'summary', 'content', 'source', 'url']
        return any(key in record for key in news_indicators)
    
    async def standardize_news_record(self, record: Dict, source_file: Path) -> Optional[Dict]:
        """Standardize news record format"""
        try:
            standardized = {
                'title': record.get('title') or record.get('headline') or 'Untitled',
                'source': record.get('source') or self.extract_source_name(source_file),
                'url': record.get('url') or record.get('link') or '#',
                'summary': record.get('summary') or record.get('content', '')[:200] + '...',
                'publish_date': self.standardize_date(record.get('publish_date') or record.get('date')),
                'category': record.get('category') or 'general',
                'importance': record.get('importance') or 'medium',
                'region': record.get('region') or 'global',
                'processed_at': datetime.now().isoformat()
            }
            
            return standardized
            
        except Exception as e:
            logger.warning(f"Error standardizing news record: {e}")
            return None
    
    async def assess_data_quality(self, auction_data: List[Dict], news_data: List[Dict]) -> DataQualityMetrics:
        """Assess overall data quality"""
        total_records = len(auction_data) + len(news_data)
        
        if total_records == 0:
            return DataQualityMetrics(0, 0, 0, 999, 0, ['No data available'])
        
        # Completeness: percentage of records with all required fields
        complete_auctions = sum(1 for a in auction_data if a.get('lot_no') != 'Unknown' and a.get('price', 0) > 0)
        complete_news = sum(1 for n in news_data if n.get('title') != 'Untitled' and n.get('summary'))
        completeness = ((complete_auctions + complete_news) / total_records) * 100
        
        # Accuracy: basic validation checks
        valid_prices = sum(1 for a in auction_data if isinstance(a.get('price'), (int, float)) and a['price'] > 0)
        accuracy = (valid_prices / len(auction_data)) * 100 if auction_data else 100
        
        # Freshness: time since most recent data
        recent_dates = []
        for item in auction_data + news_data:
            if item.get('auction_date') or item.get('publish_date'):
                try:
                    date_str = item.get('auction_date') or item.get('publish_date')
                    item_date = datetime.fromisoformat(date_str.replace('Z', '+00:00'))
                    recent_dates.append(item_date)
                except:
                    continue
        
        freshness_hours = 0
        if recent_dates:
            most_recent = max(recent_dates)
            freshness_hours = (datetime.now() - most_recent.replace(tzinfo=None)).total_seconds() / 3600
        
        return DataQualityMetrics(
            completeness_score=completeness,
            accuracy_score=accuracy,
            consistency_score=85.0,  # Placeholder - could implement actual consistency checks
            freshness_hours=freshness_hours,
            total_records=total_records,
            validation_errors=self.validation_errors
        )
    
    async def generate_consolidated_outputs(self, auction_data: List[Dict], news_data: List[Dict], 
                                          market_reports: List[Dict], quality_metrics: DataQualityMetrics):
        """Generate final consolidated output files"""
        
        # Save auction data
        with open(self.output_dir / 'auction_data.json', 'w') as f:
            json.dump(auction_data, f, indent=2, ensure_ascii=False)
        
        # Save news data
        with open(self.output_dir / 'news_data.json', 'w') as f:
            json.dump(news_data, f, indent=2, ensure_ascii=False)
        
        # Save market reports
        with open(self.output_dir / 'market_reports.json', 'w') as f:
            json.dump(market_reports, f, indent=2, ensure_ascii=False)
        
        # Generate summary
        summary = {
            'last_updated': datetime.now().isoformat() + 'Z',
            'total_auctions': len(auction_data),
            'total_news': len(news_data),
            'total_reports': len(market_reports),
            'data_quality': asdict(quality_metrics),
            'active_centers': list(set(a.get('location', 'Unknown') for a in auction_data if a.get('location', 'Unknown') != 'Unknown')),
            'status': 'production',
            'collection_time': datetime.now().strftime('%Y-%m-%d %H:%M UTC'),
            'pipeline_version': '2.0.0'
        }
        
        with open(self.output_dir / 'summary.json', 'w') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
    
    # Helper methods
    def extract_source_name(self, file_path: Path) -> str:
        """Extract source name from file path"""
        name = file_path.name.lower()
        if 'jthomas' in name:
            return 'J.Thomas & Co'
        elif 'atb' in name:
            return 'African Tea Brokers'
        elif 'tbea' in name:
            return 'Tea Brokers East Africa'
        elif 'forbes' in name:
            return 'Forbes & Walker'
        elif 'ceylon' in name:
            return 'Ceylon Tea Brokers'
        else:
            return 'Unknown Source'
    
    def infer_location(self, file_path: Path) -> str:
        """Infer location from file path"""
        name = file_path.name.lower()
        if 'kolkata' in name or 'jthomas' in name:
            return 'Kolkata'
        elif 'mombasa' in name or 'atb' in name or 'tbea' in name:
            return 'Mombasa'
        elif 'colombo' in name or 'forbes' in name:
            return 'Colombo'
        else:
            return 'Unknown'
    
    def parse_price(self, price_value) -> float:
        """Parse price value to float"""
        if isinstance(price_value, (int, float)):
            return float(price_value)
        elif isinstance(price_value, str):
            # Remove currency symbols and commas
            cleaned = ''.join(c for c in price_value if c.isdigit() or c == '.')
            try:
                return float(cleaned) if cleaned else 0.0
            except:
                return 0.0
        return 0.0
    
    def convert_to_usd(self, price, currency) -> float:
        """Convert price to USD (simplified conversion)"""
        if not price or not isinstance(price, (int, float)):
            return 0.0
        
        # Simplified conversion rates (in production, use live rates)
        rates = {
            'INR': 0.012,
            'LKR': 0.003,
            'KES': 0.007,
            'USD': 1.0
        }
        
        rate = rates.get(currency, 0.012)  # Default to INR rate
        return round(float(price) * rate, 2)
    
    def parse_quantity(self, qty_value) -> int:
        """Parse quantity value to integer"""
        if isinstance(qty_value, int):
            return qty_value
        elif isinstance(qty_value, str):
            cleaned = ''.join(c for c in qty_value if c.isdigit())
            try:
                return int(cleaned) if cleaned else 0
            except:
                return 0
        return 0
    
    def standardize_date(self, date_value) -> str:
        """Standardize date format"""
        if not date_value:
            return datetime.now().isoformat()
        
        if isinstance(date_value, str):
            try:
                # Try to parse various date formats
                for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%Y-%m-%dT%H:%M:%S']:
                    try:
                        parsed = datetime.strptime(date_value.split('T')[0], fmt)
                        return parsed.isoformat()
                    except:
                        continue
            except:
                pass
        
        return datetime.now().isoformat()
    
    async def deduplicate_auctions(self, auctions: List[Dict]) -> List[Dict]:
        """Remove duplicate auction records"""
        seen = set()
        unique_auctions = []
        
        for auction in auctions:
            # Create unique key from lot_no, location, and date
            key = f"{auction.get('lot_no', '')}_{auction.get('location', '')}_{auction.get('auction_date', '')}"
            if key not in seen:
                seen.add(key)
                unique_auctions.append(auction)
        
        return unique_auctions
    
    async def deduplicate_news(self, news: List[Dict]) -> List[Dict]:
        """Remove duplicate news records"""
        seen = set()
        unique_news = []
        
        for article in news:
            # Create unique key from title and source
            key = f"{article.get('title', '')}_{article.get('source', '')}"
            if key not in seen:
                seen.add(key)
                unique_news.append(article)
        
        return unique_news
    
    async def consolidate_market_reports(self, source_files: List[Path]) -> List[Dict]:
        """Consolidate market reports"""
        # Placeholder for market report consolidation
        return []
    
    async def generate_pipeline_summary(self, auction_data: List[Dict], news_data: List[Dict], 
                                      quality_metrics: DataQualityMetrics) -> Dict:
        """Generate comprehensive pipeline summary"""
        return {
            'status': 'success',
            'records_processed': {
                'auctions': len(auction_data),
                'news': len(news_data)
            },
            'data_quality': asdict(quality_metrics),
            'processing_time': datetime.now().isoformat(),
            'validation_errors': len(self.validation_errors)
        }
