#!/usr/bin/env python3
"""
TeaTrade Intelligence Aggregator
Enhances consolidated reports with market intelligence and creates website-compatible library
"""

import json
import os
from datetime import datetime
from pathlib import Path
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class TeaTradeIntelligenceAggregator:
    def __init__(self, repo_root="."):
        self.repo_root = Path(repo_root)
        self.consolidated_dir = self.repo_root / "Data" / "Consolidated"
        self.library_file = self.repo_root / "market-reports-library.json"
        
        # Create directories if they don't exist
        self.consolidated_dir.mkdir(parents=True, exist_ok=True)

    def enhance_with_market_intelligence(self, data):
        """Add market intelligence sections to consolidated data"""
        metadata = data.get("metadata", {})
        summary = data.get("summary", {})
        volume_analysis = data.get("volume_analysis", {})
        auction_centre = metadata.get("auction_centre", "Unknown")
        
        # Generate market intelligence commentary
        market_intelligence = self.generate_market_commentary(data)
        weather_analysis = self.generate_weather_analysis(auction_centre)
        forecast_analysis = self.generate_forecast_analysis(data)
        
        # Enhanced data structure
        enhanced_data = data.copy()
        enhanced_data.update({
            "market_intelligence": market_intelligence,
            "weather_analysis": weather_analysis,
            "forecast_analysis": forecast_analysis,
            "buyer_activity": self.generate_buyer_activity(data),
            "factory_performance": self.generate_factory_performance(data),
            "price_quotations": self.generate_price_quotations(data),
            "interactive_data": {
                "raw_lots": data.get("auction_lots", [])[:50]  # Limit for web display
            }
        })
        
        return enhanced_data

    def generate_market_commentary(self, data):
        """Generate dynamic market commentary based on data patterns"""
        summary = data.get("summary", {})
        volume_analysis = data.get("volume_analysis", {})
        auction_centre = data.get("metadata", {}).get("auction_centre", "Market")
        
        total_offered = summary.get("total_offered_kg", 0)
        total_lots = summary.get("total_lots", 0)
        avg_price = summary.get("auction_average_price", 0)
        
        # Dynamic commentary based on data patterns
        if total_offered > 100000:
            volume_comment = f"Substantial offering of {total_offered:,} kg indicates strong regional production"
        elif total_offered > 50000:
            volume_comment = f"Moderate offering of {total_offered:,} kg reflects steady market activity"
        else:
            volume_comment = f"Focused offering of {total_offered:,} kg suggests selective marketing"
        
        grades_offered = len(volume_analysis.get("by_grade_summary", {}))
        
        executive_commentary = f"{auction_centre} market demonstrated steady trading conditions. {volume_comment}. Quality premiums evident across {grades_offered} grade categories."
        
        detailed_analysis = f"Market dynamics reflect regional production patterns. Average pricing of {avg_price:.0f} per kg indicates balanced demand sentiment. Buyer participation across grade categories suggests active market engagement."
        
        return {
            "executive_commentary": executive_commentary,
            "detailed_analysis": detailed_analysis,
            "volume_insights": volume_comment,
            "quality_assessment": f"Average pricing of {avg_price:.0f} indicates balanced market conditions"
        }

    def generate_weather_analysis(self, auction_centre):
        """Generate location-specific weather analysis"""
        weather_patterns = {
            "Colombo": {
                "current_conditions": "Monsoon transition period with moderate rainfall patterns supporting tea bush health",
                "impact_assessment": "Favorable growing conditions maintaining leaf quality standards",
                "seasonal_outlook": "Inter-monsoon period expected to support consistent production volumes"
            },
            "Mombasa": {
                "current_conditions": "East African highland climate with optimal temperature ranges for CTC production",
                "impact_assessment": "Stable weather patterns supporting continuous manufacturing operations",
                "seasonal_outlook": "Regional climate stability expected to maintain production consistency"
            },
            "Nairobi": {
                "current_conditions": "Highland conditions with cool temperatures benefiting quality production",
                "impact_assessment": "Elevation and climate combination producing premium grade characteristics",
                "seasonal_outlook": "Seasonal patterns favorable for sustained quality output"
            }
        }
        
        return weather_patterns.get(auction_centre, {
            "current_conditions": "Regional weather patterns supporting tea production activities",
            "impact_assessment": "Climate conditions within normal ranges for quality tea manufacture",
            "seasonal_outlook": "Weather outlook supportive of continued production operations"
        })

    def generate_forecast_analysis(self, data):
        """Generate market forecast analysis"""
        summary = data.get("summary", {})
        total_offered = summary.get("total_offered_kg", 0)
        avg_price = summary.get("auction_average_price", 0)
        
        supply_outlook = "Steady" if total_offered > 50000 else "Tight"
        price_trend = "Stable" if 300 <= avg_price <= 400 else "Premium" if avg_price > 400 else "Competitive"
        
        return {
            "supply_dynamics": f"{supply_outlook} supply conditions with production aligned to seasonal patterns",
            "price_trends": f"{price_trend} pricing environment reflecting current market fundamentals",
            "demand_indicators": "Export and domestic demand maintaining balanced absorption",
            "opportunities": "Quality premiums available for superior leaf grades and processing standards"
        }

    def generate_buyer_activity(self, data):
        """Generate simulated buyer activity data"""
        lots = data.get("auction_lots", [])
        buyers = set()
        
        for lot in lots:
            buyer = lot.get("buyer", f"Buyer_{len(buyers) % 10 + 1}")
            buyers.add(buyer)
        
        # Create buyer summary
        buyer_activity = []
        for i, buyer in enumerate(sorted(buyers)[:10]):  # Top 10 buyers
            buyer_activity.append({
                "buyer_name": buyer,
                "lots_purchased": len([l for l in lots if l.get("buyer") == buyer]),
                "total_packages": sum(l.get("packages", 0) for l in lots if l.get("buyer") == buyer),
                "average_price": round(sum(l.get("price_per_kg", 0) for l in lots if l.get("buyer") == buyer) / max(1, len([l for l in lots if l.get("buyer") == buyer])), 2)
            })
        
        return {
            "active_buyers": len(buyers),
            "buyer_summary": buyer_activity,
            "participation_level": "Active" if len(buyers) > 15 else "Moderate" if len(buyers) > 8 else "Limited"
        }

    def generate_factory_performance(self, data):
        """Generate factory/garden performance data"""
        lots = data.get("auction_lots", [])
        gardens = {}
        
        for lot in lots:
            garden = lot.get("garden", f"Garden_{len(gardens) % 20 + 1}")
            price = lot.get("price_per_kg", 0)
            packages = lot.get("packages", 0)
            
            if garden not in gardens:
                gardens[garden] = {"lots": 0, "total_packages": 0, "total_value": 0, "prices": []}
            
            gardens[garden]["lots"] += 1
            gardens[garden]["total_packages"] += packages
            gardens[garden]["total_value"] += price * packages * 50  # Assume 50kg per package
            if price > 0:
                gardens[garden]["prices"].append(price)
        
        # Create factory performance summary
        performance_data = []
        for garden, data_dict in sorted(gardens.items(), key=lambda x: sum(x[1]["prices"]) / len(x[1]["prices"]) if x[1]["prices"] else 0, reverse=True)[:20]:
            avg_price = sum(data_dict["prices"]) / len(data_dict["prices"]) if data_dict["prices"] else 0
            performance_data.append({
                "garden_name": garden,
                "lots_offered": data_dict["lots"],
                "total_packages": data_dict["total_packages"],
                "average_price": round(avg_price, 2),
                "total_value": round(data_dict["total_value"], 0)
            })
        
        return {
            "top_performers": performance_data,
            "total_gardens": len(gardens)
        }

    def generate_price_quotations(self, data):
        """Generate price quotation ranges by grade"""
        price_analysis = data.get("price_analysis", {})
        by_grade = price_analysis.get("by_grade", {})
        
        quotations = []
        for grade, price_data in by_grade.items():
            quotations.append({
                "grade": grade,
                "price_range": f"{price_data['lowest']:.0f} - {price_data['highest']:.0f}",
                "average": price_data['average'],
                "lots": price_data['lot_count']
            })
        
        return {
            "grade_quotations": sorted(quotations, key=lambda x: x['average'], reverse=True)
        }

    def create_website_library(self):
        """Create market-reports-library.json for website integration"""
        library_entries = []
        
        # Look for consolidated files
        consolidated_files = list(self.consolidated_dir.glob("*_consolidated.json"))
        
        for file_path in consolidated_files:
            try:
                with open(file_path, 'r') as f:
                    data = json.load(f)
                
                metadata = data.get("metadata", {})
                summary = data.get("summary", {})
                
                # Extract location from filename
                location = file_path.stem.split('_')[0]
                
                library_entry = {
                    "title": metadata.get("report_title", f"{location} Market Report - Week 38, 2025"),
                    "description": f"Individual auction analysis for {location} Week 38, 2025 with {summary.get('total_lots', 0)} lots offered totaling {summary.get('total_offered_kg', 0):,} kg",
                    "year": metadata.get("year", 2025),
                    "week_number": metadata.get("week_number", 38),
                    "auction_centre": metadata.get("auction_centre", location),
                    "source": "TeaTrade",
                    "report_link": f"report-viewer.html?dataset={file_path.stem}",
                    "highlight": True,
                    "report_type": "individual_auction"
                }
                
                library_entries.append(library_entry)
                
            except Exception as e:
                logger.error(f"Error processing {file_path}: {e}")
        
        # Sort by year and week descending
        library_entries.sort(key=lambda x: (x["year"], x["week_number"]), reverse=True)
        
        # Save library file
        with open(self.library_file, 'w') as f:
            json.dump(library_entries, f, indent=2)
        
        logger.info(f"Created market reports library with {len(library_entries)} entries: {self.library_file}")
        return len(library_entries)

    def run_intelligence_enhancement(self):
        """Main intelligence enhancement process"""
        logger.info("Starting market intelligence enhancement...")
        
        # Find consolidated files
        consolidated_files = list(self.consolidated_dir.glob("*_consolidated.json"))
        
        if not consolidated_files:
            logger.error("No consolidated files found. Run teatrade_aggregator_robust.py first.")
            return False
        
        enhanced_count = 0
        
        for file_path in consolidated_files:
            try:
                logger.info(f"Enhancing {file_path.name}...")
                
                with open(file_path, 'r') as f:
                    data = json.load(f)
                
                # Enhance with market intelligence
                enhanced_data = self.enhance_with_market_intelligence(data)
                
                # Save enhanced version
                with open(file_path, 'w') as f:
                    json.dump(enhanced_data, f, indent=2)
                
                enhanced_count += 1
                logger.info(f"Enhanced {file_path.name}")
                
            except Exception as e:
                logger.error(f"Error enhancing {file_path}: {e}")
        
        # Create website library
        library_count = self.create_website_library()
        
        logger.info(f"Intelligence enhancement complete. Enhanced {enhanced_count} files, created library with {library_count} entries.")
        return enhanced_count > 0 and library_count > 0

if __name__ == "__main__":
    aggregator = TeaTradeIntelligenceAggregator()
    success = aggregator.run_intelligence_enhancement()
    
    if success:
        print("Intelligence enhancement completed successfully!")
    else:
        print("Intelligence enhancement failed - check logs for details")
