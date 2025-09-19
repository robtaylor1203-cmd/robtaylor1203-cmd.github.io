#!/usr/bin/env python3
"""
Complete library rebuilder that fixes the disconnect between scrapers and templates
"""
import json
import os
import re
from datetime import datetime

def analyze_consolidated_files():
    """Analyze what consolidated files actually exist"""
    consolidated_dir = 'Data/Consolidated'
    files_found = []
    
    if not os.path.exists(consolidated_dir):
        print(f"ERROR: {consolidated_dir} directory not found!")
        return []
    
    for filename in os.listdir(consolidated_dir):
        if filename.endswith('_consolidated.json'):
            filepath = os.path.join(consolidated_dir, filename)
            try:
                with open(filepath, 'r') as f:
                    data = json.load(f)
                
                # Extract info from the actual file
                file_info = {
                    'filename': filename,
                    'base_id': filename.replace('_consolidated.json', ''),
                    'data_quality': data.get('metadata', {}).get('data_quality', 'Unknown'),
                    'location': data.get('metadata', {}).get('location', 'Unknown'),
                    'display_name': data.get('metadata', {}).get('display_name', 'Unknown'),
                    'period': data.get('metadata', {}).get('period', 'Unknown'),
                    'week_number': data.get('metadata', {}).get('week_number', 'Unknown'),
                    'year': data.get('metadata', {}).get('year', 'Unknown'),
                    'report_title': data.get('metadata', {}).get('report_title', 'Market Report'),
                    'has_real_data': 'Excellent' in data.get('metadata', {}).get('data_quality', ''),
                    'total_volume': data.get('summary', {}).get('total_volume', 'N/A'),
                    'average_price': data.get('summary', {}).get('average_price', 'N/A'),
                    'file_size': os.path.getsize(filepath)
                }
                files_found.append(file_info)
                
            except Exception as e:
                print(f"Warning: Could not read {filename}: {e}")
    
    return files_found

def create_proper_library(files_info):
    """Create a proper library that matches actual files"""
    
    library = {
        'reports': [],
        'last_updated': datetime.now().isoformat(),
        'cache_buster': int(datetime.now().timestamp() * 1000),
        'total_reports': len(files_info),
        'real_data_reports': sum(1 for f in files_info if f['has_real_data'])
    }
    
    # Sort by year (desc) then week number (desc)
    sorted_files = sorted(files_info, key=lambda x: (
        int(x['year']) if str(x['year']).isdigit() else 0,
        int(x['week_number']) if str(x['week_number']).isdigit() else 0
    ), reverse=True)
    
    for file_info in sorted_files:
        # Create proper description
        if file_info['has_real_data']:
            description = f"REAL DATA - {file_info['report_title']}. Volume: {file_info['total_volume']}, Price: {file_info['average_price']}. Quality: {file_info['data_quality']}"
        else:
            description = f"Placeholder data - {file_info['report_title']}. {file_info['data_quality']}"
        
        # Create proper period text
        period_text = f"Week {file_info['week_number']}" if str(file_info['week_number']).isdigit() else file_info['period']
        
        report_entry = {
            'title': f"{file_info['display_name']} Market Report - {period_text}, {file_info['year']}",
            'description': description,
            'year': int(file_info['year']) if str(file_info['year']).isdigit() else 2025,
            'week_number': int(file_info['week_number']) if str(file_info['week_number']).isdigit() else None,
            'auction_centre': file_info['display_name'],
            'source': 'TeaTrade Enhanced Scrapers',
            'report_link': f"report-viewer.html?dataset={file_info['base_id']}",
            'highlight': file_info['has_real_data'],
            'data_quality': file_info['data_quality'],
            'file_size_mb': round(file_info['file_size'] / 1024 / 1024, 2)
        }
        
        library['reports'].append(report_entry)
    
    return library

def convert_data_to_bloomberg_format(files_info):
    """Convert aggregator output to Bloomberg template format"""
    
    conversions_made = 0
    
    for file_info in files_info:
        filepath = os.path.join('Data/Consolidated', file_info['filename'])
        
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)
            
            # Check if already in Bloomberg format
            if 'volume_analysis' in data and 'buyer_activity' in data:
                print(f"◯ {file_info['filename']} already in Bloomberg format")
                continue
            
            # Convert to Bloomberg format
            bloomberg_data = convert_single_report(data)
            
            # Write back
            with open(filepath, 'w') as f:
                json.dump(bloomberg_data, f, indent=2, ensure_ascii=False)
            
            conversions_made += 1
            print(f"✓ Converted {file_info['filename']} to Bloomberg format")
            
        except Exception as e:
            print(f"✗ Error converting {file_info['filename']}: {e}")
    
    return conversions_made

def convert_single_report(original_data):
    """Convert a single report to Bloomberg template format"""
    
    # Create Bloomberg-compatible structure
    bloomberg_data = {
        'metadata': original_data.get('metadata', {}),
        'summary': {},
        'market_intelligence': original_data.get('market_intelligence', {}),
        'volume_analysis': {},
        'price_analysis': original_data.get('price_analysis', {}),
        'intelligence': original_data.get('intelligence', {}),
        'buyer_activity': [],
        'comparative_analysis': {},
        'factory_performance': [],
        'price_quotations': [],
        'forecast_analysis': [],
        'weather_analysis': [],
        'upcoming_offerings': {},
        'international_summaries': []
    }
    
    # Convert summary data
    if 'summary' in original_data:
        orig_summary = original_data['summary']
        bloomberg_data['summary'] = {
            'total_offered_kg': extract_numeric(orig_summary.get('total_volume', '0')),
            'total_sold_kg': extract_numeric(orig_summary.get('total_quantity', '0')),
            'total_lots': extract_numeric(orig_summary.get('total_lots', '0')),
            'auction_average_price': extract_numeric(orig_summary.get('average_price', '0')),
            'percent_sold': extract_numeric(orig_summary.get('sold_percentage', '0')),
            'percent_unsold': 100 - extract_numeric(orig_summary.get('sold_percentage', '0')),
            'commentary_synthesized': orig_summary.get('market_synopsis', 'Market commentary not available')
        }
    
    # Convert volume analysis
    if 'volume_analysis' in original_data:
        orig_vol = original_data['volume_analysis']
        bloomberg_data['volume_analysis'] = {
            'total_offered': orig_vol.get('total_offered', 0),
            'total_sold': orig_vol.get('total_sold', 0),
            'sold_percentage': orig_vol.get('sold_percentage', 0),
            'by_grade_detailed': []  # Template expects this structure
        }
    
    # Ensure metadata has required fields
    if 'currency' not in bloomberg_data['metadata']:
        bloomberg_data['metadata']['currency'] = 'USD'  # Default
    
    if 'year' not in bloomberg_data['metadata']:
        bloomberg_data['metadata']['year'] = 2025  # Default
    
    # Add cache busting
    bloomberg_data['metadata']['last_updated'] = datetime.now().isoformat()
    bloomberg_data['metadata']['template_format'] = 'Bloomberg Compatible v1.0'
    
    return bloomberg_data

def extract_numeric(value):
    """Extract numeric value from strings like '₹245/kg' or '2300000 kg'"""
    if isinstance(value, (int, float)):
        return value
    
    if isinstance(value, str):
        # Remove common currency symbols and units
        clean_value = re.sub(r'[₹$£€,/kg\s]', '', value)
        try:
            return float(clean_value)
        except ValueError:
            return 0
    
    return 0

def main():
    print("=== Complete Library Rebuilder ===\n")
    
    # Step 1: Analyze what we actually have
    print("Step 1: Analyzing consolidated files...")
    files_info = analyze_consolidated_files()
    
    if not files_info:
        print("ERROR: No consolidated files found!")
        print("Make sure you've run: python3 automation/teatrade_corrected_aggregator.py")
        return
    
    print(f"Found {len(files_info)} consolidated files")
    real_data_count = sum(1 for f in files_info if f['has_real_data'])
    print(f"Files with real data: {real_data_count}/{len(files_info)}")
    
    # Step 2: Convert to Bloomberg format
    print(f"\nStep 2: Converting to Bloomberg format...")
    conversions = convert_data_to_bloomberg_format(files_info)
    print(f"Converted {conversions} files to Bloomberg format")
    
    # Step 3: Rebuild library
    print(f"\nStep 3: Creating proper library...")
    library = create_proper_library(files_info)
    
    # Backup old library
    if os.path.exists('market-reports-library.json'):
        backup_name = f'market-reports-library.json.backup.{int(datetime.now().timestamp())}'
        os.rename('market-reports-library.json', backup_name)
        print(f"✓ Backed up old library as {backup_name}")
    
    # Write new library
    with open('market-reports-library.json', 'w') as f:
        json.dump(library['reports'], f, indent=2, ensure_ascii=False)
    
    print(f"✓ Created new library with {len(library['reports'])} reports")
    
    # Step 4: Create debug info
    with open('library-debug-info.json', 'w') as f:
        json.dump({
            'rebuild_timestamp': datetime.now().isoformat(),
            'total_files_found': len(files_info),
            'real_data_files': real_data_count,
            'conversions_made': conversions,
            'files_analyzed': [f['filename'] for f in files_info],
            'real_data_files_list': [f['filename'] for f in files_info if f['has_real_data']]
        }, f, indent=2)
    
    print(f"✓ Created library-debug-info.json for troubleshooting")
    
    print(f"\n=== Rebuild Complete ===")
    print(f"Results:")
    print(f"- Analyzed {len(files_info)} consolidated files")
    print(f"- {real_data_count} files contain real market data")
    print(f"- Converted {conversions} files to Bloomberg format")
    print(f"- Created library with {len(library['reports'])} entries")
    
    print(f"\nNext steps:")
    print(f"1. Test: python3 -m http.server 8000")
    print(f"2. Visit: http://localhost:8000/market-reports.html")
    print(f"3. Click on reports marked 'REAL DATA' to see actual market data")
    
    if real_data_count == 0:
        print(f"\n⚠ WARNING: No real data found!")
        print(f"   Run enhanced scrapers first:")
        print(f"   python3 scrape_FW_reports_enhanced.py")
        print(f"   python3 automation/teatrade_corrected_aggregator.py")
        print(f"   python3 rebuild_complete_library.py")

if __name__ == "__main__":
    main()
