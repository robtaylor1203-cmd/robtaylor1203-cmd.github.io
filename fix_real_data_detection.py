#!/usr/bin/env python3
"""
Fix real data detection in library generation
"""
import json
import os
from datetime import datetime

def analyze_real_data():
    """Properly analyze which files have real data"""
    consolidated_dir = 'Data/Consolidated'
    analysis = {}
    
    for filename in os.listdir(consolidated_dir):
        if not filename.endswith('_consolidated.json'):
            continue
            
        filepath = os.path.join(consolidated_dir, filename)
        
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)
            
            # Multiple ways to detect real data
            has_real_data = False
            data_quality = data.get('metadata', {}).get('data_quality', 'Unknown')
            
            # Method 1: Check data quality string
            if 'Excellent' in data_quality and 'Real market intelligence' in data_quality:
                has_real_data = True
            
            # Method 2: Check for substantial volume data
            summary = data.get('summary', {})
            volume = summary.get('total_offered_kg', 0) or summary.get('total_volume', 0)
            if isinstance(volume, str):
                # Extract number from strings like "415,419 kg"
                import re
                volume_match = re.search(r'[\d,]+', str(volume).replace(',', ''))
                volume = int(volume_match.group()) if volume_match else 0
            
            if volume > 50000:  # Substantial volume indicates real data
                has_real_data = True
            
            # Method 3: Check for detailed price data
            price = summary.get('auction_average_price', 0) or summary.get('average_price', 0)
            if isinstance(price, str):
                import re
                price_match = re.search(r'[\d.]+', str(price))
                price = float(price_match.group()) if price_match else 0
            
            if price > 100:  # Reasonable price indicates real data
                has_real_data = True
            
            # Method 4: Check market intelligence content
            market_intel = data.get('market_intelligence', {})
            synopsis = market_intel.get('market_synopsis', '')
            if len(synopsis) > 100 and not synopsis.startswith('Market conditions for'):
                has_real_data = True
            
            analysis[filename] = {
                'has_real_data': has_real_data,
                'data_quality': data_quality,
                'volume': volume,
                'price': price,
                'synopsis_length': len(synopsis),
                'detection_reasons': []
            }
            
            # Track why it was detected as real data
            if has_real_data:
                reasons = []
                if 'Excellent' in data_quality:
                    reasons.append('Excellent data quality')
                if volume > 50000:
                    reasons.append(f'Substantial volume: {volume:,} kg')
                if price > 100:
                    reasons.append(f'Real price data: {price}')
                if len(synopsis) > 100:
                    reasons.append(f'Detailed synopsis: {len(synopsis)} chars')
                analysis[filename]['detection_reasons'] = reasons
            
        except Exception as e:
            print(f"Error analyzing {filename}: {e}")
            analysis[filename] = {
                'has_real_data': False,
                'data_quality': 'Error',
                'error': str(e)
            }
    
    return analysis

def create_corrected_library(analysis):
    """Create library with correct real data detection"""
    
    # Sort files by real data first, then by date
    files_list = []
    for filename, info in analysis.items():
        base_id = filename.replace('_consolidated.json', '')
        parts = base_id.split('_')
        
        if len(parts) >= 2:
            location = parts[0].title()
            period = '_'.join(parts[1:])
            
            import re
            week_match = re.search(r'S(\d+)', period)
            year_match = re.search(r'(\d{4})', period)
            
            week_num = int(week_match.group(1)) if week_match else 0
            year = int(year_match.group(1)) if year_match else 2025
            
            files_list.append({
                'filename': filename,
                'base_id': base_id,
                'location': location,
                'week_num': week_num,
                'year': year,
                'period': period,
                'has_real_data': info['has_real_data'],
                'data_quality': info['data_quality'],
                'volume': info.get('volume', 0),
                'price': info.get('price', 0),
                'reasons': info.get('detection_reasons', [])
            })
    
    # Sort: Real data first, then by year desc, then by week desc
    files_list.sort(key=lambda x: (
        not x['has_real_data'],  # Real data first (False sorts before True)
        -x['year'],              # Year descending
        -x['week_num']           # Week descending
    ))
    
    library = []
    
    for file_info in files_list:
        if file_info['has_real_data']:
            title = f"🟢 {file_info['location']} Market Report - Week {file_info['week_num']}, {file_info['year']}"
            description = f"REAL DATA - Enhanced scrapers extracted actual market intelligence. "
            if file_info['volume'] > 0:
                description += f"Volume: {file_info['volume']:,} kg. "
            if file_info['price'] > 0:
                description += f"Avg Price: {file_info['price']:.2f}. "
            description += f"Quality: {file_info['data_quality']}"
            source = "TeaTrade Enhanced Scrapers (REAL DATA)"
            highlight = True
        else:
            title = f"⚪ {file_info['location']} Market Report - Week {file_info['week_num']}, {file_info['year']}"
            description = f"Placeholder data - {file_info['data_quality']}"
            source = "TeaTrade Corrected (Limited Data)"
            highlight = False
        
        entry = {
            "title": title,
            "description": description,
            "year": file_info['year'],
            "week_number": file_info['week_num'],
            "auction_centre": file_info['location'],
            "source": source,
            "report_link": f"report-viewer.html?dataset={file_info['base_id']}",
            "highlight": highlight,
            "data_quality": file_info['data_quality']
        }
        
        library.append(entry)
    
    return library, files_list

def main():
    print("=== Fixing Real Data Detection ===\n")
    
    # Analyze files properly
    print("Step 1: Analyzing files for real data...")
    analysis = analyze_real_data()
    
    real_count = sum(1 for info in analysis.values() if info['has_real_data'])
    print(f"✓ Analyzed {len(analysis)} files")
    print(f"✓ Found {real_count} files with real data")
    
    # Show which files have real data
    print(f"\nFiles with REAL DATA:")
    for filename, info in analysis.items():
        if info['has_real_data']:
            print(f"  ✓ {filename}")
            for reason in info.get('detection_reasons', []):
                print(f"    - {reason}")
    
    # Create corrected library
    print(f"\nStep 2: Creating corrected library...")
    library, files_list = create_corrected_library(analysis)
    
    # Backup existing
    if os.path.exists('market-reports-library.json'):
        backup_name = f'market-reports-library.json.backup.{int(datetime.now().timestamp())}'
        os.rename('market-reports-library.json', backup_name)
        print(f"✓ Backed up library as {backup_name}")
    
    # Write corrected library
    with open('market-reports-library.json', 'w') as f:
        json.dump(library, f, indent=2, ensure_ascii=False)
    
    # Save analysis for debugging
    with open('real-data-analysis.json', 'w') as f:
        json.dump({
            'analysis': analysis,
            'summary': {
                'total_files': len(analysis),
                'real_data_files': real_count,
                'real_data_filenames': [f for f, info in analysis.items() if info['has_real_data']]
            }
        }, f, indent=2)
    
    print(f"✓ Created corrected library with {len(library)} reports")
    print(f"✓ {real_count} reports marked as REAL DATA")
    print(f"✓ Saved analysis to real-data-analysis.json")
    
    print(f"\n=== Fix Complete ===")
    print(f"Summary:")
    print(f"- {real_count}/{len(analysis)} files have real market data")
    print(f"- Library now correctly identifies real vs placeholder data")
    print(f"- Real data reports are marked with 🟢 and highlight=true")
    
    print(f"\nTest the fix:")
    print(f"1. Visit: http://localhost:8000/market-reports.html")
    print(f"2. Look for reports with 🟢 (these have real data)")
    print(f"3. Click on them to see actual market intelligence")

if __name__ == "__main__":
    main()
