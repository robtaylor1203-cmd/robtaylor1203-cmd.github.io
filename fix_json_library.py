#!/usr/bin/env python3
"""
Fix JSON library generation with better error handling
"""
import json
import os
import re
from datetime import datetime

def validate_and_fix_consolidated_files():
    """Validate and fix consolidated JSON files"""
    consolidated_dir = 'Data/Consolidated'
    fixed_files = []
    
    if not os.path.exists(consolidated_dir):
        print(f"ERROR: {consolidated_dir} not found")
        return []
    
    for filename in os.listdir(consolidated_dir):
        if not filename.endswith('_consolidated.json'):
            continue
            
        filepath = os.path.join(consolidated_dir, filename)
        
        try:
            # Try to load and validate JSON
            with open(filepath, 'r') as f:
                data = json.load(f)
            
            # Ensure basic structure
            if 'metadata' not in data:
                data['metadata'] = {}
            
            # Extract location, period, year from filename if missing from metadata
            base_name = filename.replace('_consolidated.json', '')
            parts = base_name.split('_')
            
            if len(parts) >= 2:
                location = parts[0]
                period_part = '_'.join(parts[1:])
                
                # Extract year and week/sale number
                year_match = re.search(r'(\d{4})', period_part)
                week_match = re.search(r'S(\d+)', period_part)
                
                year = int(year_match.group(1)) if year_match else 2025
                week_number = int(week_match.group(1)) if week_match else None
                
                # Update metadata with extracted info
                data['metadata'].update({
                    'location': location.lower(),
                    'display_name': location.title(),
                    'year': year,
                    'week_number': week_number,
                    'period': period_part,
                    'report_title': f"{location.title()} Market Report"
                })
            
            # Ensure required Bloomberg structure
            required_sections = ['summary', 'market_intelligence', 'volume_analysis', 
                               'price_analysis', 'intelligence', 'buyer_activity']
            
            for section in required_sections:
                if section not in data:
                    data[section] = {} if section != 'buyer_activity' else []
            
            # Add timestamp
            data['metadata']['last_updated'] = datetime.now().isoformat()
            
            # Write back with proper formatting
            with open(filepath, 'w') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            
            fixed_files.append({
                'filename': filename,
                'location': location.title(),
                'year': year,
                'week_number': week_number,
                'data_quality': data.get('metadata', {}).get('data_quality', 'Unknown'),
                'has_real_data': 'Excellent' in data.get('metadata', {}).get('data_quality', '')
            })
            
            print(f"✓ Fixed {filename}")
            
        except json.JSONDecodeError as e:
            print(f"✗ JSON error in {filename}: {e}")
        except Exception as e:
            print(f"✗ Error processing {filename}: {e}")
    
    return fixed_files

def create_simple_library(files_info):
    """Create a simple, valid JSON library"""
    
    # Sort by year (desc) then week (desc)
    sorted_files = sorted(files_info, key=lambda x: (
        x['year'],
        x['week_number'] if x['week_number'] else 0
    ), reverse=True)
    
    library = []
    
    for file_info in sorted_files:
        # Determine title and description
        week_text = f"Week {file_info['week_number']}" if file_info['week_number'] else "Report"
        title = f"{file_info['location']} Market Report - {week_text}, {file_info['year']}"
        
        if file_info['has_real_data']:
            description = f"REAL DATA - Enhanced market intelligence with actual trading data. Quality: {file_info['data_quality']}"
            source = "TeaTrade Enhanced Scrapers (Real Data)"
        else:
            description = f"Placeholder data - {file_info['data_quality']}"
            source = "TeaTrade Corrected (Limited Data)"
        
        base_id = file_info['filename'].replace('_consolidated.json', '')
        
        entry = {
            "title": title,
            "description": description,
            "year": file_info['year'],
            "week_number": file_info['week_number'],
            "auction_centre": file_info['location'],
            "source": source,
            "report_link": f"report-viewer.html?dataset={base_id}",
            "highlight": file_info['has_real_data'],
            "data_quality": file_info['data_quality']
        }
        
        library.append(entry)
    
    return library

def main():
    print("=== Fixing JSON Library ===\n")
    
    # Step 1: Fix consolidated files
    print("Step 1: Validating and fixing consolidated files...")
    files_info = validate_and_fix_consolidated_files()
    
    if not files_info:
        print("ERROR: No valid consolidated files found!")
        return
    
    print(f"✓ Fixed {len(files_info)} consolidated files")
    real_data_count = sum(1 for f in files_info if f['has_real_data'])
    print(f"✓ {real_data_count} files contain real data")
    
    # Step 2: Create simple library
    print("\nStep 2: Creating simple library...")
    library = create_simple_library(files_info)
    
    # Backup existing library
    if os.path.exists('market-reports-library.json'):
        backup_name = f'market-reports-library.json.broken.{int(datetime.now().timestamp())}'
        os.rename('market-reports-library.json', backup_name)
        print(f"✓ Backed up broken library as {backup_name}")
    
    # Write new library with error handling
    try:
        with open('market-reports-library.json', 'w') as f:
            json.dump(library, f, indent=2, ensure_ascii=False)
        print(f"✓ Created valid library with {len(library)} reports")
        
        # Validate the new library immediately
        with open('market-reports-library.json', 'r') as f:
            test_load = json.load(f)
        print(f"✓ Library validation successful - {len(test_load)} entries")
        
    except Exception as e:
        print(f"✗ Error creating library: {e}")
        return
    
    print(f"\n=== Fix Complete ===")
    print(f"Summary:")
    print(f"- Fixed {len(files_info)} consolidated files")
    print(f"- Created library with {len(library)} reports")
    print(f"- {real_data_count} reports have real market data")
    
    print(f"\nTest commands:")
    print(f"python3 -c \"import json; print('Library valid:', bool(json.load(open('market-reports-library.json'))))\"")
    print(f"python3 -m http.server 8000")

if __name__ == "__main__":
    main()
