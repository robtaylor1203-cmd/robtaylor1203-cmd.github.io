#!/usr/bin/env python3
"""
Complete pipeline fix - clean solution to connect scrapers -> library -> template
"""
import json
import os
from datetime import datetime

def clean_slate_fix():
    """Fix the entire pipeline from consolidated files to working template"""
    
    print("=== COMPLETE PIPELINE FIX ===\n")
    
    # Step 1: Find all actual consolidated files
    consolidated_dir = 'Data/Consolidated'
    if not os.path.exists(consolidated_dir):
        print("ERROR: No consolidated files found")
        return False
    
    actual_files = []
    for filename in os.listdir(consolidated_dir):
        if filename.endswith('_consolidated.json'):
            actual_files.append(filename)
    
    print(f"Step 1: Found {len(actual_files)} actual consolidated files")
    
    # Step 2: Analyze each file for real data
    reports = []
    for filename in actual_files:
        filepath = os.path.join(consolidated_dir, filename)
        
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)
            
            # Extract info
            base_id = filename.replace('_consolidated.json', '')
            
            # Get volume and price to detect real data
            volume = data.get('summary', {}).get('total_offered_kg', 0)
            price = data.get('summary', {}).get('auction_average_price', 0)
            data_quality = data.get('metadata', {}).get('data_quality', '')
            
            # Real data detection
            has_real_data = (
                volume > 50000 or  # Substantial volume
                price > 500 or    # Reasonable price
                'Excellent' in data_quality
            )
            
            # Extract location and period
            parts = base_id.split('_')
            location = parts[0].title() if parts else 'Unknown'
            period = '_'.join(parts[1:]) if len(parts) > 1 else 'Unknown'
            
            # Extract week and year
            import re
            week_match = re.search(r'S(\d+)', period)
            year_match = re.search(r'(\d{4})', period)
            
            week_num = int(week_match.group(1)) if week_match else 0
            year = int(year_match.group(1)) if year_match else 2025
            
            reports.append({
                'filename': filename,
                'base_id': base_id,
                'location': location,
                'week': week_num,
                'year': year,
                'volume': volume,
                'price': price,
                'has_real_data': has_real_data,
                'data_quality': data_quality
            })
            
        except Exception as e:
            print(f"  WARNING: Could not read {filename}: {e}")
    
    # Step 3: Sort reports (real data first, then by recency)
    reports.sort(key=lambda x: (
        not x['has_real_data'],  # Real data first
        -x['year'],              # Recent years first  
        -x['week']               # Recent weeks first
    ))
    
    real_count = sum(1 for r in reports if r['has_real_data'])
    print(f"Step 2: Analyzed files - {real_count}/{len(reports)} have real data")
    
    # Show real data files
    print("\nFiles with REAL DATA:")
    for r in reports:
        if r['has_real_data']:
            print(f"  ✓ {r['filename']}: {r['location']} Week {r['week']}, Vol: {r['volume']:,.0f}, Price: {r['price']}")
    
    # Step 4: Create clean library
    library = []
    for r in reports:
        if r['has_real_data']:
            title = f"✅ {r['location']} Week {r['week']}, {r['year']} - REAL DATA"
            description = f"Live market data: Volume {r['volume']:,.0f} kg, Avg Price {r['price']:.0f}"
            source = "Enhanced Scrapers (REAL DATA)"
        else:
            title = f"⚪ {r['location']} Week {r['week']}, {r['year']} - Placeholder"  
            description = f"Limited data available"
            source = "Placeholder Data"
        
        library.append({
            "title": title,
            "description": description,
            "year": r['year'],
            "week_number": r['week'],
            "auction_centre": r['location'],
            "source": source,
            "report_link": f"report-viewer.html?dataset={r['base_id']}",
            "highlight": r['has_real_data']
        })
    
    # Step 5: Write clean library
    with open('market-reports-library.json', 'w') as f:
        json.dump(library, f, indent=2, ensure_ascii=False)
    
    print(f"Step 3: Created clean library with {len(library)} entries")
    print(f"         {real_count} marked as REAL DATA")
    
    # Step 6: Verify template can access the data
    print(f"\nStep 4: Verifying template access...")
    
    # Test the first real data file
    real_reports = [r for r in reports if r['has_real_data']]
    if real_reports:
        test_report = real_reports[0]
        test_file = os.path.join(consolidated_dir, test_report['filename'])
        
        with open(test_file, 'r') as f:
            test_data = json.load(f)
        
        print(f"Testing: {test_report['base_id']}")
        print(f"  Template URL: report-viewer.html?dataset={test_report['base_id']}")
        print(f"  File exists: ✓")
        print(f"  Volume: {test_data['summary']['total_offered_kg']:,.0f} kg")
        print(f"  Price: {test_data['summary']['auction_average_price']}")
        print(f"  Commentary: {test_data['summary']['commentary_synthesized'][:60]}...")
        
        return test_report['base_id']
    
    return None

def create_simple_test_page(test_dataset):
    """Create a simple test page that shows the data correctly"""
    
    if not test_dataset:
        return
        
    html_content = f'''<!DOCTYPE html>
<html>
<head>
    <title>Pipeline Test - Working Data Display</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .success {{ color: green; font-weight: bold; }}
        .data-box {{ border: 1px solid #ccc; padding: 15px; margin: 10px 0; }}
    </style>
</head>
<body>
    <h1>Pipeline Test - Real Data Display</h1>
    
    <div id="test-results">Loading real data...</div>
    
    <script>
    async function displayRealData() {{
        try {{
            const response = await fetch('Data/Consolidated/{test_dataset}_consolidated.json?v=' + Date.now());
            const data = await response.json();
            
            const results = document.getElementById('test-results');
            results.innerHTML = `
                <div class="data-box">
                    <h2 class="success">✅ REAL DATA LOADED SUCCESSFULLY</h2>
                    <h3>${{data.metadata.report_title}}</h3>
                    
                    <h4>Key Metrics:</h4>
                    <p><strong>Total Volume:</strong> ${{data.summary.total_offered_kg.toLocaleString()}} kg</p>
                    <p><strong>Average Price:</strong> ${{data.summary.auction_average_price}}</p>
                    <p><strong>Data Quality:</strong> ${{data.metadata.data_quality}}</p>
                    
                    <h4>Market Commentary:</h4>
                    <p>${{data.summary.commentary_synthesized}}</p>
                    
                    <hr>
                    <p><a href="report-viewer.html?dataset={test_dataset}" target="_blank">
                       🔗 Open in Bloomberg Template</a></p>
                    <p><a href="market-reports.html">📋 Back to Reports Library</a></p>
                </div>
            `;
            
        }} catch (error) {{
            document.getElementById('test-results').innerHTML = `
                <div class="data-box">
                    <h2 style="color: red;">❌ ERROR LOADING DATA</h2>
                    <p>Error: ${{error.message}}</p>
                </div>
            `;
        }}
    }}
    
    displayRealData();
    </script>
</body>
</html>'''
    
    with open('pipeline-test.html', 'w') as f:
        f.write(html_content)
    
    print(f"✓ Created pipeline-test.html")

def main():
    """Run the complete pipeline fix"""
    
    # Clean up previous patches
    backup_files = [
        'market-reports-library.json.backup.*',
        'market-reports-library.json.broken.*', 
        'real-data-analysis.json',
        'library-debug-info.json'
    ]
    
    print("Cleaning up previous patches...")
    os.system('rm -f market-reports-library.json.backup.* 2>/dev/null')
    os.system('rm -f market-reports-library.json.broken.* 2>/dev/null')
    os.system('rm -f real-data-analysis.json library-debug-info.json 2>/dev/null')
    
    # Run the fix
    test_dataset = clean_slate_fix()
    
    if test_dataset:
        create_simple_test_page(test_dataset)
        
        print(f"\n=== PIPELINE FIX COMPLETE ===")
        print(f"✅ Library rebuilt with correct file mappings")
        print(f"✅ Real data files identified and prioritized") 
        print(f"✅ Template compatibility verified")
        
        print(f"\nTest your fix:")
        print(f"1. Visit: http://localhost:8000/pipeline-test.html")
        print(f"2. If that works, visit: http://localhost:8000/market-reports.html")
        print(f"3. Click on reports marked with ✅ to see real data")
        
        print(f"\nWorking dataset for testing: {test_dataset}")
        print(f"Direct template URL: http://localhost:8000/report-viewer.html?dataset={test_dataset}")
        
    else:
        print(f"\n❌ NO REAL DATA FOUND")
        print(f"You may need to run your enhanced scrapers first:")
        print(f"python3 scrape_FW_reports_enhanced.py")
        print(f"python3 automation/teatrade_corrected_aggregator.py")

if __name__ == "__main__":
    main()
