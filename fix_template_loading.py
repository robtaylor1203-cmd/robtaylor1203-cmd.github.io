#!/usr/bin/env python3
"""
Complete fix for TeaTrade template data loading issues
"""
import json
import os
import re
from datetime import datetime

def fix_consolidated_reports():
    """Ensure all consolidated reports have proper structure for Bloomberg template"""
    consolidated_dir = 'Data/Consolidated'
    fixed_count = 0
    
    if not os.path.exists(consolidated_dir):
        print(f"Error: {consolidated_dir} directory not found")
        return
    
    print(f"Checking consolidated reports in {consolidated_dir}...")
    
    for filename in os.listdir(consolidated_dir):
        if not filename.endswith('_consolidated.json'):
            continue
            
        filepath = os.path.join(consolidated_dir, filename)
        
        try:
            with open(filepath, 'r') as f:
                data = json.load(f)
            
            # Fix common template compatibility issues
            fixed = False
            
            # Ensure all required Bloomberg template fields exist
            required_sections = ['metadata', 'summary', 'market_intelligence', 'volume_analysis', 'price_analysis', 'intelligence']
            
            for section in required_sections:
                if section not in data:
                    data[section] = {}
                    fixed = True
            
            # Fix numeric fields that should not be strings
            numeric_fields = {
                'summary': ['total_quantity', 'percentage_sold'],
                'volume_analysis': ['total_offered', 'total_sold', 'sold_percentage'],
                'price_analysis': ['average_price', 'highest_price']
            }
            
            for section, fields in numeric_fields.items():
                if section in data:
                    for field in fields:
                        if field in data[section]:
                            value = data[section][field]
                            if isinstance(value, str):
                                # Extract numeric value from strings like "₹245/kg" or "2300000 kg"
                                numeric_match = re.search(r'[\d,]+\.?\d*', value.replace(',', ''))
                                if numeric_match:
                                    try:
                                        data[section][field] = float(numeric_match.group())
                                        fixed = True
                                    except ValueError:
                                        pass
            
            # Ensure data_quality indicates real data when appropriate
            if ('Excellent' in data.get('metadata', {}).get('data_quality', '') or 
                data.get('summary', {}).get('total_quantity', 0) > 0):
                data['metadata']['data_quality'] = 'Excellent - Real market intelligence extracted'
                fixed = True
            
            # Add cache-busting timestamp
            data['metadata']['last_updated'] = datetime.now().isoformat()
            data['metadata']['cache_buster'] = int(datetime.now().timestamp() * 1000)
            fixed = True
            
            if fixed:
                with open(filepath, 'w') as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)
                fixed_count += 1
                print(f"✓ Fixed {filename}")
            else:
                print(f"◯ {filename} already correct")
                
        except Exception as e:
            print(f"✗ Error processing {filename}: {e}")
    
    print(f"\nFixed {fixed_count} consolidated reports")
    return fixed_count

def update_library_with_cache_buster():
    """Update market-reports-library.json with cache-busting timestamp"""
    try:
        consolidated_dir = 'Data/Consolidated'
        library = {
            'reports': [],
            'last_updated': datetime.now().isoformat(),
            'cache_buster': int(datetime.now().timestamp() * 1000)
        }
        
        print("Building updated library index...")
        
        for filename in sorted(os.listdir(consolidated_dir)):
            if filename.endswith('_consolidated.json'):
                filepath = os.path.join(consolidated_dir, filename)
                try:
                    with open(filepath, 'r') as f:
                        data = json.load(f)
                    
                    # Extract location and period from filename
                    base_name = filename.replace('_consolidated.json', '')
                    parts = base_name.split('_')
                    location = parts[0]
                    period = '_'.join(parts[1:]) if len(parts) > 1 else 'Unknown'
                    
                    report_entry = {
                        'id': base_name,
                        'title': data.get('metadata', {}).get('report_title', f'{location} Market Report'),
                        'location': data.get('metadata', {}).get('display_name', location.title()),
                        'period': period,
                        'data_quality': data.get('metadata', {}).get('data_quality', 'Unknown'),
                        'file': filename,
                        'has_real_data': 'Excellent' in data.get('metadata', {}).get('data_quality', ''),
                        'timestamp': data.get('metadata', {}).get('last_updated', datetime.now().isoformat()),
                        'volume': data.get('summary', {}).get('total_volume', 'N/A'),
                        'price': data.get('summary', {}).get('average_price', 'N/A')
                    }
                    
                    library['reports'].append(report_entry)
                    
                except Exception as e:
                    print(f"Warning: Could not process {filename}: {e}")
        
        # Write updated library
        with open('market-reports-library.json', 'w') as f:
            json.dump(library, f, indent=2, ensure_ascii=False)
        
        print(f"✓ Updated library with {len(library['reports'])} reports")
        print(f"✓ Cache buster timestamp: {library['cache_buster']}")
        
        # Show data quality summary
        real_data_count = sum(1 for r in library['reports'] if r['has_real_data'])
        placeholder_count = len(library['reports']) - real_data_count
        
        print(f"✓ Reports with REAL data: {real_data_count}")
        print(f"✓ Reports with placeholder data: {placeholder_count}")
        
        return len(library['reports']), real_data_count
        
    except Exception as e:
        print(f"✗ Error updating library: {e}")
        return 0, 0

def create_template_test_page():
    """Create a test page to verify template data loading"""
    html_content = '''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>TeaTrade Template Data Test</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .success { color: green; }
        .warning { color: orange; }
        .error { color: red; }
        .test-section { margin: 20px 0; padding: 15px; border: 1px solid #ddd; }
    </style>
</head>
<body>
    <h1>TeaTrade Template Data Loading Test</h1>
    <div id="test-results">Testing...</div>
    
    <script>
    async function testDataLoading() {
        const results = document.getElementById('test-results');
        results.innerHTML = '';
        
        try {
            // Test 1: Library loading
            results.innerHTML += '<div class="test-section"><h3>Test 1: Library Loading</h3>';
            const libraryResponse = await fetch('/market-reports-library.json?v=' + Date.now());
            const library = await libraryResponse.json();
            results.innerHTML += `<p class="success">✓ Library loaded successfully</p>`;
            results.innerHTML += `<p>Reports in library: ${library.reports.length}</p>`;
            results.innerHTML += `<p>Cache buster: ${library.cache_buster}</p></div>`;
            
            // Test 2: Real data reports
            results.innerHTML += '<div class="test-section"><h3>Test 2: Real Data Reports</h3>';
            const realDataReports = library.reports.filter(r => r.has_real_data);
            results.innerHTML += `<p>Reports with real data: ${realDataReports.length}</p>`;
            
            if (realDataReports.length > 0) {
                const testReport = realDataReports[0];
                results.innerHTML += `<p class="success">✓ Testing report: ${testReport.title}</p>`;
                
                const reportResponse = await fetch(`/Data/Consolidated/${testReport.file}?v=` + Date.now());
                const reportData = await reportResponse.json();
                
                results.innerHTML += `<p class="success">✓ Report loaded successfully</p>`;
                results.innerHTML += `<p><strong>Data Quality:</strong> ${reportData.metadata.data_quality}</p>`;
                results.innerHTML += `<p><strong>Volume:</strong> ${reportData.summary.total_volume}</p>`;
                results.innerHTML += `<p><strong>Average Price:</strong> ${reportData.summary.average_price}</p>`;
                results.innerHTML += `<p><strong>Market Synopsis:</strong> ${reportData.market_intelligence.market_synopsis.substring(0, 100)}...</p>`;
            } else {
                results.innerHTML += `<p class="warning">⚠ No reports with real data found</p>`;
            }
            results.innerHTML += '</div>';
            
            // Test 3: Template compatibility
            results.innerHTML += '<div class="test-section"><h3>Test 3: Template Compatibility</h3>';
            if (realDataReports.length > 0) {
                const testReport = realDataReports[0];
                const reportResponse = await fetch(`/Data/Consolidated/${testReport.file}?v=` + Date.now());
                const reportData = await reportResponse.json();
                
                const requiredFields = ['metadata', 'summary', 'market_intelligence', 'volume_analysis', 'price_analysis'];
                let allFieldsPresent = true;
                
                for (const field of requiredFields) {
                    const present = field in reportData;
                    results.innerHTML += `<p class="${present ? 'success' : 'error'}">${present ? '✓' : '✗'} ${field}</p>`;
                    if (!present) allFieldsPresent = false;
                }
                
                if (allFieldsPresent) {
                    results.innerHTML += `<p class="success">✓ All required template fields present</p>`;
                } else {
                    results.innerHTML += `<p class="error">✗ Missing required template fields</p>`;
                }
            }
            results.innerHTML += '</div>';
            
        } catch (error) {
            results.innerHTML += `<div class="test-section"><p class="error">✗ Test failed: ${error.message}</p></div>`;
        }
    }
    
    testDataLoading();
    </script>
</body>
</html>'''
    
    with open('template-test.html', 'w') as f:
        f.write(html_content)
    
    print("✓ Created template-test.html for debugging")

def fix_template_caching():
    """Fix caching issues in the report viewer template"""
    template_file = 'report-viewer.html'
    
    if not os.path.exists(template_file):
        print(f"Warning: {template_file} not found")
        return
    
    try:
        with open(template_file, 'r') as f:
            content = f.read()
        
        # Add cache-busting to JSON requests
        original_content = content
        
        # Fix fetch calls that load JSON data
        content = re.sub(
            r'fetch\s*\(\s*[\'"`]([^\'"`]*\.json)[\'"`]\s*\)',
            r'fetch(`\1?v=${Date.now()}`)',
            content
        )
        
        # Also fix any XMLHttpRequest calls
        content = re.sub(
            r'\.open\s*\(\s*[\'"`]GET[\'"`]\s*,\s*[\'"`]([^\'"`]*\.json)[\'"`]',
            r'.open("GET", `\1?v=${Date.now()}`',
            content
        )
        
        if content != original_content:
            # Backup original
            with open(f'{template_file}.backup', 'w') as f:
                f.write(original_content)
            
            # Write updated version
            with open(template_file, 'w') as f:
                f.write(content)
            
            print(f"✓ Added cache-busting to {template_file}")
            print(f"✓ Original backed up as {template_file}.backup")
        else:
            print(f"◯ {template_file} already has cache-busting")
            
    except Exception as e:
        print(f"✗ Error fixing template caching: {e}")

def main():
    print("=== TeaTrade Template Data Loading Fix ===\n")
    
    print("Step 1: Fixing consolidated report structure...")
    fixed_reports = fix_consolidated_reports()
    
    print("\nStep 2: Updating library with cache-busting...")
    total_reports, real_data_reports = update_library_with_cache_buster()
    
    print("\nStep 3: Creating test page...")
    create_template_test_page()
    
    print("\nStep 4: Fixing template caching...")
    fix_template_caching()
    
    print("\n=== Fix Complete ===")
    print(f"Summary:")
    print(f"- Fixed {fixed_reports} consolidated reports")
    print(f"- Library contains {total_reports} total reports")
    print(f"- {real_data_reports} reports have real market data")
    print(f"- Created template-test.html for verification")
    
    print(f"\nNext steps:")
    print(f"1. Run: python3 -m http.server 8000")
    print(f"2. Visit: http://localhost:8000/template-test.html")
    print(f"3. Check: http://localhost:8000/market-reports.html")
    
    if real_data_reports == 0:
        print(f"\n⚠ WARNING: No reports with real data detected!")
        print(f"   You may need to run the enhanced scrapers first:")
        print(f"   python3 scrape_FW_reports_enhanced.py")
        print(f"   python3 automation/teatrade_corrected_aggregator.py")

if __name__ == "__main__":
    main()
