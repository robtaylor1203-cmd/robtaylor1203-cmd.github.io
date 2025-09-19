#!/usr/bin/env python3
"""
Fix the original report-viewer.html to use real data instead of hardcoded values
This preserves the Bloomberg design but fixes the data loading
"""

import re
import shutil
from datetime import datetime

def backup_original():
    """Backup the current template"""
    backup_name = f"report-viewer-backup-{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
    shutil.copy('report-viewer.html', backup_name)
    print(f"✅ Backed up original template as: {backup_name}")
    return backup_name

def fix_template():
    """Fix the hardcoded values in the template"""
    
    # Read the original template
    with open('report-viewer.html', 'r') as f:
        content = f.read()
    
    print("Analyzing template for hardcoded values...")
    
    # Common hardcoded patterns to look for and fix
    fixes_made = 0
    
    # Replace hardcoded price values
    if '₹245' in content or '245/kg' in content:
        content = re.sub(r'₹245[/\w]*', '₹${avgPrice}/kg', content)
        content = re.sub(r'245[/\w]*', '${avgPrice}', content)
        fixes_made += 1
        print("  Fixed hardcoded price value")
    
    # Replace hardcoded volume values  
    if '2.3M kg' in content or '2300000' in content:
        content = re.sub(r'2\.3M kg', '${totalVolume} kg', content)
        content = re.sub(r'2300000', '${totalVolumeRaw}', content)
        fixes_made += 1
        print("  Fixed hardcoded volume value")
    
    # Replace hardcoded percentage values
    if '87%' in content:
        content = re.sub(r'87%', '${soldPercent}%', content)
        fixes_made += 1
        print("  Fixed hardcoded sold percentage")
    
    # Look for JavaScript section and ensure data loading is correct
    if 'renderReport' in content or 'loadReportData' in content:
        print("  Template already has data loading functions")
    else:
        # Add basic data loading if missing
        script_insertion = '''
    <script>
        async function loadReportData() {
            try {
                const urlParams = new URLSearchParams(window.location.search);
                const dataset = urlParams.get('dataset');
                
                if (!dataset) {
                    console.error('No dataset parameter found');
                    return;
                }

                const datasetFile = dataset.endsWith('_consolidated') ? dataset : dataset + '_consolidated';
                const dataUrl = `Data/Consolidated/${datasetFile}.json?v=${Date.now()}`;
                
                const response = await fetch(dataUrl);
                if (!response.ok) {
                    throw new Error(`HTTP ${response.status}: ${response.statusText}`);
                }

                const reportData = await response.json();
                updateDisplay(reportData);

            } catch (error) {
                console.error('Error loading report:', error);
            }
        }

        function updateDisplay(data) {
            if (data.summary) {
                // Update price
                const priceElements = document.querySelectorAll('[data-field="price"], #avgPrice, .price-value');
                const price = data.summary.auction_average_price || 0;
                priceElements.forEach(el => {
                    el.textContent = `₹${price.toFixed(2)}/kg`;
                });

                // Update volume  
                const volumeElements = document.querySelectorAll('[data-field="volume"], #totalVolume, .volume-value');
                const volume = data.summary.total_offered_kg || 0;
                volumeElements.forEach(el => {
                    el.textContent = `${volume.toLocaleString()} kg`;
                });

                // Update lots
                const lotsElements = document.querySelectorAll('[data-field="lots"], #lotsOffered, .lots-value');
                const lots = data.summary.total_lots || 0;
                lotsElements.forEach(el => {
                    el.textContent = lots.toString();
                });

                // Update sold percentage
                const soldElements = document.querySelectorAll('[data-field="sold"], #soldPercent, .sold-value');
                const soldPercent = data.summary.percent_sold || 0;
                soldElements.forEach(el => {
                    el.textContent = `${soldPercent.toFixed(1)}%`;
                });

                // Update title
                const titleElements = document.querySelectorAll('[data-field="title"], #reportTitle, .report-title');
                const title = data.metadata?.report_title || 'Market Report';
                titleElements.forEach(el => {
                    el.textContent = title;
                });

                console.log('Display updated with real data');
            }
        }

        // Load data when page loads
        document.addEventListener('DOMContentLoaded', loadReportData);
    </script>
    '''
        
        # Insert before closing </body> tag
        content = content.replace('</body>', script_insertion + '\n</body>')
        fixes_made += 1
        print("  Added data loading JavaScript")
    
    if fixes_made == 0:
        print("  No obvious hardcoded values found to fix")
        print("  The template might need manual inspection")
        return False
    
    # Write the fixed template
    with open('report-viewer.html', 'w') as f:
        f.write(content)
    
    print(f"✅ Applied {fixes_made} fixes to template")
    return True

def main():
    print("=== FIXING ORIGINAL TEMPLATE ===")
    
    # Check if template exists
    try:
        with open('report-viewer.html', 'r') as f:
            pass
    except FileNotFoundError:
        print("❌ report-viewer.html not found")
        return
    
    # Backup original
    backup_file = backup_original()
    
    # Apply fixes
    success = fix_template()
    
    if success:
        print("\n✅ Template fixed!")
        print("Test with: http://localhost:8000/report-viewer.html?dataset=Colombo_S27_2025_consolidated")
        print(f"Original backed up as: {backup_file}")
    else:
        print("\n⚠️  Automatic fix incomplete")
        print("You may need to manually edit the template")
        print("Look for hardcoded values like:")
        print("  - ₹245/kg (should use real price)")
        print("  - 2.3M kg (should use real volume)") 
        print("  - 87% (should use real sold percentage)")

if __name__ == "__main__":
    main()
