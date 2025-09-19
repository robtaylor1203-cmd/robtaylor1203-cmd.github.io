#!/usr/bin/env python3
"""
Properly fix the template by replacing template variables with actual DOM updates
"""

def fix_template_completely():
    """Replace the broken template variables with proper JavaScript"""
    
    # Read current template
    with open('report-viewer.html', 'r') as f:
        content = f.read()
    
    print("Fixing template variables...")
    
    # Replace the template variables with proper element targeting
    replacements = [
        ('₹${avgPrice}/kg', '<span id="price-display">Loading...</span>'),
        ('${totalVolume} kg', '<span id="volume-display">Loading...</span>'),
        ('${soldPercent}%', '<span id="sold-display">Loading...</span>'),
        ('22', '<span id="lots-display">22</span>'),  # Keep the 22 but make it updateable
    ]
    
    for old, new in replacements:
        if old in content:
            content = content.replace(old, new)
            print(f"  Replaced: {old} -> {new}")
    
    # Create proper JavaScript that updates the DOM elements
    new_script = '''
    <script>
        async function loadReportData() {
            try {
                console.log('Loading report data...');
                
                const urlParams = new URLSearchParams(window.location.search);
                const dataset = urlParams.get('dataset');
                
                if (!dataset) {
                    console.error('No dataset parameter found');
                    return;
                }

                const datasetFile = dataset.endsWith('_consolidated') ? dataset : dataset + '_consolidated';
                const dataUrl = `Data/Consolidated/${datasetFile}.json?v=${Date.now()}`;
                
                console.log('Fetching:', dataUrl);
                
                const response = await fetch(dataUrl);
                if (!response.ok) {
                    throw new Error(`HTTP ${response.status}: ${response.statusText}`);
                }

                const reportData = await response.json();
                console.log('Data loaded:', reportData);
                
                updateDisplay(reportData);

            } catch (error) {
                console.error('Error loading report:', error);
                // Show error in the displays
                document.getElementById('price-display').textContent = 'Error loading';
                document.getElementById('volume-display').textContent = 'Error loading';
                document.getElementById('sold-display').textContent = 'Error loading';
            }
        }

        function updateDisplay(data) {
            if (data.summary) {
                console.log('Updating display with summary data:', data.summary);
                
                // Update price
                const price = data.summary.auction_average_price || 0;
                const priceElement = document.getElementById('price-display');
                if (priceElement) {
                    priceElement.textContent = `₹${price.toFixed(2)}/kg`;
                    console.log('Updated price:', price);
                }

                // Update volume  
                const volume = data.summary.total_offered_kg || 0;
                const volumeElement = document.getElementById('volume-display');
                if (volumeElement) {
                    volumeElement.textContent = `${volume.toLocaleString()} kg`;
                    console.log('Updated volume:', volume);
                }

                // Update sold percentage
                const soldPercent = data.summary.percent_sold || 0;
                const soldElement = document.getElementById('sold-display');
                if (soldElement) {
                    soldElement.textContent = `${soldPercent.toFixed(1)}%`;
                    console.log('Updated sold percent:', soldPercent);
                }

                // Update lots
                const lots = data.summary.total_lots || 0;
                const lotsElement = document.getElementById('lots-display');
                if (lotsElement) {
                    lotsElement.textContent = lots.toString();
                    console.log('Updated lots:', lots);
                }

                console.log('All displays updated successfully');
            } else {
                console.error('No summary data found in response');
            }
        }

        // Load data when page loads
        document.addEventListener('DOMContentLoaded', function() {
            console.log('Page loaded, starting data load...');
            loadReportData();
        });
    </script>
    '''
    
    # Remove any existing script tags for data loading
    import re
    content = re.sub(r'<script>.*?loadReportData.*?</script>', '', content, flags=re.DOTALL)
    
    # Insert new script before closing body tag
    content = content.replace('</body>', new_script + '\n</body>')
    
    # Write fixed template
    with open('report-viewer.html', 'w') as f:
        f.write(content)
    
    print("✅ Template completely fixed!")
    print("Now the page will:")
    print("  - Load real data from JSON")
    print("  - Update specific DOM elements")
    print("  - Show proper values instead of template variables")

if __name__ == "__main__":
    print("=== COMPLETE TEMPLATE FIX ===")
    fix_template_completely()
