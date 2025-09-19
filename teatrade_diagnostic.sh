#!/bin/bash

echo "=== TEATRADE SIMPLE DIAGNOSTIC ==="
echo "Checking your data files..."
echo

# Check if we're in the right directory
if [ ! -d "Data/Consolidated" ]; then
    echo "ERROR: Data/Consolidated directory not found"
    echo "Make sure you're in the right project directory"
    exit 1
fi

echo "1. FINDING FILES WITH REAL DATA"
echo "Scanning Data/Consolidated/ directory..."

python3 << 'EOF'
import json
import os

print("Files with real market data:")
real_data_files = []

for filename in sorted(os.listdir('Data/Consolidated')):
    if filename.endswith('.json'):
        try:
            with open(f'Data/Consolidated/{filename}', 'r') as f:
                data = json.load(f)
            volume = data.get('summary', {}).get('total_offered_kg', 0)
            price = data.get('summary', {}).get('auction_average_price', 0)
            
            if volume > 100000 and price > 500:
                base_id = filename.replace('_consolidated.json', '')
                real_data_files.append(base_id)
                print(f"  REAL DATA: {base_id}")
                print(f"    Volume: {volume:,.0f} kg")
                print(f"    Price: Rs{price:.2f}")
                print()
        except Exception as e:
            pass

if not real_data_files:
    print("  NO REAL DATA FILES FOUND")
    print("  You may need to run your scrapers first")
else:
    print(f"Found {len(real_data_files)} files with real data")
    
    # Save the first real file for testing
    with open('test_file.txt', 'w') as f:
        f.write(real_data_files[0])
    print(f"Saved test file ID: {real_data_files[0]}")
EOF

echo
echo "2. CHECKING YOUR LIBRARY FILE"

if [ -f "market-reports-library.json" ]; then
    echo "Current library file exists. First few entries:"
    head -15 market-reports-library.json
else
    echo "WARNING: market-reports-library.json not found"
fi

echo
echo "3. TESTING DIRECT ACCESS"
echo "To test if your template works:"
echo "1. Start your web server with: python3 -m http.server 8000"
echo "2. Open your browser"

if [ -f "test_file.txt" ]; then
    test_id=$(cat test_file.txt)
    echo "3. Try this URL:"
    echo "   http://localhost:8000/report-viewer.html?dataset=${test_id}_consolidated"
    echo
    echo "If that URL shows real data, your problem is just the library file."
    echo "If it shows placeholder data, your template has a problem."
else
    echo "3. No test file available - run scrapers first"
fi

echo
echo "=== DIAGNOSTIC COMPLETE ==="
echo "Next: Start web server and test the URL above"
