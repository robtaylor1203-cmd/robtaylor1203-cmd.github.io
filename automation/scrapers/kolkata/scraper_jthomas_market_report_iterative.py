import requests
import json
import datetime
from pathlib import Path
from bs4 import BeautifulSoup

# Define directories
REPO_BASE = Path(__file__).resolve().parent.parent.parent
OUTPUT_DIR = REPO_BASE / "source_reports" / "kolkata"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# URLs based on the POST-back mechanism
BASE_URL = "https://jthomasindia.com/market_report.php"
DATA_STREAM_URL = "https://jthomasindia.com/show_market_report.php"

# Mimic the headers a real browser sends
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Referer': BASE_URL,
    'X-Requested-With': 'XMLHttpRequest' 
}

def scrape_full_market_report_http():
    # Renamed function to reflect the robust technique
    print(f"Starting J Thomas Market Report Iterative scraper (Direct HTTP Simulation Mode)...")
    
    all_commentaries = []
    latest_sale_no = "Unknown"
    session = requests.Session()
    session.headers.update(HEADERS)

    # Kolkata Centre ID is '5'
    KOLKATA_CENTRE_ID = "5"

    try:
        # --- 1. Initial Page Visit ---
        print("Establishing session...")
        response = session.get(BASE_URL, timeout=60)
        response.raise_for_status()

        # --- 2. Simulate Centre Selection (POST-back) ---
        # We must identify the correct input names for the POST request.
        # Analysis confirms the names on this page are 'CboCentre', 'CboSale', 'CboLeaf'.
        print(f"Fetching Sale Nos and Leaf Types for Kolkata (ID: {KOLKATA_CENTRE_ID}) via POST-back...")
        
        payload = {'CboCentre': KOLKATA_CENTRE_ID}
        
        # POST to the BASE_URL.
        post_response = session.post(BASE_URL, data=payload, timeout=60)
        post_response.raise_for_status()
        
        # The response is the FULL HTML. Use lxml parser.
        soup_post = BeautifulSoup(post_response.text, 'lxml')

        # --- 3. Parse Parameters Dynamically ---
        
        # Extract Sale Numbers (using ID 'CboSale')
        # We check both known ID formats for maximum robustness
        sale_select = soup_post.find('select', {'id': 'CboSale'})
        if not sale_select:
            sale_select = soup_post.find('select', {'id': 'cbosale'})
            if not sale_select:
                 raise Exception("Could not find Sale dropdown (CboSale or cbosale) in POST response.")
        
        # !!! THE FIX: Read the TEXT (e.g., "36/2025") and the VALUE (internal ID)
        # Find the first valid option
        latest_sale_option = None
        for opt in sale_select.find_all('option'):
            if opt.get('value') and opt['value'] != '0':
                latest_sale_option = opt
                break
                
        if not latest_sale_option:
            raise Exception("No valid Sale Numbers found.")

        # We need both the value (for the subsequent request) and the text (for the Sale No)
        latest_sale_value = latest_sale_option['value']
        latest_sale_text = latest_sale_option.get_text(strip=True)
        
        # Parse the Sale No from the text (e.g., "36" from "36/2025")
        try:
            latest_sale_no = latest_sale_text.split('/')[0]
            int(latest_sale_no) # Verify it's a number
        except (IndexError, ValueError):
            print(f"Warning: Could not parse Sale Number from text '{latest_sale_text}'. Defaulting to Unknown.")
            latest_sale_no = "Unknown"

        print(f"Latest Sale No identified: {latest_sale_no} (Value: {latest_sale_value})")

        # Extract Leaf Types (Categories) (using ID 'CboLeaf' or 'cbocat')
        leaf_select = soup_post.find('select', {'id': 'CboLeaf'})
        if not leaf_select:
            leaf_select = soup_post.find('select', {'id': 'cbocat'})
            if not leaf_select:
                raise Exception("Could not find Leaf Type/Category dropdown (CboLeaf or cbocat) in POST response.")

        categories_to_scrape = []
        for opt in leaf_select.find_all('option'):
            if opt.get('value') and opt['value'] != '0':
                categories_to_scrape.append({'value': opt['value'], 'text': opt.get_text(strip=True)})

        print(f"Found categories to scrape: {[lt['text'] for lt in categories_to_scrape]}")

        # --- 4. Iterate and Fetch Commentary (AJAX Simulation) ---
        for category in categories_to_scrape:
            print(f"\nProcessing Category: {category['text']}...")

            # The payload required to fetch the commentary
            # Note: The exact parameter names ('CboCentre', 'CboLeaf', 'CboSale') must match the server expectations.
            commentary_payload = {
                'CboCentre': KOLKATA_CENTRE_ID,
                'CboLeaf': category['value'], 
                'CboSale': latest_sale_value # Use the internal value for the request
            }
            
            print("Fetching commentary stream...")
            # This request goes to the dedicated DATA_STREAM_URL.
            data_response = session.post(DATA_STREAM_URL, data=commentary_payload, timeout=120)
            data_response.raise_for_status()

            # Parse the commentary HTML
            print("Parsing HTML response...")
            # Use lxml parser
            soup_data = BeautifulSoup(data_response.text, 'lxml')
            
            # Extract all text, preserving line breaks.
            commentary_text = soup_data.get_text(separator='\n', strip=True)

            if commentary_text:
                # Add to the standardized structure
                all_commentaries.append({
                    "type": f"Commentary - {category['text']}",
                    "comment": commentary_text
                })
                print("Successfully extracted commentary.")
            else:
                print("Warning: Response received but no text found.")

    except requests.exceptions.RequestException as e:
        print(f"\nERROR: Network request failed. Details: {e}")
        return
    except Exception as e:
        print(f"\nERROR: Processing failed. Details: {e}")
        return

    # --- 5. Save Data ---
    if all_commentaries:
        # Standardized output filename (using the correctly identified Sale Number)
        sale_suffix = f"S{str(latest_sale_no).zfill(2)}"
        output_filename = f"market_commentary_{sale_suffix}.json"
        output_path = OUTPUT_DIR / output_filename

        with open(output_path, 'w', encoding='utf-8') as f:
            # Save as a list, compatible with the consolidation engine
            json.dump(all_commentaries, f, indent=4, ensure_ascii=False)
        
        print(f"\n--- PROCESS COMPLETE ---")
        print(f"Successfully saved final data to: {output_path}")
    else:
        print("\nFAILURE: No market report data was collected.")

if __name__ == "__main__":
    # Call the HTTP simulation function
    scrape_full_market_report_http()
