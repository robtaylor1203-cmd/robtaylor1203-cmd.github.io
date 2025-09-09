import requests
import json
import os
import datetime
import pytesseract
from PIL import Image
import io # Used to handle image data in memory

# --- Configuration ---
# PASTE YOUR SCRAPINGBEE API KEY HERE
API_KEY = 'TXS8TGO0S4P9EYEUWPNNPNKQJZBUOJ616ZVYW58I8S7RVOL3HXS70HRKBNDBMFNCFC1ON1LH0FDEC61U' 
TARGET_URL = "https://www.atbltd.com/Docs/current_market_report"
OUTPUT_DIR = "source_reports/mombasa_atbltd_reports"

def process_atbltd_with_scrapingbee():
    """
    Uses ScrapingBee to get screenshots of the multi-page image report,
    parses them with OCR, and saves the combined text.
    """
    print(f"--- Starting ScrapingBee process for {TARGET_URL} ---")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    full_report_text = ""
    
    try:
        # --- Step 1: Get Page 1 Screenshot ---
        print("\n[PART 1/3] Asking ScrapingBee to get a screenshot of Page 1...")
        params_page1 = {
            'api_key': API_KEY,
            'url': TARGET_URL,
            'render_js': 'true',
            'screenshot': 'true',
            'window_width': 1280, # Use a standard desktop width
            'js_scenario': json.dumps({"instructions": [{"wait": 5000}]}) # Wait 5s for JS
        }
        response_page1 = requests.get('https://app.scrapingbee.com/api/v1/', params=params_page1, timeout=180)
        response_page1.raise_for_status() # Will raise an error if the request failed
        
        print("Parsing Page 1 image with OCR...")
        page1_text = pytesseract.image_to_string(Image.open(io.BytesIO(response_page1.content)))
        full_report_text += page1_text + "\n\n--- End of Page 1 ---\n\n"
        print("Successfully parsed Page 1.")
        
        # --- Step 2: Get Page 2 Screenshot (after clicking 'Next') ---
        print("\n[PART 2/3] Asking ScrapingBee to CLICK 'Next' and get Page 2...")
        js_click_scenario = {
            "instructions": [
                {"wait": 3000}, # Wait for initial page
                {"click": "input[value='Next']"}, # Click the 'Next' button
                {"wait": 3000} # Wait again for the new image to load
            ]
        }
        params_page2 = {
            'api_key': API_KEY,
            'url': TARGET_URL,
            'render_js': 'true',
            'screenshot': 'true',
            'window_width': 1280,
            'js_scenario': json.dumps(js_click_scenario)
        }
        response_page2 = requests.get('https://app.scrapingbee.com/api/v1/', params=params_page2, timeout=180)
        response_page2.raise_for_status()
        
        print("Parsing Page 2 image with OCR...")
        page2_text = pytesseract.image_to_string(Image.open(io.BytesIO(response_page2.content)))
        full_report_text += page2_text + "\n\n--- End of Page 2 ---\n\n"
        print("Successfully parsed Page 2.")

    except requests.exceptions.RequestException as e:
        print(f"!!! An error occurred while communicating with ScrapingBee: {e}")
        if 'response' in locals() and response.text:
             print(f"ScrapingBee Error Details: {response.text}")
        return # Stop the script if there's an error
    except Exception as e:
        print(f"!!! An unexpected error occurred: {e}")
        return

    # --- Step 3: Save the final combined text ---
    if full_report_text:
        print("\n[PART 3/3] Saving the combined extracted text...")
        output_data = {
            "report_title": f"ATB Ltd Market Report - {datetime.datetime.now().strftime('%Y-%m-%d')}",
            "raw_text": full_report_text
        }
        date_str = datetime.datetime.now().strftime('%Y-%m-%d')
        output_filename = f"ATBLtd_MarketReport_CLEAN_{date_str}.json"
        output_path = os.path.join(OUTPUT_DIR, output_filename)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=4, ensure_ascii=False)
        print(f"\n--- PROCESS COMPLETE ---")
        print(f"Successfully saved combined data to: {output_path}")
    else:
        print("\n--- PROCESS COMPLETE ---")
        print("No text was extracted from the report images.")

if __name__ == "__main__":
    process_atbltd_with_scrapingbee()
