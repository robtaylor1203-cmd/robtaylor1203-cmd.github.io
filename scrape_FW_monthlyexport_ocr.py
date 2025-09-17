import json
import datetime
import re
import cv2
import numpy as np
from pathlib import Path
from pipeline_utils import generate_manifest

# Enhanced imports with fallback options
try:
    import pytesseract
    from PIL import Image
    TESSERACT_AVAILABLE = True
    print("Tesseract OCR loaded successfully")
except ImportError as e:
    TESSERACT_AVAILABLE = False
    print(f"Warning: Tesseract OCR not available: {e}")

try:
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_AVAILABLE = True
    print("Playwright loaded successfully")
except ImportError as e:
    PLAYWRIGHT_AVAILABLE = False
    print(f"Warning: Playwright not available: {e}")

import io

# --- Configuration ---
REPO_ROOT = Path(__file__).resolve().parent
LOCATION = "colombo"
OUTPUT_DIR = REPO_ROOT / "source_reports" / LOCATION
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

TARGET_URL = "https://web.forbestea.com/statistics/sri-lankan-statistics/64-sri-lanka-tea-exports/1302-sri-lanka-tea-exports"

# Enhanced timeouts and settings
MAX_TIMEOUT = 1800000  # 30 minutes
DISCOVERY_TIMEOUT = 300000  # 5 minutes
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'

# Temp directory
TEMP_IMG_DIR = REPO_ROOT / "temp_downloads"
TEMP_IMG_DIR.mkdir(parents=True, exist_ok=True)

def preprocess_image_for_ocr(image_bytes):
    """Applies OpenCV preprocessing to enhance OCR accuracy."""
    try:
        nparr = np.frombuffer(image_bytes, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

        # Convert to Grayscale
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

        # Apply Gaussian blur to reduce noise
        blurred = cv2.GaussianBlur(gray, (5, 5), 0)

        # Thresholding (Binarization) - Otsu's method
        _, binary = cv2.threshold(blurred, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
        
        # Morphological operations to clean up
        kernel = np.ones((2, 2), np.uint8)
        cleaned = cv2.morphologyEx(binary, cv2.MORPH_CLOSE, kernel)
        
        # Convert back to PIL Image format for Pytesseract
        processed_image = Image.fromarray(cleaned)
        return processed_image

    except Exception as e:
        print(f"Warning: Error during image preprocessing: {e}. Falling back to original.")
        return Image.open(io.BytesIO(image_bytes))

def parse_exports_from_ocr_enhanced(ocr_text):
    """Enhanced parsing of raw OCR text using robust counting method."""
    lines = ocr_text.strip().split('\n')
    
    # Data structures
    all_tables_data = {
        "MONTHLY_QUANTITY": [],
        "MONTHLY_VALUE": [],
        "CUMULATIVE_QUANTITY": [],
        "CUMULATIVE_VALUE": []
    }
    
    table_order = ["MONTHLY_QUANTITY", "MONTHLY_VALUE", "CUMULATIVE_QUANTITY", "CUMULATIVE_VALUE"]
    
    headers = {
        "MONTHLY_QUANTITY": ['DESCRIPTION', 'JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'],
        "MONTHLY_VALUE": ['DESCRIPTION', 'JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'],
        "CUMULATIVE_QUANTITY": ['DESCRIPTION', 'JAN/FEB', 'JAN/MAR', 'JAN/APR', 'JAN/MAY', 'JAN/JUN', 'JAN/JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'],
        "CUMULATIVE_VALUE": ['DESCRIPTION', 'JAN/FEB', 'JAN/MAR', 'JAN/APR', 'JAN/MAY', 'JAN/JUN', 'JAN/JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
    }
    
    row_keywords = {
        "Bulk": "Tea In Bulk",
        "Packets": "Tea In Packets", 
        "Bags": "Tea In Bags",
        "Instant": "Instant Tea",
        "Green": "Green Tea",
        "TOTAL": "TOTAL"
    }
    
    # Enhanced counting logic
    seen_counts = {desc: 0 for desc in row_keywords.values()}

    for line_num, line in enumerate(lines):
        line = line.strip()
        if not line:
            continue

        for keyword, clean_description in row_keywords.items():
            # More flexible keyword matching
            if (re.search(r'\b' + re.escape(keyword) + r'\b', line, re.IGNORECASE) or
                (keyword == "TOTAL" and "total" in line.lower())):
                
                seen_counts[clean_description] += 1
                
                table_index = seen_counts[clean_description] - 1
                if table_index < len(table_order):
                    table_name = table_order[table_index]
                    
                    # Extract numbers with enhanced regex
                    numbers = re.findall(r'[\d,]+(?:\.\d+)?', line)
                    if numbers:
                        # Clean numbers
                        cleaned_numbers = []
                        for num in numbers:
                            cleaned_num = re.sub(r'[^\d,.]', '', num)
                            if cleaned_num:
                                cleaned_numbers.append(cleaned_num)
                        
                        values = [clean_description] + cleaned_numbers
                        current_headers = headers.get(table_name, [])
                        
                        # Handle potential mismatch in column counts
                        if len(values) > len(current_headers):
                             values = values[:len(current_headers)]
                        
                        # Pad with empty strings if not enough values
                        while len(values) < len(current_headers):
                            values.append("")
                        
                        row_data = dict(zip(current_headers, values))
                        all_tables_data[table_name].append(row_data)
                        
                        print(f"  Parsed {clean_description} for {table_name}: {len(cleaned_numbers)} values")
                
                break
                
    return all_tables_data

def scrape_forbes_exports_ocr():
    print(f"Starting Forbes Tea Exports OCR scraper (Enhanced Version)")
    
    if not TESSERACT_AVAILABLE:
        print("ERROR: Tesseract OCR is not available. Please install:")
        print("  sudo apt-get update")
        print("  sudo apt-get install tesseract-ocr")
        print("  pip install pytesseract pillow")
        return
    
    if not PLAYWRIGHT_AVAILABLE:
        print("ERROR: Playwright is not available. Please install:")
        print("  pip install playwright")
        print("  playwright install chromium")
        return
    
    report_period = datetime.date.today().strftime("%Y_%m")
    print(f"Using Current System Date for Report Period: {report_period}")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            timeout=MAX_TIMEOUT,
            args=['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
        )
        page = browser.new_page(
            device_scale_factor=3,  # Higher DPI for better OCR
            viewport={'width': 1920, 'height': 1080}
        ) 
        
        try:
            print(f"[1/6] Navigating to page: {TARGET_URL}")
            page.goto(TARGET_URL, wait_until="networkidle", timeout=180000)

            print("[2/6] Waiting for page to fully load...")
            page.wait_for_timeout(10000)

            print("[3/6] Taking full-page screenshot (Enhanced DPI)...")
            screenshot_bytes = page.screenshot(full_page=True)
            
            print("[4/6] Preprocessing image with OpenCV...")
            processed_image = preprocess_image_for_ocr(screenshot_bytes)

            print("[5/6] Performing OCR with enhanced configuration...")
            # Enhanced Tesseract config for better table recognition
            custom_config = r'--oem 3 --psm 6 -c tessedit_char_whitelist=0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz,./ -l eng'
            
            try:
                ocr_text = pytesseract.image_to_string(processed_image, config=custom_config)
                
                if not ocr_text.strip():
                    # Fallback with different PSM mode
                    print("    First OCR attempt returned empty. Trying alternative config...")
                    fallback_config = r'--oem 3 --psm 4 -l eng'
                    ocr_text = pytesseract.image_to_string(processed_image, config=fallback_config)
                
                if not ocr_text.strip():
                    raise Exception("OCR failed to extract any text from the screenshot.")
                
                print(f"    OCR extracted {len(ocr_text)} characters of text")
                
            except Exception as ocr_error:
                print(f"OCR Error: {ocr_error}")
                # Save screenshot for debugging
                debug_path = TEMP_IMG_DIR / f"debug_export_screenshot_{report_period}.png"
                with open(debug_path, 'wb') as f:
                    f.write(screenshot_bytes)
                print(f"Debug screenshot saved to: {debug_path}")
                raise
            
            structured_data = parse_exports_from_ocr_enhanced(ocr_text)

            # Validate that some data was parsed
            total_rows = sum(len(table_data) for table_data in structured_data.values())
            if total_rows == 0:
                raise Exception("Parsing logic failed to find any data rows in the tables.")

            print(f"Successfully parsed data from {total_rows} total rows across all tables.")

            print("[6/6] Saving extracted data to JSON file...")
            output_data = {
                "report_title": "Forbes Tea - Sri Lanka Tea Exports (Enhanced OCR)",
                "period": report_period,
                "source_url": TARGET_URL,
                "retrieval_date": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "ocr_method": "Enhanced Tesseract with OpenCV preprocessing",
                "export_data_by_table": structured_data
            }

            file_prefix = "FW_monthly_export_enhanced"
            output_filename = f"{file_prefix}_{report_period}.json"
            output_path = OUTPUT_DIR / output_filename

            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=4, ensure_ascii=False)

            print(f"\n--- PROCESS COMPLETE ---")
            print(f"Successfully saved parsed data to: {output_path}")
            
            generate_manifest(REPO_ROOT, LOCATION, report_period, currency="LKR", report_type="Monthly Export Statistics")

        except Exception as e:
            print(f"ERROR: An error occurred: {e}")
            # Enhanced error logging
            error_log = {
                "error": str(e),
                "timestamp": datetime.datetime.now().isoformat(),
                "tesseract_available": TESSERACT_AVAILABLE,
                "playwright_available": PLAYWRIGHT_AVAILABLE,
                "url": TARGET_URL
            }
            
            error_path = OUTPUT_DIR / f"error_log_export_{report_period}.json"
            with open(error_path, 'w') as f:
                json.dump(error_log, f, indent=2)
            print(f"Error details saved to: {error_path}")
            
        finally:
            browser.close()

def check_dependencies():
    """Check if all required dependencies are available."""
    print("Checking dependencies...")
    
    missing = []
    
    if not TESSERACT_AVAILABLE:
        missing.append("tesseract-ocr (system package) and pytesseract (Python package)")
    
    if not PLAYWRIGHT_AVAILABLE:
        missing.append("playwright")
    
    try:
        import cv2
        print("✓ OpenCV available")
    except ImportError:
        missing.append("opencv-python")
    
    try:
        import numpy
        print("✓ NumPy available")
    except ImportError:
        missing.append("numpy")
    
    if TESSERACT_AVAILABLE:
        print("✓ Tesseract OCR available")
    
    if PLAYWRIGHT_AVAILABLE:
        print("✓ Playwright available")
    
    if missing:
        print(f"\nMISSING DEPENDENCIES: {', '.join(missing)}")
        print("\nTo install missing dependencies:")
        print("  sudo apt-get update")
        if "tesseract-ocr" in str(missing):
            print("  sudo apt-get install tesseract-ocr")
        print("  pip install pytesseract pillow opencv-python numpy playwright")
        if "playwright" in str(missing):
            print("  playwright install chromium")
        return False
    else:
        print("✓ All dependencies available")
        return True

if __name__ == "__main__":
    if check_dependencies():
        scrape_forbes_exports_ocr()
    else:
        print("\nPlease install missing dependencies and try again.")
