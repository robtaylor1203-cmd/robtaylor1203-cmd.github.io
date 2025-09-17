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

# --- Configuration ---
REPO_ROOT = Path(__file__).resolve().parent
LOCATION = "colombo"
OUTPUT_DIR = REPO_ROOT / "source_reports" / LOCATION
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

TARGET_URL = "https://web.forbestea.com/statistics/sri-lankan-statistics/90-weekly-tea-auction-quantities-averages/1299-sri-lanka-weekly-tea-auction-quantities-averages"

# Enhanced timeouts and settings
MAX_TIMEOUT = 1800000  # 30 minutes
DISCOVERY_TIMEOUT = 300000  # 5 minutes
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'

# Temp directory
TEMP_IMG_DIR = REPO_ROOT / "temp_downloads"
TEMP_IMG_DIR.mkdir(parents=True, exist_ok=True)

def clean_ocr_value(value_str):
    """Cleans common OCR errors from a string that should be a number."""
    if isinstance(value_str, str):
        # Remove common OCR artifacts but keep digits, commas, and periods
        cleaned = re.sub(r'[^\d,.]', '', value_str)
        # Remove multiple periods/commas
        cleaned = re.sub(r'[,.]{2,}', '', cleaned)
        return cleaned
    return value_str

def parse_quantities_from_ocr_enhanced(ocr_text):
    """Enhanced parsing of the raw OCR text."""
    lines = ocr_text.strip().split('\n')
    structured_data = []
    headers = [
        'DATE', 'QTY_HIGH', 'QTY_MEDIUM', 'QTY_LOW', 'QTY_TOTAL', 
        'AVG_HIGH', 'AVG_MEDIUM', 'AVG_LOW', 'AVG_TOTAL'
    ]
    
    # Enhanced regex pattern for date matching
    row_pattern = re.compile(r'^\s*([\d]{1,2}[-\s][A-Za-z]{3,9}[-\s.]?[\d]{4})\s+(.*)', re.IGNORECASE)

    for line_num, line in enumerate(lines):
        line = line.strip()
        if not line:
            continue
            
        match = row_pattern.search(line)
        if match:
            date_str, rest_of_line = match.groups()
            
            # Enhanced date format standardization
            try:
                # Clean up the date string
                cleaned_date_str = re.sub(r'[-\s.]+', '-', date_str.strip())
                
                # Try different date formats
                date_obj = None
                for date_format in ["%d-%b-%Y", "%d-%B-%Y", "%d-%m-%Y"]:
                    try:
                        date_obj = datetime.datetime.strptime(cleaned_date_str, date_format)
                        break
                    except ValueError:
                        continue
                
                if date_obj:
                    iso_date = date_obj.strftime("%Y-%m-%d")
                else:
                    # If all parsing fails, keep original but cleaned
                    iso_date = cleaned_date_str
                    
            except Exception as e:
                print(f"    Date parsing warning for '{date_str}': {e}")
                iso_date = date_str

            # Enhanced number extraction
            # Split the rest of the line and extract all numeric-looking values
            potential_numbers = re.findall(r'[\d,.\s]+', rest_of_line)
            
            # Clean and validate numbers
            number_values = []
            for num_str in potential_numbers:
                cleaned_num = clean_ocr_value(num_str.strip())
                if cleaned_num and len(cleaned_num) > 0:
                    # Only include if it has actual digits
                    if re.search(r'\d', cleaned_num):
                        number_values.append(cleaned_num)
            
            # Take up to 8 number values (matching the headers)
            if len(number_values) >= 4:  # At least 4 values required for a valid row
                # Pad with empty strings if we don't have 8 values
                while len(number_values) < 8:
                    number_values.append("")
                
                # Take only the first 8 values
                number_values = number_values[:8]
                
                all_values = [iso_date] + number_values
                cleaned_values = [clean_ocr_value(v) if i > 0 else v for i, v in enumerate(all_values)]
                
                row_data = dict(zip(headers, cleaned_values))
                structured_data.append(row_data)
                
                print(f"    Parsed row {len(structured_data)}: {iso_date} with {len(number_values)} values")
                
    return structured_data

def preprocess_image_for_ocr(image_bytes):
    """Applies OpenCV preprocessing to enhance OCR accuracy for table data."""
    try:
        nparr = np.frombuffer(image_bytes, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

        # Convert to Grayscale
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

        # Apply slight Gaussian blur to reduce noise
        blurred = cv2.GaussianBlur(gray, (3, 3), 0)

        # Adaptive thresholding for better text recognition
        adaptive_thresh = cv2.adaptiveThreshold(
            blurred, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, cv2.THRESH_BINARY, 11, 2
        )
        
        # Morphological operations to clean up
        kernel = np.ones((1, 1), np.uint8)
        cleaned = cv2.morphologyEx(adaptive_thresh, cv2.MORPH_CLOSE, kernel)
        
        # Convert back to PIL Image format
        processed_image = Image.fromarray(cleaned)
        return processed_image

    except Exception as e:
        print(f"Warning: Error during image preprocessing: {e}. Using original image.")
        import io
        return Image.open(io.BytesIO(image_bytes))

def scrape_forbes_quantities_ocr():
    """Enhanced Forbes quantities table scraper with improved OCR."""
    print(f"Starting Forbes Weekly Quantities OCR scraper (Enhanced Version)")
    
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
    
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            timeout=MAX_TIMEOUT,
            args=['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
        )
        page = browser.new_page(
            device_scale_factor=3,  # High DPI for better OCR
            viewport={'width': 1920, 'height': 1080}
        )
        
        try:
            print(f"[1/6] Navigating to page: {TARGET_URL}")
            page.goto(TARGET_URL, wait_until="networkidle", timeout=180000)

            print("[2/6] Waiting for content to fully load...")
            page.wait_for_timeout(10000)

            print("[3/6] Taking full-page screenshot...")
            screenshot_bytes = page.screenshot(full_page=True)
            
            # Save screenshot for debugging
            screenshot_path = TEMP_IMG_DIR / "temp_quantities_screenshot.png"
            with open(screenshot_path, 'wb') as f:
                f.write(screenshot_bytes)
            print(f"Screenshot saved to {screenshot_path}")

            print("[4/6] Preprocessing image with OpenCV...")
            processed_image = preprocess_image_for_ocr(screenshot_bytes)

            print("[5/6] Performing OCR with enhanced configuration...")
            # Enhanced Tesseract config optimized for tables
            custom_config = r'--oem 3 --psm 6 -c tessedit_char_whitelist=0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz,./-: -l eng'
            
            try:
                ocr_text = pytesseract.image_to_string(processed_image, config=custom_config)
                
                if not ocr_text.strip():
                    # Fallback with different configuration
                    print("    First OCR attempt returned empty. Trying fallback config...")
                    fallback_config = r'--oem 3 --psm 4 -l eng'
                    ocr_text = pytesseract.image_to_string(processed_image, config=fallback_config)
                
                if not ocr_text.strip():
                    raise Exception("OCR failed to extract any text from the screenshot.")
                
                print(f"    OCR extracted {len(ocr_text)} characters of text")
                
                # Save OCR text for debugging
                ocr_debug_path = TEMP_IMG_DIR / "debug_ocr_text.txt"
                with open(ocr_debug_path, 'w', encoding='utf-8') as f:
                    f.write(ocr_text)
                print(f"    OCR text saved to {ocr_debug_path}")
                
            except Exception as ocr_error:
                print(f"OCR Error: {ocr_error}")
                raise

            structured_data = parse_quantities_from_ocr_enhanced(ocr_text)

            if not structured_data:
                raise Exception("OCR text was extracted, but parser failed to find any data rows.")

            print(f"Successfully parsed {len(structured_data)} rows of data.")

            print("[6/6] Saving extracted data to JSON file...")
            output_data = {
                "report_title": "Forbes Tea - Sri Lanka Weekly Tea Auction Quantities/Averages (Enhanced OCR)",
                "source_url": TARGET_URL,
                "retrieval_date": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "ocr_method": "Enhanced Tesseract with OpenCV preprocessing",
                "total_rows_parsed": len(structured_data),
                "auction_data": structured_data
            }

            # Use current date for filename since this covers historical data
            date_str_file = datetime.date.today().strftime('%Y_%m_%d')
            date_str_period = datetime.date.today().strftime('%Y_%m')
            
            file_prefix = "FW_weekly_quantities_enhanced"
            output_filename = f"{file_prefix}_{date_str_file}.json"
            output_path = OUTPUT_DIR / output_filename

            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=4, ensure_ascii=False)

            print(f"\n--- PROCESS COMPLETE ---")
            print(f"Successfully saved parsed data to: {output_path}")
            
            generate_manifest(REPO_ROOT, LOCATION, date_str_period, currency="LKR", report_type="Weekly Production Statistics")

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
            
            error_path = OUTPUT_DIR / f"error_log_quantities_{datetime.date.today().strftime('%Y_%m_%d')}.json"
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
        missing.append("tesseract-ocr (system) + pytesseract (Python)")
    
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
        print("\nTo install:")
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
        scrape_forbes_quantities_ocr()
    else:
        print("\nPlease install missing dependencies and try again.")
