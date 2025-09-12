import json
import datetime
import re
import pytesseract
from PIL import Image
from playwright.sync_api import sync_playwright, TimeoutError
from pathlib import Path
import io
import cv2
import numpy as np

# --- Standardized Configuration ---
REPO_ROOT = Path(__file__).resolve().parent
LOCATION = "colombo"
OUTPUT_DIR = REPO_ROOT / "source_reports" / LOCATION
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

TARGET_URL = "https://web.forbestea.com/statistics/sri-lankan-statistics/65-sri-lanka-tea-production/1301-sri-lanka-tea-production"
SCREENSHOT_SELECTOR = "article.item-page" 
# ----------------------------------

def preprocess_image_for_ocr(image_bytes):
    """Applies OpenCV preprocessing to enhance OCR accuracy."""
    try:
        # Convert bytes to numpy array
        nparr = np.frombuffer(image_bytes, np.uint8)
        img = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

        # 1. Convert to Grayscale
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)

        # 2. Thresholding (Binarization)
        _, binary = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
        
        # Convert back to PIL Image format for Pytesseract
        processed_image = Image.fromarray(binary)
        return processed_image

    except Exception as e:
        print(f"Error during image preprocessing: {e}")
        # Fallback to original image if preprocessing fails
        return Image.open(io.BytesIO(image_bytes))


def scrape_forbes_walker_production_ocr():
    print(f"Starting Forbes & Walker Monthly Production OCR scraper...")
    extracted_data = {}
    report_period = None # To store YYYY_MM

    with sync_playwright() as p:
        # Reverted to Chromium as it is proven in this environment
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(device_scale_factor=2) # High DPI screenshot
        # Increased timeout
        page.set_default_timeout(60000)

        try:
            page.goto(TARGET_URL, wait_until="networkidle", timeout=90000)

            # Locate the specific element containing the statistics
            try:
                element_handle = page.wait_for_selector(SCREENSHOT_SELECTOR, timeout=30000)
            except TimeoutError:
                raise ValueError(f"Could not find the statistics element using selector: {SCREENSHOT_SELECTOR}")

            # Take a screenshot of the content area
            screenshot_bytes = element_handle.screenshot()
            
            # Preprocess the image
            processed_image = preprocess_image_for_ocr(screenshot_bytes)

            # Perform OCR
            custom_config = r'--oem 3 --psm 6 -l eng'
            ocr_text = pytesseract.image_to_string(processed_image, config=custom_config)
            
            # Determine the report period (Year and Month) from the text
            
            # Pattern matching for Month YYYY
            period_match = re.search(r'(?:JANUARY-)?([A-Z]+)\s+(\d{4})', ocr_text, re.IGNORECASE)
            if period_match:
                month_name = period_match.group(1).capitalize()
                year = period_match.group(2)
                # Convert month name to number
                try:
                    month_num = datetime.datetime.strptime(month_name, "%B").month
                    report_period = f"{year}_{str(month_num).zfill(2)}"
                except ValueError:
                    print(f"Warning: Could not parse month name: {month_name}")

            if not report_period:
                 print("Warning: Could not determine the report period. Using current month fallback.")
                 report_period = datetime.date.today().strftime("%Y_%m")

            if ocr_text.strip():
                extracted_data = {
                    "report_type": "Monthly Production Statistics",
                    "period": report_period,
                    "source_url": TARGET_URL,
                    "scraped_date": datetime.date.today().isoformat(),
                    "ocr_raw_text": ocr_text.strip()
                }
                print(f"Successfully extracted OCR text for period {report_period}.")
            else:
                print("OCR extraction resulted in empty text.")

        except TimeoutError:
            print(f"Timeout Error: The website ({TARGET_URL}) did not respond in time.")
        except Exception as e:
            print(f"An error occurred during scraping/OCR: {e}")
        
        finally:
            browser.close()

    # --- Standardized Saving Mechanism ---
    if extracted_data and report_period:
        file_prefix = "FW_monthly_production_raw_ocr"
        filename = f"{file_prefix}_{report_period}.json"
        output_path = OUTPUT_DIR / filename

        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                 json.dump(extracted_data, f, ensure_ascii=False, indent=4)
            print(f"Successfully saved raw OCR text to {output_path}")
        except Exception as e:
            print(f"Error saving file: {e}")
    else:
        print("Scraping finished without extracting text or determining the report period.")

if __name__ == "__main__":
    scrape_forbes_walker_production_ocr()
