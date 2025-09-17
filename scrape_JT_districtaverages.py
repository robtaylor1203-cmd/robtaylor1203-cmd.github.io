from playwright.sync_api import sync_playwright
import datetime
import json
from urllib.parse import urljoin
from bs4 import BeautifulSoup
import fitz  # PyMuPDF
import requests
from pathlib import Path
import re
from pipeline_utils import generate_manifest

# --- Configuration ---
REPO_ROOT = Path(__file__).resolve().parent
LANDING_PAGE_URL = "https://jthomasindia.com/district_average.php"
BASE_URL = "https://jthomasindia.com/"

# Enhanced timeouts
MAX_TIMEOUT = 3600000  # 60 minutes
DISCOVERY_TIMEOUT = 600000  # 10 minutes
STABILIZATION_WAIT = 120000  # 2 minutes
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'

# Standardized temporary directory for PDF downloads
TEMP_PDF_DIR = REPO_ROOT / "temp_downloads"
TEMP_PDF_DIR.mkdir(parents=True, exist_ok=True)

# District Average is single location (no centres like auction lots)
LOCATION_FOLDER = "district_averages"

def get_available_sales(page):
    """Helper to discover available sales with enhanced waiting."""
    print("Discovering available district average sales...")
    
    page.wait_for_selector("#cbosale", timeout=DISCOVERY_TIMEOUT)
    print("Initial page load complete. Waiting 15 seconds for stabilization...")
    page.wait_for_timeout(15000)
    
    print("Waiting for actual sale options to load...")
    page.wait_for_function("""
        () => {
            const select = document.querySelector('#cbosale');
            if (!select) return false;
            const options = select.querySelectorAll('option');
            if (options.length <= 1) return false;
            
            let realOptions = 0;
            for (let i = 0; i < options.length; i++) {
                const value = options[i].value;
                const text = options[i].textContent;
                if (value && value.trim() !== '' && !text.includes('Select') && !text.includes('Choose')) {
                    realOptions++;
                }
            }
            return realOptions > 0;
        }
    """, timeout=DISCOVERY_TIMEOUT)
    
    print("Real sale options detected. Extracting sales...")
    sale_options = page.locator("#cbosale option").all()
    sales = []
    
    for opt in sale_options:
        label = opt.inner_text().strip()
        value = opt.get_attribute("value")
        if (value and 
            value.strip() != "" and 
            value != "0" and 
            not ("Select" in label or "Choose" in label or "Loading" in label)):
            
            match = re.search(r'(\d{1,2})', label)
            if match:
                sale_number = match.group(1)
                sales.append({
                    "label": label, 
                    "value": value, 
                    "sale_number": sale_number
                })
    
    print(f"Found sales: {[(s['label'], s['sale_number']) for s in sales]}")
    return sales

def process_jthomas_district_averages():
    print(f"Starting J Thomas District Averages scraper (Enhanced Version)")
    print("\nIMPORTANT: This process may take 30+ minutes. Ensure system sleep is disabled.\n")

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True, 
            timeout=MAX_TIMEOUT,
            args=['--no-sandbox', '--disable-setuid-sandbox', '--disable-dev-shm-usage']
        )
        context = browser.new_context(
            user_agent=USER_AGENT,
            viewport={'width': 1920, 'height': 1080}
        )
        
        try:
            page = context.new_page()
            page.set_default_timeout(DISCOVERY_TIMEOUT)
            page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            print("Navigating to district averages page...")
            page.goto(LANDING_PAGE_URL, wait_until="networkidle", timeout=DISCOVERY_TIMEOUT)
            
            sales = get_available_sales(page)
            page.close()

            if not sales:
                print("Warning: Could not find any district average sales. Using fallback.")
                sales = [{"label": "Latest Sale", "value": "1", "sale_number": "01"}]

            # Process up to 3 latest sales
            latest_sales = sales[:3]
            
            for sale in latest_sales:
                sale_label = sale['label']
                sale_value = sale['value']
                sale_number = sale['sale_number']
                print(f"\n{'='*20} Processing District Average Sale: {sale_label} (Sale #{sale_number}) {'='*20}")
                
                process_single_district_average_sale(context, sale_label, sale_value, sale_number)

        except Exception as e:
            print(f"Critical error occurred: {e}")
        finally:
            browser.close()
            print("Browser closed.")

def process_single_district_average_sale(context, sale_label, sale_value, sale_number):
    """Handles the district average scraping logic for one specific sale."""
    downloaded_pdf_path = None
    page = None

    try:
        page = context.new_page()
        page.set_default_timeout(MAX_TIMEOUT)
        page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        page.goto(LANDING_PAGE_URL, wait_until="networkidle", timeout=300000)

        print(f"Selecting district average sale '{sale_label}'...")
        sale_dropdown_selector = "#cbosale"
        
        try:
            print("Waiting for sale dropdown to populate with real options...")
            page.wait_for_function(f"""
                () => {{
                    const select = document.querySelector('{sale_dropdown_selector}');
                    if (!select) return false;
                    const options = select.querySelectorAll('option');
                    if (options.length <= 1) return false;
                    
                    let realOptions = 0;
                    for (let i = 0; i < options.length; i++) {{
                        const value = options[i].value;
                        const text = options[i].textContent;
                        if (value && value.trim() !== '' && !text.includes('Select') && !text.includes('Choose')) {{
                            realOptions++;
                        }}
                    }}
                    return realOptions > 0;
                }}
            """, timeout=180000)
            
            print(f"Sale options loaded. Selecting sale with value: {sale_value}")
            
        except Exception as e:
            print(f"Warning: Could not wait for sale options for {sale_label}: {e}")
            return

        page.select_option(sale_dropdown_selector, value=sale_value)
        print("Sale selected. Waiting...")
        page.wait_for_timeout(5000)

        print("Clicking refresh button to load district averages...")
        
        with page.context.expect_page() as new_page_info:
            page.click("#refresh")

        report_page = new_page_info.value
        print("New district averages page opened. Waiting for content to load...")
        
        report_page.wait_for_load_state("networkidle", timeout=MAX_TIMEOUT)
        print(f"Waiting for district averages content to stabilize...")
        report_page.wait_for_timeout(STABILIZATION_WAIT)

        print("Searching for PDF link on district averages page...")
        report_page_html = report_page.content()
        soup = BeautifulSoup(report_page_html, 'lxml')

        pdf_tag = soup.find('embed') or soup.find('iframe')
        if not pdf_tag:
            print("No embed/iframe found. Searching for direct PDF links...")
            pdf_links = soup.find_all('a', href=lambda x: x and x.endswith('.pdf'))
            if pdf_links:
                pdf_relative_url = pdf_links[0]['href']
            else:
                raise Exception("Could not find any PDF links on district averages page")
        else:
            pdf_relative_url = pdf_tag.get('src')
            if not pdf_relative_url:
                raise Exception("Found PDF tag but it has no 'src' attribute")

        pdf_full_url = urljoin(BASE_URL, pdf_relative_url)
        print(f"Found PDF download link: {pdf_full_url}")

        headers = {
            'User-Agent': USER_AGENT,
            'Referer': LANDING_PAGE_URL
        }
        
        print("Downloading PDF...")
        response_pdf = requests.get(pdf_full_url, headers=headers, timeout=120)
        response_pdf.raise_for_status()

        pdf_filename = Path(pdf_full_url).name or f"district_avg_S{sale_number}.pdf"
        downloaded_pdf_path = TEMP_PDF_DIR / pdf_filename

        with open(downloaded_pdf_path, 'wb') as f:
            f.write(response_pdf.content)
        print(f"Successfully downloaded PDF to: {downloaded_pdf_path}")

        print("Parsing PDF content...")
        doc = fitz.open(downloaded_pdf_path)
        full_text = ""
        
        for page_num in range(doc.page_count):
            pdf_page = doc.load_page(page_num)
            full_text += pdf_page.get_text()
        doc.close()

        output_data = {
            "report_title": f"J Thomas District Average - Sale {sale_number}",
            "sale_number": sale_number,
            "sale_label": sale_label,
            "source_pdf": downloaded_pdf_path.name,
            "extraction_timestamp": datetime.datetime.now().isoformat(),
            "raw_text": full_text
        }

        if not report_page.is_closed():
            report_page.close()

    except Exception as e:
        print(f"\nERROR: Failed during district averages processing for sale {sale_number}. Details: {e}")
        if page and not page.is_closed():
            try:
                page.screenshot(path=f'error_screenshot_dist_avg_S{sale_number}.png')
                print(f"Saved error screenshot")
            except:
                pass
        return

    finally:
        if page and not page.is_closed():
            page.close()

    if downloaded_pdf_path and sale_number and output_data.get('raw_text'):
        OUTPUT_DIR = REPO_ROOT / "source_reports" / LOCATION_FOLDER
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        sale_suffix = str(sale_number).zfill(2)
        filename = f"JT_district_average_enhanced_S{sale_suffix}.json"
        output_path = OUTPUT_DIR / filename

        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=4, ensure_ascii=False)
            
            print(f"\nSUCCESS! District averages processed for Sale {sale_number}.")
            print(f"Successfully saved district averages data to {output_path}")
            
            generate_manifest(REPO_ROOT, LOCATION_FOLDER, sale_suffix, currency="INR")

            if downloaded_pdf_path.exists():
                downloaded_pdf_path.unlink()
                print(f"Cleaned up temporary PDF")

        except Exception as e:
            print(f"Error saving file or generating manifest: {e}")
    else:
        print(f"District averages processing finished for sale {sale_number} without complete data.")

if __name__ == "__main__":
    process_jthomas_district_averages()
