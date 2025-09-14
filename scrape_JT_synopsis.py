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

# --- Configuration (Following Proven Auction Lots Logic) ---
REPO_ROOT = Path(__file__).resolve().parent
LANDING_PAGE_URL = "https://jthomasindia.com/market_synopsis.php"
BASE_URL = "https://jthomasindia.com/"

# Timeouts and waits (Matching Proven Logic)
MAX_TIMEOUT = 600000  # 10 minutes
DISCOVERY_TIMEOUT = 300000  # 5 minutes
STABILIZATION_WAIT = 90000  # 90 seconds
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'

# Standardized temporary directory for PDF downloads
TEMP_PDF_DIR = REPO_ROOT / "temp_downloads"
TEMP_PDF_DIR.mkdir(parents=True, exist_ok=True)
# ---------------------

def get_centres_synopsis(page):
    """Helper to discover available centres with enhanced waiting (adapted for synopsis)."""
    print("Discovering available synopsis centres...")
    
    # Wait for the dropdown to exist first
    page.wait_for_selector("#cbocenter", timeout=DISCOVERY_TIMEOUT)
    
    # Add a stabilization wait to let the page settle
    print("Initial page load complete. Waiting 10 seconds for stabilization...")
    page.wait_for_timeout(10000)
    
    # Now wait specifically for real options to appear
    print("Waiting for actual centre options to load...")
    page.wait_for_function("""
        () => {
            const select = document.querySelector('#cbocenter');
            if (!select) return false;
            const options = select.querySelectorAll('option');
            
            // We need more than just the "Select Centre" option
            if (options.length <= 1) return false;
            
            // Check if we have real options (with actual values)
            let realOptions = 0;
            for (let i = 0; i < options.length; i++) {
                const value = options[i].value;
                const text = options[i].textContent;
                if (value && value.trim() !== '' && !text.includes('Select')) {
                    realOptions++;
                }
            }
            return realOptions > 0;
        }
    """, timeout=DISCOVERY_TIMEOUT)
    
    print("Real centre options detected. Extracting centres...")
    centre_options = page.locator("#cbocenter option").all()
    centres = []
    
    for opt in centre_options:
        label = opt.inner_text().strip()
        value = opt.get_attribute("value")
        # Enhanced filtering
        if (value and 
            value.strip() != "" and 
            value != "0" and 
            not ("Select" in label or "Choose" in label or "Loading" in label)):
            folder_name = label.lower().replace(' ', '_')
            centres.append({"label": label, "value": value, "folder_name": folder_name})
    
    print(f"Found centres: {[c['label'] for c in centres]}")
    return centres

def process_jthomas_synopsis():
    print(f"Starting EXPANDED J Thomas Market Synopsis scraper (Proven Logic)...")
    print("\nREMINDER: Ensure Chromebook sleep settings are disabled. This process may take 15+ minutes per centre.\n")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, timeout=MAX_TIMEOUT)
        context = browser.new_context(user_agent=USER_AGENT)
        
        try:
            # Discover centres dynamically with enhanced logic
            page = context.new_page()
            page.set_default_timeout(DISCOVERY_TIMEOUT)
            
            print("Navigating to market synopsis page...")
            page.goto(LANDING_PAGE_URL, wait_until="networkidle", timeout=DISCOVERY_TIMEOUT)
            
            centres = get_centres_synopsis(page)
            page.close()

            if not centres:
                print("Warning: Could not find any synopsis centres. The site may be having issues.")
                # Fallback to known centres if discovery fails
                centres = [
                    {"label": "KOLKATA", "value": "5", "folder_name": "kolkata"},
                    {"label": "GUWAHATI", "value": "4", "folder_name": "guwahati"},
                    {"label": "SILIGURI", "value": "6", "folder_name": "siliguri"}
                ]
                print(f"Using fallback centres: {[c['label'] for c in centres]}")

            # Iterate through each centre
            for centre in centres:
                location_label = centre['label']
                location_value = centre['value']
                location_folder = centre['folder_name']
                print(f"\n{'='*20} Processing Synopsis Centre: {location_label} (Value: {location_value}) {'='*20}")
                
                process_single_centre_synopsis(context, location_label, location_value, location_folder)

        except Exception as e:
            print(f"!!! A critical error occurred during the main process: {e}")
        finally:
            browser.close()
            print("Browser closed.")

def process_single_centre_synopsis(context, location_label, location_value, location_folder):
    """Handles the synopsis scraping logic for one specific centre."""
    downloaded_pdf_path = None
    sale_number = None
    page = None

    try:
        page = context.new_page()
        page.set_default_timeout(MAX_TIMEOUT)
        
        # Add stealth script
        page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        # --- Navigation ---
        page.goto(LANDING_PAGE_URL, wait_until="networkidle", timeout=180000)

        # --- Centre Selection ---
        print(f"Selecting synopsis centre '{location_label}'...")
        centre_dropdown_selector = "#cbocenter"
        page.select_option(centre_dropdown_selector, value=location_value)

        # --- Fixed Wait (Following Proven Logic) ---
        print("Synopsis centre selected. Waiting for page to update (Fixed 5s wait)...")
        page.wait_for_timeout(5000)

        # --- Enhanced Sale Selection Logic ---
        sale_dropdown_selector = "#cbosale"
        
        try:
            # Wait for sale options to actually load (not just exist)
            print("Waiting for sale dropdown to populate with real options...")
            page.wait_for_function(f"""
                () => {{
                    const select = document.querySelector('{sale_dropdown_selector}');
                    if (!select) return false;
                    const options = select.querySelectorAll('option');
                    
                    // We need more than just the placeholder option
                    if (options.length <= 1) return false;
                    
                    // Check if we have real options (with actual values)
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
            """, timeout=120000)
            
            # Now get the latest sale (second option, but verify it's real)
            sale_options = page.locator(f"{sale_dropdown_selector} option").all()
            latest_sale_option = None
            
            for i, opt in enumerate(sale_options):
                if i == 0:  # Skip first option (usually placeholder)
                    continue
                    
                label = opt.inner_text().strip()
                value = opt.get_attribute("value")
                
                if (value and 
                    value.strip() != "" and 
                    not ("Select" in label or "Choose" in label)):
                    latest_sale_option = opt
                    latest_sale_label = label
                    break
            
            if not latest_sale_option:
                raise Exception("No valid sale options found")
                
            # Extract sale number from label
            if latest_sale_label:
                match = re.search(r'(\d{1,2})', latest_sale_label)
                if match:
                    sale_number = match.group(1)
                    print(f"Determined Sale Number: {sale_number} from label: '{latest_sale_label}'")
                else:
                    print(f"Warning: Could not extract number from label: {latest_sale_label}")
            
        except Exception as e:
            print(f"Warning: Could not determine sale number or no sales available for {location_label}: {e}")
            return

        if not sale_number:
            print(f"Cannot proceed without determining the Sale Number for {location_label}. Skipping.")
            return

        # Select the latest sale by value (more reliable than index)
        latest_sale_value = latest_sale_option.get_attribute("value")
        page.select_option(sale_dropdown_selector, value=latest_sale_value)

        # --- Fixed Wait (Following Proven Logic) ---
        print("Sale selected. Waiting (Fixed 3s wait)...")
        page.wait_for_timeout(3000)

        # --- Trigger Load and Handle New Page ---
        print("Clicking refresh button to load synopsis...")
        
        # Handle the new page that opens
        with page.context.expect_page() as new_page_info:
            page.click("#refresh")

        report_page = new_page_info.value
        print("New synopsis page opened. Waiting for content to load...")
        
        # Wait with increased timeout
        report_page.wait_for_load_state("networkidle", timeout=MAX_TIMEOUT)
        
        # Additional stabilization wait
        print(f"Waiting for synopsis content to stabilize ({STABILIZATION_WAIT/1000}s)...")
        report_page.wait_for_timeout(STABILIZATION_WAIT)

        # --- PDF Discovery and Download ---
        print("Searching for PDF link on synopsis page...")
        report_page_html = report_page.content()
        soup = BeautifulSoup(report_page_html, 'lxml')

        # Look for PDF in embed or iframe tags
        pdf_tag = soup.find('embed') or soup.find('iframe')
        if not pdf_tag:
            print("No embed/iframe found. Searching for direct PDF links...")
            # Alternative: look for direct PDF links
            pdf_links = soup.find_all('a', href=lambda x: x and x.endswith('.pdf'))
            if pdf_links:
                pdf_relative_url = pdf_links[0]['href']
            else:
                raise Exception("Could not find any PDF links on synopsis page")
        else:
            pdf_relative_url = pdf_tag.get('src')
            if not pdf_relative_url:
                raise Exception("Found PDF tag but it has no 'src' attribute")

        pdf_full_url = urljoin(BASE_URL, pdf_relative_url)
        print(f"Found PDF download link: {pdf_full_url}")

        # --- Download PDF ---
        headers = {
            'User-Agent': USER_AGENT,
            'Referer': LANDING_PAGE_URL
        }
        
        print("Downloading PDF...")
        response_pdf = requests.get(pdf_full_url, headers=headers, timeout=90)
        response_pdf.raise_for_status()

        pdf_filename = Path(pdf_full_url).name or f"synopsis_S{sale_number}.pdf"
        downloaded_pdf_path = TEMP_PDF_DIR / pdf_filename

        with open(downloaded_pdf_path, 'wb') as f:
            f.write(response_pdf.content)
        print(f"Successfully downloaded PDF to: {downloaded_pdf_path}")

        # --- Parse PDF ---
        print("Parsing PDF content...")
        doc = fitz.open(downloaded_pdf_path)
        full_text = ""
        
        for page_num in range(doc.page_count):
            pdf_page = doc.load_page(page_num)
            full_text += pdf_page.get_text()
        doc.close()

        # Create the final JSON object
        output_data = {
            "report_title": f"J Thomas Market Synopsis - {location_label} - Sale {sale_number}",
            "centre": location_label,
            "sale_number": sale_number,
            "source_pdf": downloaded_pdf_path.name,
            "extraction_timestamp": datetime.datetime.now().isoformat(),
            "raw_text": full_text
        }

        # Close the report page
        if not report_page.is_closed():
            report_page.close()

    except Exception as e:
        print(f"\nERROR: Failed during synopsis processing for {location_label}. Details: {e}")
        return

    finally:
        if page and not page.is_closed():
            page.close()

    # --- Standardized Saving & Auto-Manifest Mechanism (Following Proven Logic) ---
    if downloaded_pdf_path and sale_number and output_data.get('raw_text'):
        OUTPUT_DIR = REPO_ROOT / "source_reports" / location_folder
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        sale_suffix = str(sale_number).zfill(2)
        filename = f"JT_synopsis_parsed_S{sale_suffix}.json"
        output_path = OUTPUT_DIR / filename

        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=4, ensure_ascii=False)
            
            print(f"\nSUCCESS! Synopsis processed for {location_label}.")
            print(f"Successfully saved synopsis data to {output_path}")
            
            # Generate manifest using the same utility as auction lots
            generate_manifest(REPO_ROOT, location_folder, sale_suffix, currency="INR")

            # Clean up temporary PDF
            if downloaded_pdf_path.exists():
                downloaded_pdf_path.unlink()
                print(f"Cleaned up temporary PDF: {downloaded_pdf_path}")

        except Exception as e:
            print(f"Error saving file or generating manifest: {e}")
    else:
        print(f"Synopsis processing finished for {location_label} without complete data. No file saved.")

if __name__ == "__main__":
    process_jthomas_synopsis()
