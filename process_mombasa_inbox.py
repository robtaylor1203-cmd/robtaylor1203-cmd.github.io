import json
import datetime
import zipfile
import shutil
import fitz  # PyMuPDF for PDFs
import docx  # python-docx for .docx files
import openpyxl  # openpyxl for .xlsx files
import requests
import re
from pathlib import Path
from playwright.sync_api import sync_playwright
from urllib.parse import urljoin, urlparse
from pipeline_utils import generate_manifest

# --- Configuration (Following Proven Logic) ---
REPO_ROOT = Path(__file__).resolve().parent
LOCATION = "mombasa"
LANDING_PAGE_URL = "https://www.tbeal.net/tbea-market-report/"
BASE_URL = "https://www.tbeal.net"

# Standardized INPUT directory (for local files)
INBOX_PATH = REPO_ROOT / "Inbox" / LOCATION
INBOX_PATH.mkdir(parents=True, exist_ok=True)

# Standardized OUTPUT directory
OUTPUT_DIR = REPO_ROOT / "source_reports" / LOCATION
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Archive path
ARCHIVE_PATH = INBOX_PATH / "archive"
ARCHIVE_PATH.mkdir(parents=True, exist_ok=True)

# Temporary download directory for TBEAL reports
TEMP_DOWNLOAD_DIR = REPO_ROOT / "temp_downloads" / LOCATION
TEMP_DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

# Timeouts and waits (Matching Proven Logic)
MAX_TIMEOUT = 600000  # 10 minutes
DISCOVERY_TIMEOUT = 300000  # 5 minutes
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36'

# --- Reference data for calculating the sale number (Restored) ---
REFERENCE_DATE = datetime.date(2025, 9, 1)
REFERENCE_SALE_NO = 34
# ---------------------

def calculate_upcoming_sale_no():
    """
    Calculates the sale number for the upcoming Monday. (Restored Logic)
    """
    today = datetime.date.today()
    start_of_week = today - datetime.timedelta(days=today.weekday())
    weeks_passed = (start_of_week - REFERENCE_DATE).days // 7
    
    if weeks_passed < 0:
        print("Warning: Reference date is in the future or calculation error.")
        return REFERENCE_SALE_NO 

    current_sale_no = REFERENCE_SALE_NO + weeks_passed
    print(f"Calculated Current/Upcoming Sale Number: {current_sale_no}")
    return current_sale_no

def extract_text_from_file(file_path: Path):
    """
    Intelligently extracts text from a file based on its extension. (Restored Logic)
    """
    extension = file_path.suffix.lower()
    text = ""

    print(f"  - Reading file: {file_path.name}")

    try:
        if extension == ".txt":
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                text = f.read()
        elif extension == ".pdf":
            with fitz.open(file_path) as doc:
                for page in doc:
                    text += page.get_text()
        elif extension == ".docx" or extension == ".doc":
            if extension == ".docx":
                doc = docx.Document(file_path)
                for para in doc.paragraphs:
                    text += para.text + '\n'
            else:
                # For .doc files, try to read as docx anyway (sometimes works)
                try:
                    doc = docx.Document(file_path)
                    for para in doc.paragraphs:
                        text += para.text + '\n'
                except:
                    text = f"Could not process .doc file: {file_path.name}. Consider converting to .docx"
        elif extension == ".xlsx" or extension == ".xls":
            workbook = openpyxl.load_workbook(file_path)
            for sheetname in workbook.sheetnames:
                sheet = workbook[sheetname]
                text += f"--- Sheet: {sheetname} ---\n"
                for row in sheet.iter_rows(values_only=True):
                    text += "\t".join(str(cell) for cell in row if cell is not None) + "\n"
        else:
            text = f"Unsupported file type: {extension}"
            print(f"    Warning: {text}")
    except Exception as e:
        print(f"    Error extracting text from {file_path.name}: {e}")
        text = f"Error processing file: {e}"
        
    return text

def extract_sale_and_year_from_title(title):
    """Extract sale number and year from TBEAL report titles."""
    # Strategy 1: Look for "Sale XX" pattern
    sale_match = re.search(r'Sale\s+(\d{1,2})', title, re.IGNORECASE)
    
    # Strategy 2: Look for year (4 digits)
    year_match = re.search(r'(\d{4})', title)
    
    sale_number = None
    year = None
    
    if sale_match:
        sale_number = sale_match.group(1).zfill(2)
    
    if year_match:
        year = year_match.group(1)
    
    # Fallback: current year if no year found
    if not year:
        year = str(datetime.datetime.now().year)
        
    return sale_number, year

def get_all_tbeal_reports(page):
    """Discover all available TBEAL market reports with enhanced waiting."""
    print("Discovering all available TBEAL market reports...")
    
    # Wait for page to load
    page.wait_for_load_state("networkidle", timeout=DISCOVERY_TIMEOUT)
    page.wait_for_timeout(10000)  # 10 second stabilization
    
    all_reports = []
    
    # Look for yearly categories (2024, 2023, 2022, etc.)
    try:
        # Find year links/categories
        year_links = page.locator('a[href*="market-report"], a[href*="tbea-market-report"]').all()
        
        print(f"Found {len(year_links)} potential year/report links")
        
        for link in year_links:
            try:
                href = link.get_attribute('href')
                text = link.inner_text().strip()
                
                if href and ('2024' in text or '2023' in text or '2022' in text or 
                           '2021' in text or '2020' in text or '2019' in text):
                    year = re.search(r'(20\d{2})', text)
                    if year:
                        all_reports.append({
                            'type': 'year_category',
                            'year': year.group(1),
                            'href': href,
                            'text': text
                        })
                elif 'download' in href.lower():
                    # Direct download links
                    sale_number, year = extract_sale_and_year_from_title(text)
                    if sale_number:
                        all_reports.append({
                            'type': 'direct_download',
                            'sale_number': sale_number,
                            'year': year,
                            'href': href,
                            'title': text
                        })
            except Exception as e:
                continue
    
    except Exception as e:
        print(f"Error discovering reports: {e}")
    
    print(f"Discovered {len(all_reports)} potential reports/categories")
    return all_reports

def explore_year_category(context, category_info):
    """Explore a specific year category to find individual reports."""
    year = category_info['year']
    year_url = category_info['href']
    
    if not year_url.startswith('http'):
        year_url = urljoin(BASE_URL, year_url)
    
    print(f"Exploring {year} category: {year_url}")
    
    page = None
    try:
        page = context.new_page()
        page.set_default_timeout(MAX_TIMEOUT)
        
        page.goto(year_url, wait_until="networkidle", timeout=180000)
        page.wait_for_timeout(5000)
        
        # Look for download links in this year's page
        download_links = page.locator('a[href*="download"]').all()
        
        year_reports = []
        for link in download_links:
            try:
                href = link.get_attribute('href')
                text = link.inner_text().strip()
                
                if href and 'market-report' in href.lower():
                    sale_number, report_year = extract_sale_and_year_from_title(text)
                    if sale_number:
                        year_reports.append({
                            'sale_number': sale_number,
                            'year': report_year or year,
                            'href': href,
                            'title': text,
                            'download_url': urljoin(BASE_URL, href) if not href.startswith('http') else href
                        })
            except Exception as e:
                continue
        
        print(f"Found {len(year_reports)} reports in {year}")
        return year_reports
        
    except Exception as e:
        print(f"Error exploring {year} category: {e}")
        return []
    finally:
        if page and not page.is_closed():
            page.close()

def download_word_document(url, filename):
    """Download a Word document from TBEAL."""
    print(f"Downloading Word document: {filename}")
    
    try:
        headers = {
            'User-Agent': USER_AGENT,
            'Accept': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document,application/msword,*/*'
        }
        
        response = requests.get(url, headers=headers, timeout=90)
        response.raise_for_status()
        
        # Determine file extension from content type or URL
        content_type = response.headers.get('content-type', '').lower()
        if 'wordprocessingml' in content_type:
            extension = '.docx'
        elif 'msword' in content_type:
            extension = '.doc'
        else:
            # Fallback: try to determine from URL
            if url.lower().endswith('.docx'):
                extension = '.docx'
            elif url.lower().endswith('.doc'):
                extension = '.doc'
            else:
                extension = '.docx'  # Default assumption
        
        # Ensure filename has correct extension
        if not filename.lower().endswith(('.doc', '.docx')):
            filename = filename + extension
        
        file_path = TEMP_DOWNLOAD_DIR / filename
        
        with open(file_path, 'wb') as f:
            f.write(response.content)
        
        print(f"Successfully downloaded to: {file_path}")
        return file_path
        
    except Exception as e:
        print(f"Error downloading {filename}: {e}")
        return None

def scrape_tbeal_all_reports():
    """Main function to scrape all available TBEAL reports."""
    print(f"Starting EXPANDED TBEAL Market Reports scraper - ALL REPORTS (Proven Logic)...")
    print("\nREMINDER: This will discover and process ALL available TBEAL reports with Word document downloads.\n")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, timeout=MAX_TIMEOUT)
        context = browser.new_context(user_agent=USER_AGENT)
        
        try:
            # Discover all available reports
            page = context.new_page()
            page.set_default_timeout(DISCOVERY_TIMEOUT)
            
            print("Navigating to TBEAL market reports page...")
            page.goto(LANDING_PAGE_URL, wait_until="networkidle", timeout=DISCOVERY_TIMEOUT)
            
            # Get initial report listings
            initial_reports = get_all_tbeal_reports(page)
            
            # Direct access strategy for 2025 (and other recent years)
            print("\nDirect access strategy for recent years...")
            direct_access_years = [
                {"year": "2025", "url": "https://www.tbeal.net/download-category/tbea-market-report-2025/"},
                {"year": "2024", "url": "https://www.tbeal.net/download-category/tbea-market-report-2024/"},
                {"year": "2023", "url": "https://www.tbeal.net/download-category/tbea-market-report-2023/"}
            ]
            
            # Check which years we haven't found yet
            found_years = {item['year'] for item in initial_reports if item['type'] == 'year_category'}
            
            for year_info in direct_access_years:
                year = year_info['year']
                url = year_info['url']
                
                if year not in found_years:
                    print(f"Attempting direct access to {year} via: {url}")
                    try:
                        # Test if the URL exists and has content
                        test_page = context.new_page()
                        response = test_page.goto(url, wait_until="networkidle", timeout=60000)
                        
                        if response.status == 200:
                            # Check if there are actually reports on this page
                            download_links = test_page.locator('a[href*="download"]').all()
                            if len(download_links) > 0:
                                print(f"  Success! Found {len(download_links)} reports for {year}")
                                initial_reports.append({
                                    'type': 'year_category',
                                    'year': year,
                                    'href': url,
                                    'text': f'TBEA Market Report {year} (Direct Access)'
                                })
                                found_years.add(year)
                            else:
                                print(f"  {year} page exists but no reports found")
                        else:
                            print(f"  {year} page returned status {response.status}")
                        
                        test_page.close()
                        
                    except Exception as e:
                        print(f"  Direct access to {year} failed: {e}")
                else:
                    print(f"  {year} already found via normal discovery")
            
            # Additional strategy: Look for 2025 reports on the main page itself
            print("\nChecking main page for recent reports (including 2025)...")
            try:
                # Look for any recent download links on the main page
                main_page_links = page.locator('a[href*="download"]').all()
                print(f"Found {len(main_page_links)} download links on main page")
                
                for link in main_page_links:
                    try:
                        href = link.get_attribute('href')
                        text = link.inner_text().strip()
                        
                        # Check if this is a 2025 report
                        if '2025' in text or '2025' in href:
                            sale_number, year = extract_sale_and_year_from_title(text)
                            if sale_number and year == '2025':
                                print(f"  Found 2025 report on main page: {text}")
                                initial_reports.append({
                                    'type': 'direct_download',
                                    'sale_number': sale_number,
                                    'year': year,
                                    'href': href,
                                    'title': text,
                                    'download_url': urljoin(BASE_URL, href) if not href.startswith('http') else href
                                })
                    except:
                        continue
            except Exception as e:
                print(f"Error checking main page for 2025 reports: {e}")
            
            page.close()
            
            if not initial_reports:
                print("Warning: Could not find any TBEAL market reports.")
                return
            
            # Explore year categories to get individual reports
            all_individual_reports = []
            
            for item in initial_reports:
                if item['type'] == 'year_category':
                    year_reports = explore_year_category(context, item)
                    all_individual_reports.extend(year_reports)
                elif item['type'] == 'clickable_year':
                    # Handle clickable elements without href
                    print(f"Attempting to click {item['year']} button to discover URL...")
                    try:
                        # Create a new page for clicking
                        click_page = context.new_page()
                        click_page.goto(LANDING_PAGE_URL, wait_until="networkidle", timeout=DISCOVERY_TIMEOUT)
                        
                        # Find and click the year button
                        year_button = click_page.locator(f'*:has-text("TBEA Market Report {item["year"]}")').first
                        
                        # Wait for navigation after click
                        with click_page.expect_navigation(timeout=30000):
                            year_button.click()
                        
                        # Now we have the URL for this year's category
                        current_url = click_page.url
                        print(f"  Discovered {item['year']} URL: {current_url}")
                        
                        # Create category info and explore it
                        category_info = {
                            'year': item['year'],
                            'href': current_url,
                            'text': item['text']
                        }
                        
                        click_page.close()
                        year_reports = explore_year_category(context, category_info)
                        all_individual_reports.extend(year_reports)
                        
                    except Exception as e:
                        print(f"  Failed to click and explore {item['year']}: {e}")
                        
                elif item['type'] == 'direct_download':
                    all_individual_reports.append(item)
            
            if not all_individual_reports:
                print("Warning: No individual reports found.")
                return
            
            # Sort by year and sale number (newest first)
            all_individual_reports.sort(key=lambda x: (int(x['year']), int(x['sale_number'])), reverse=True)
            
            # Show summary of what we found
            years_found = {}
            for report in all_individual_reports:
                year = report['year']
                if year not in years_found:
                    years_found[year] = 0
                years_found[year] += 1
            
            print(f"\nTBEAL Discovery Summary:")
            for year in sorted(years_found.keys(), reverse=True):
                print(f"  {year}: {years_found[year]} reports")
            
            # Limit to reasonable number for processing
            max_reports = 15  # Process up to 15 latest reports
            reports_to_process = all_individual_reports[:max_reports]
            
            print(f"\nFound {len(all_individual_reports)} total reports. Processing {len(reports_to_process)} latest reports:")
            for report in reports_to_process:
                print(f"  - Sale {report['sale_number']} ({report['year']}): {report['title']}")

            # Process each report
            for i, report in enumerate(reports_to_process):
                sale_number = report['sale_number']
                year = report['year']
                title = report['title']
                download_url = report.get('download_url', report['href'])
                
                if not download_url.startswith('http'):
                    download_url = urljoin(BASE_URL, download_url)
                
                print(f"\n{'='*20} Processing Report {i+1}/{len(reports_to_process)}: Sale {sale_number} ({year}) {'='*20}")
                
                process_single_tbeal_report(sale_number, year, title, download_url)

        except Exception as e:
            print(f"!!! A critical error occurred during the main process: {e}")
        finally:
            browser.close()
            print("Browser closed.")

def process_single_tbeal_report(sale_number, year, title, download_url):
    """Process a single TBEAL market report."""
    
    try:
        # Create filename for download
        safe_filename = f"tbeal_market_report_S{sale_number}_{year}"
        
        # Download the Word document
        downloaded_file = download_word_document(download_url, safe_filename)
        
        if not downloaded_file or not downloaded_file.exists():
            print(f"Failed to download report for Sale {sale_number} ({year})")
            return
        
        # Extract text from the downloaded Word document
        print(f"Extracting text from downloaded Word document...")
        extracted_text = extract_text_from_file(downloaded_file)
        
        if not extracted_text or extracted_text.startswith("Error"):
            print(f"Failed to extract text from Sale {sale_number} ({year})")
            return
        
        # Create output data structure
        output_data = {
            "report_title": title,
            "sale_number": sale_number,
            "year": year,
            "source_url": download_url,
            "retrieval_date": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "file_type": "Word Document",
            "content_length": len(extracted_text),
            "extracted_text": extracted_text
        }
        
        print(f"Extracted {len(extracted_text)} characters from Sale {sale_number} ({year})")
        
        # Clean up downloaded file
        downloaded_file.unlink()
        print(f"Cleaned up temporary download: {downloaded_file}")
        
    except Exception as e:
        print(f"\nERROR: Failed during TBEAL processing for Sale {sale_number} ({year}). Details: {e}")
        return

    # --- Standardized Saving & Auto-Manifest Mechanism (Following Proven Logic) ---
    if output_data and extracted_text.strip():
        sale_suffix = str(sale_number).zfill(2)
        
        # Include year in filename: tbeal_market_report_W01_2024.json (using 'W' for Week as in original)
        filename = f"tbeal_market_report_W{sale_suffix}_{year}.json"
        output_path = OUTPUT_DIR / filename

        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=4, ensure_ascii=False, default=str)
            
            print(f"SUCCESS! TBEAL report processed for Sale {sale_number} ({year}).")
            print(f"Successfully saved report data to {output_path}")
            
            # Generate manifest using the same utility (KES currency for Mombasa)
            generate_manifest(REPO_ROOT, LOCATION, f"W{sale_suffix}_{year}", currency="KES")

        except Exception as e:
            print(f"Error saving file or generating manifest: {e}")
    else:
        print(f"TBEAL processing finished for Sale {sale_number} ({year}) without valid data. No file saved.")

def process_local_sale_files():
    """
    Legacy function to find and process local files for a given sale. (Restored Logic)
    """
    print(f"--- Starting Local File Processor for Mombasa Sales ---")

    try:
        sale_no = calculate_upcoming_sale_no()
        
        # --- Define file paths based on calculated sale number ---
        zip_filename = f"{sale_no}.zip"
        txt_filename = f"Mombasa Sale {sale_no}.txt"
        current_year = datetime.date.today().year
        pdf_filename = f"Final Market Report Sale {sale_no} {current_year}.pdf"

        zip_filepath = INBOX_PATH / zip_filename
        txt_filepath = INBOX_PATH / txt_filename
        pdf_filepath = INBOX_PATH / pdf_filename
        
        temp_extract_path = INBOX_PATH / f"temp_{sale_no}"

        final_data = {
            "sale_number": sale_no,
            "retrieval_date": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "main_report_text": None,
            "final_market_report_pdf_text": None,
            "zip_attachments_content": {}
        }
        
        files_found = False

        # --- PART 1: Process the Main Text File ---
        print(f"\n[1/3] Processing main text file: {txt_filename}")
        if txt_filepath.exists():
            final_data["main_report_text"] = extract_text_from_file(txt_filepath)
            print("  -> Success.")
            shutil.move(str(txt_filepath), ARCHIVE_PATH / txt_filepath.name)
            files_found = True
        else:
            print(f"  -> Warning: File not found.")
            final_data["main_report_text"] = "File not found."

        # --- PART 2: Process the Final Market Report PDF ---
        print(f"\n[2/3] Processing Final Market Report PDF: {pdf_filename}")
        if pdf_filepath.exists():
            final_data["final_market_report_pdf_text"] = extract_text_from_file(pdf_filepath)
            print("  -> Success.")
            shutil.move(str(pdf_filepath), ARCHIVE_PATH / pdf_filepath.name)
            files_found = True
        else:
            print(f"  -> Warning: File not found.")
            final_data["final_market_report_pdf_text"] = "File not found."

        # --- PART 3: Process the ZIP File ---
        print(f"\n[3/3] Processing ZIP file: {zip_filename}")
        if zip_filepath.exists():
            temp_extract_path.mkdir(parents=True, exist_ok=True)
            print(f"  - Extracting to temporary directory...")
            with zipfile.ZipFile(zip_filepath, 'r') as zip_ref:
                zip_ref.extractall(temp_extract_path)
            
            print(f"  - Reading contents of extracted files...")
            for filepath in temp_extract_path.iterdir():
                if filepath.is_file():
                    content = extract_text_from_file(filepath)
                    final_data["zip_attachments_content"][filepath.name] = content
            
            print("  - Cleaning up temporary files...")
            shutil.rmtree(temp_extract_path)
            print("  -> Success.")
            shutil.move(str(zip_filepath), ARCHIVE_PATH / zip_filepath.name)
            files_found = True
        else:
            print(f"  -> Warning: ZIP file not found.")
            final_data["zip_attachments_content"] = {"error": "File not found."}

        # --- Standardized Saving & Auto-Manifest Mechanism ---
        if files_found:
            file_prefix = "mombasa_processed_calculated"
            sale_suffix = str(sale_no).zfill(2)
            output_filename = f"{file_prefix}_W{sale_suffix}.json"
            output_path = OUTPUT_DIR / output_filename
            
            print(f"\nSaving all extracted data to: {output_path}")
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(final_data, f, indent=4, ensure_ascii=False, default=str)

            generate_manifest(REPO_ROOT, LOCATION, sale_suffix, currency="KES")
            print("\n--- PROCESS COMPLETE ---")
        else:
            print("\n--- PROCESS COMPLETE: No files found matching the calculated sale number. ---")

    except Exception as e:
        print(f"!!! An unexpected error occurred: {e}")

if __name__ == "__main__":
    print("=== COMPREHENSIVE MOMBASA MARKET REPORTS PROCESSOR ===")
    print("This script processes:")
    print("1. TBEAL web reports (Word/HTML documents)")
    print("2. ATB reports (Image extraction + OCR)")  
    print("3. Local inbox files (ZIP, PDF, TXT)")
    print("")
    
    # Run the comprehensive TBEAL scraper for ALL reports
    print("PHASE 1: TBEAL Web Reports")
    try:
        scrape_tbeal_all_reports()
    except Exception as e:
        print(f"Error in TBEAL processing: {e}")
    
    print("\n" + "="*60)
    
    # Run the ATB reports processor
    print("PHASE 2: ATB Reports (Current Sale Week)")
    try:
        process_atb_reports()
    except Exception as e:
        print(f"Error in ATB processing: {e}")
    
    print("\n" + "="*60)
    
    # Run local file processing
    print("PHASE 3: Local File Processing")
    try:
        process_local_sale_files()
    except Exception as e:
        print(f"Error in local file processing: {e}")
    
    print("\n=== ALL MOMBASA PROCESSING COMPLETE ===")
    
    # Uncomment individual phases if you want to run them separately:
    # scrape_tbeal_all_reports()        # Only TBEAL web scraping
    # process_atb_reports()             # Only ATB image extraction  
    # process_local_sale_files()        # Only local file processing
