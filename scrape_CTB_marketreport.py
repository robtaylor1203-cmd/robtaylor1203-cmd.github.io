import json
import datetime
import time
import re
from playwright.sync_api import sync_playwright, TimeoutError
from pathlib import Path
from pipeline_utils import generate_manifest

# --- Configuration ---
REPO_ROOT = Path(__file__).resolve().parent
LANDING_PAGE_URL = "https://ceylonteabrokers.com/all-market-reports/"
BASE_URL = "https://ceylonteabrokers.com"

# Enhanced timeouts
MAX_TIMEOUT = 3600000  # 60 minutes
DISCOVERY_TIMEOUT = 600000  # 10 minutes
STABILIZATION_WAIT = 120000  # 2 minutes
BI_WAIT_TIMEOUT = 300000  # 5 minutes for BI reports
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'

# Ceylon Tea Brokers is single location (Colombo)
LOCATION_FOLDER = "colombo"

def determine_sale_number(report_title):
    """Helper function to determine sale number accurately."""
    # Strategy 1: Look for explicit "Sale No." or "Week"
    match = re.search(r'(?:Sale No\.|Week)[\s]*(\d{1,2})', report_title, re.IGNORECASE)
    if match:
        return match.group(1).zfill(2)

    # Strategy 2: Look for "Market Report" followed by number
    match = re.search(r'Market Report.*?(\d{1,2})', report_title, re.IGNORECASE)
    if match:
        return match.group(1).zfill(2)

    # Strategy 3: Calculate ISO Week Number from the date in the title
    date_match = re.search(r'(\d{1,2})(?:st|nd|rd|th)?(?:\s*&\s*\d{1,2}(?:st|nd|rd|th)?)?\s+([A-Za-z]+)\s+(\d{4})', report_title)
    if date_match:
        day, month, year = date_match.groups()
        try:
            date_str = f"{day} {month} {year}"
            report_date = datetime.datetime.strptime(date_str, "%d %B %Y").date()
            iso_week = report_date.isocalendar()[1]
            print(f"Calculated ISO Week Number: {iso_week} from date: {report_date}")
            return str(iso_week).zfill(2)
        except ValueError:
            print(f"Warning: Could not parse date from title: {report_title}")
            return None
            
    return None

def get_all_available_reports(page):
    """Helper to discover all available market reports with enhanced waiting."""
    print("Discovering all available Ceylon Tea Brokers market reports...")
    
    page.wait_for_load_state("networkidle", timeout=DISCOVERY_TIMEOUT)
    print("Initial page load complete. Waiting 15 seconds for stabilization...")
    page.wait_for_timeout(15000)
    
    print("Waiting for market report links to load...")
    page.wait_for_function("""
        () => {
            const links = document.querySelectorAll('a[href*="market-report"]');
            return links.length > 0;
        }
    """, timeout=DISCOVERY_TIMEOUT)
    
    print("Market report links detected. Extracting all reports...")
    
    report_links = []
    
    # Strategy 1: Look for links with "Market Report" in the text
    try:
        links_strategy1 = page.get_by_role("link", name=re.compile(r"Market Report.*?\d+", re.IGNORECASE)).all()
        for link in links_strategy1:
            try:
                title = link.inner_text().strip()
                href = link.get_attribute("href")
                if title and href:
                    report_links.append({"title": title, "href": href, "element": link})
                    print(f"  Found via strategy 1: {title}")
            except:
                continue
    except Exception as e:
        print(f"Strategy 1 failed: {e}")
    
    # Strategy 2: Look for links containing "market-report" in href
    try:
        all_links = page.locator('a[href*="market-report"]').all()
        for link in all_links:
            try:
                title = link.inner_text().strip()
                href = link.get_attribute("href")
                if title and href and "Market Report" in title:
                    # Avoid duplicates
                    if not any(r["href"] == href for r in report_links):
                        report_links.append({"title": title, "href": href, "element": link})
                        print(f"  Found via strategy 2: {title}")
            except:
                continue
    except Exception as e:
        print(f"Strategy 2 failed: {e}")
    
    # Extract sale numbers and filter valid reports
    valid_reports = []
    for report in report_links:
        sale_number = determine_sale_number(report["title"])
        if sale_number:
            # Determine year from title or use current year
            year_match = re.search(r'(\d{4})', report["title"])
            year = year_match.group(1) if year_match else str(datetime.datetime.now().year)
            
            report["sale_number"] = sale_number
            report["year"] = year
            valid_reports.append(report)
            print(f"  Valid report: {report['title']} -> Sale {sale_number} ({year})")
    
    # Sort by year and sale number (newest first)
    valid_reports.sort(key=lambda x: (int(x['year']), int(x['sale_number'])), reverse=True)
    
    print(f"Found {len(valid_reports)} valid market reports")
    return valid_reports

def extract_enhanced_bi_data(sway_page):
    """Enhanced BI data extraction with multiple techniques for Power BI embedded content."""
    print("Attempting enhanced BI data extraction...")
    embedded_bi_data = []
    
    try:
        # First, click all 'View' buttons to load BI reports
        view_buttons = sway_page.get_by_role("button", name="View")
        view_button_count = view_buttons.count()

        if view_button_count > 0:
            print(f"Found {view_button_count} 'View' button(s). Clicking them all...")
            for i in range(view_button_count):
                try:
                    view_buttons.nth(i).click()
                    print(f"Clicked 'View' button #{i+1}")
                    sway_page.wait_for_timeout(3000)
                except:
                    print(f"Warning: Could not click View button #{i+1}")
            
            print(f"Clicks complete. Waiting for BI reports to load...")
            sway_page.wait_for_timeout(20000)
        
        # Strategy 1: Wait for and extract from Power BI iframes
        try:
            print(f"Waiting for Power BI iframes to load...")
            first_iframe = sway_page.locator('iframe[src*="powerbi.com"]').first
            first_iframe.wait_for(timeout=BI_WAIT_TIMEOUT)
            print("Power BI iframes detected.")
            
            iframe_elements = sway_page.locator('iframe[src*="powerbi.com"]')
            iframe_count = iframe_elements.count()

            if iframe_count > 0:
                print(f"Found {iframe_count} Power BI iframe(s). Extracting data...")
                for i in range(iframe_count):
                    try:
                        iframe = iframe_elements.nth(i)
                        frame = iframe.content_frame()
                        
                        if frame:
                            frame.wait_for_load_state("networkidle", timeout=90000)
                            
                            bi_data = {}
                            
                            # Extract visible text
                            try:
                                frame.wait_for_selector("div", timeout=45000)
                                bi_text = frame.locator("body").inner_text()
                                if bi_text.strip():
                                    bi_data["text_content"] = bi_text.strip()
                            except:
                                bi_data["text_content"] = "Could not extract text content"
                            
                            # Look for table data
                            try:
                                tables = frame.locator("table").all()
                                if tables:
                                    table_data = []
                                    for j, table in enumerate(tables):
                                        table_text = table.inner_text()
                                        if table_text.strip():
                                            table_data.append(f"Table {j+1}: {table_text}")
                                    bi_data["tables"] = table_data
                            except:
                                pass
                            
                            # Look for visual containers
                            try:
                                visuals = frame.locator("div.visual-container, div.visual, div[data-automation-key]").all()
                                if visuals:
                                    visual_data = []
                                    for j, visual in enumerate(visuals):
                                        visual_text = visual.inner_text()
                                        if visual_text.strip():
                                            visual_data.append(f"Visual {j+1}: {visual_text}")
                                    bi_data["visuals"] = visual_data
                            except:
                                pass
                            
                            bi_data["iframe_index"] = i + 1
                            bi_data["extraction_timestamp"] = datetime.datetime.now().isoformat()
                            embedded_bi_data.append(bi_data)
                            print(f"Successfully extracted enhanced data from Power BI report #{i+1}")
                            
                    except Exception as e:
                        print(f"Warning: Could not extract data from Power BI report #{i+1}: {e}")
                        embedded_bi_data.append({
                            "iframe_index": i + 1,
                            "error": str(e),
                            "status": "extraction_failed"
                        })
            else:
                print("No Power BI iframes found after waiting.")
                
        except TimeoutError:
            print("Timeout: No Power BI reports loaded within the timeout period.")
        
        # Strategy 2: Look for any embedded content that might contain BI data
        try:
            print("Searching for alternative embedded content...")
            embedded_elements = sway_page.locator("embed, object, div[data-powerbi], div[data-embed]").all()
            if embedded_elements:
                print(f"Found {len(embedded_elements)} additional embedded elements")
                for i, element in enumerate(embedded_elements):
                    try:
                        element_data = {
                            "element_type": element.evaluate("el => el.tagName"),
                            "content": element.inner_text() if element.inner_text().strip() else "No text content",
                            "attributes": element.evaluate("el => Array.from(el.attributes).reduce((acc, attr) => ({...acc, [attr.name]: attr.value}), {})")
                        }
                        embedded_bi_data.append({
                            "type": "alternative_embedded",
                            "index": i + 1,
                            "data": element_data
                        })
                        print(f"Extracted alternative embedded content #{i+1}")
                    except:
                        continue
        except Exception as e:
            print(f"Alternative extraction failed: {e}")
    
    except Exception as e:
        print(f"Enhanced BI extraction failed: {e}")
        embedded_bi_data.append({
            "error": str(e),
            "status": "complete_extraction_failed"
        })
    
    print(f"BI extraction complete. Extracted {len(embedded_bi_data)} BI data objects.")
    return embedded_bi_data

def scrape_ceylon_tea_brokers_all_reports():
    print(f"Starting Ceylon Tea Brokers Market Reports scraper - ALL REPORTS (Enhanced Version)")
    print("\nIMPORTANT: This process may take 90+ minutes. Enhanced BI data extraction enabled.\n")

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
            
            print("Navigating to all market reports page...")
            page.goto(LANDING_PAGE_URL, wait_until="networkidle", timeout=DISCOVERY_TIMEOUT)
            
            # Handle cookie consent
            try:
                page.get_by_role("button", name="Accept all").click(timeout=8000)
                print("Cookie consent banner accepted.")
            except TimeoutError:
                print("Cookie consent banner not found or already accepted.")
            
            reports = get_all_available_reports(page)
            page.close()

            if not reports:
                print("Warning: Could not find any market reports.")
                return

            # Limit to latest reports for practical purposes
            max_reports = 15  # Process up to 15 latest reports
            reports_to_process = reports[:max_reports]
            print(f"\nProcessing {len(reports_to_process)} latest reports (out of {len(reports)} found)...")

            for i, report in enumerate(reports_to_process):
                report_title = report['title']
                report_href = report['href']
                sale_number = report['sale_number']
                year = report['year']
                
                print(f"\n{'='*20} Processing Report {i+1}/{len(reports_to_process)}: {report_title} (Sale #{sale_number}) {'='*20}")
                
                process_single_report(context, report_title, report_href, sale_number, year)

        except Exception as e:
            print(f"Critical error occurred: {e}")
        finally:
            browser.close()
            print("Browser closed.")

def process_single_report(context, report_title, report_href, sale_number, year):
    """Handles the scraping logic for one specific Ceylon Tea Brokers report."""
    page = None
    sway_page = None

    try:
        page = context.new_page()
        page.set_default_timeout(MAX_TIMEOUT)
        page.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        if not report_href.startswith('http'):
            report_url = BASE_URL + report_href
        else:
            report_url = report_href
            
        print(f"Navigating to report: {report_url}")
        page.goto(report_url, wait_until="networkidle", timeout=300000)
        
        # Handle cookie consent if it appears again
        try:
            page.get_by_role("button", name="Accept all").click(timeout=5000)
        except TimeoutError:
            pass
        
        print("Looking for 'View Market Report' link...")
        try:
            view_market_report_link = page.locator("a:has-text('View Market Report')").first
            view_market_report_link.wait_for(timeout=90000)
            
            print("Clicking 'View Market Report' and waiting for new Sway tab...")
            with page.context.expect_page(timeout=180000) as new_page_info:
                view_market_report_link.click()

            sway_page = new_page_info.value
            print(f"Switched to new Sway tab: {sway_page.url}")
            sway_page.wait_for_load_state("networkidle", timeout=300000)

        except Exception as e:
            print(f"Warning: Could not access Sway presentation for {report_title}: {e}")
            return

        print("Scrolling Sway presentation to discover all content...")
        last_height = -1
        scroll_attempts = 0
        max_scroll_attempts = 25
        
        while scroll_attempts < max_scroll_attempts:
            sway_page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            sway_page.wait_for_timeout(4000)
            new_height = sway_page.evaluate("document.body.scrollHeight")
            if new_height == last_height:
                print("Reached the end of the report.")
                break
            last_height = new_height
            scroll_attempts += 1

        print("Extracting main text content...")
        body_element = sway_page.locator("body")
        full_text = body_element.inner_text()

        print("Starting enhanced BI data extraction...")
        embedded_bi_data = extract_enhanced_bi_data(sway_page)

        output_data = {
            "report_title": report_title,
            "sale_week_number": sale_number,
            "year": year,
            "source_url": sway_page.url,
            "report_page_url": report_href,
            "retrieval_date": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "raw_text": full_text,
            "embedded_bi_data": embedded_bi_data,
            "bi_extraction_summary": {
                "total_bi_objects": len(embedded_bi_data),
                "successful_extractions": len([bi for bi in embedded_bi_data if "error" not in bi]),
                "failed_extractions": len([bi for bi in embedded_bi_data if "error" in bi])
            }
        }

        if not sway_page.is_closed():
            sway_page.close()

    except Exception as e:
        print(f"\nERROR: Failed during report processing for {report_title}. Details: {e}")
        if page and not page.is_closed():
            try:
                page.screenshot(path=f'error_screenshot_ctb_S{sale_number}_{year}.png')
                print(f"Saved error screenshot")
            except:
                pass
        return

    finally:
        if sway_page and not sway_page.is_closed():
            sway_page.close()
        if page and not page.is_closed():
            page.close()

    if output_data and sale_number and year:
        OUTPUT_DIR = REPO_ROOT / "source_reports" / LOCATION_FOLDER
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

        sale_suffix = str(sale_number).zfill(2)
        filename = f"CTB_report_enhanced_S{sale_suffix}_{year}.json"
        output_path = OUTPUT_DIR / filename

        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=4, ensure_ascii=False)
            
            print(f"\nSUCCESS! Ceylon Tea Brokers report processed for Sale {sale_number} ({year}).")
            print(f"BI extraction summary: {output_data['bi_extraction_summary']}")
            print(f"Successfully saved report data to {output_path}")
            
            generate_manifest(REPO_ROOT, LOCATION_FOLDER, f"{sale_suffix}_{year}", currency="LKR")

        except Exception as e:
            print(f"Error saving file or generating manifest: {e}")
    else:
        print(f"Report processing finished for {report_title} without complete data.")

if __name__ == "__main__":
    scrape_ceylon_tea_brokers_all_reports()
