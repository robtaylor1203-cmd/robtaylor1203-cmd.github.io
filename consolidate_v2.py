import json
from pathlib import Path
import re
from datetime import datetime

# --- Configuration ---
SOURCE_DIR = Path("source_reports")
OUTPUT_FILE = Path("market-reports-library.json")

# Enhanced regex patterns for various file naming conventions
SUFFIX_PATTERN = re.compile(r'(_[SW]\d+)$', re.IGNORECASE)
YEAR_SUFFIX_PATTERN = re.compile(r'(_[SW]\d+_\d{4})$', re.IGNORECASE)  # For _S01_2024, _W15_2025 etc.
MANIFEST_PATTERN = re.compile(r'^manifest(_[SW]?\d+)?(_\d{4})?(_[A-Z]{3})?\.json$', re.IGNORECASE)

def find_manifest_files(source_path):
    """Find all manifest files in a directory using the new naming pattern."""
    manifest_files = []
    
    # Look for various manifest file patterns:
    # manifest.json, manifest_S01_2024_KES.json, manifest_W15_2025_INR.json, etc.
    for json_file in source_path.glob('manifest*.json'):
        if MANIFEST_PATTERN.match(json_file.name):
            manifest_files.append(json_file)
    
    return manifest_files

def extract_year_from_filename(filename):
    """Extract year from filename if present."""
    year_match = re.search(r'(\d{4})', filename)
    return year_match.group(1) if year_match else None

def extract_sale_from_filename(filename):
    """Extract sale/week number from filename if present."""
    sale_match = re.search(r'[SW](\d+)', filename, re.IGNORECASE)
    return sale_match.group(1).zfill(2) if sale_match else None

def consolidate_reports_enhanced():
    """
    Enhanced consolidation that handles:
    - Multiple manifest files per location
    - Year-aware report organization
    - New naming patterns from enhanced scrapers
    - Proper deduplication with year separation
    """

    # 1. READ: Load the existing library, or start with an empty one.
    try:
        with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
            consolidated_data = json.load(f)
        print(f"Loaded existing library from '{OUTPUT_FILE}'")
    except FileNotFoundError:
        consolidated_data = {}
        print(f"'{OUTPUT_FILE}' not found. Starting with a new library.")

    # Statistics tracking
    total_reports_processed = 0
    new_reports_added = 0
    updated_reports = 0

    # 2. MERGE & PROCESS: Scan source folders and merge/update reports.
    found_reports = False
    
    for source_path in SOURCE_DIR.iterdir():
        # Ignore hidden files/folders (like .DS_Store)
        if source_path.name.startswith('.'):
            continue

        if source_path.is_dir():
            location_name = source_path.name
            print(f"\nProcessing location: {location_name}")
            
            # Find all manifest files in this location
            manifest_files = find_manifest_files(source_path)
            
            if not manifest_files:
                print(f"  No manifest files found in {location_name}")
                continue
            
            found_reports = True
            
            # Ensure the location key exists
            if location_name not in consolidated_data:
                consolidated_data[location_name] = {}
            
            # Process each manifest file
            for manifest_file in manifest_files:
                print(f"  Processing manifest: {manifest_file.name}")
                
                try:
                    with open(manifest_file, 'r', encoding='utf-8') as f:
                        manifest = json.load(f)
                except json.JSONDecodeError:
                    print(f"    Error: Could not read {manifest_file.name}. Skipping.")
                    continue

                # --- Build enhanced title and description ---
                try:
                    # Extract year from manifest or filename
                    year = manifest.get('year') or extract_year_from_filename(manifest_file.name) or datetime.now().year
                    
                    # Build title with year for better organization
                    base_title = f"{manifest['location']} {manifest['report_type']} - Sale {manifest['sale_number']}"
                    if str(year) not in base_title:
                        title = f"{base_title} ({year})"
                    else:
                        title = base_title
                    
                    description = (f"Year: {year} | Week: {manifest['week_number']} | "
                                   f"Dates: {manifest['date_range']} | "
                                   f"Prompt Date: {manifest['prompt_date']} | "
                                   f"Currency: {manifest['currency']}")
                    
                    location = manifest['location']
                    
                except KeyError as e:
                    print(f"    Error: Missing key {e} in {manifest_file.name}. Skipping.")
                    continue

                # --- Enhanced Overwrite Logic ---
                is_update = title in consolidated_data[location]
                if is_update:
                    print(f"    * Updating existing report: '{title}'")
                    updated_reports += 1
                else:
                    print(f"    + New report found: '{title}'")
                    new_reports_added += 1

                # Consolidate all *other* json files for this manifest's timeframe
                report_data_amalgamated = {}
                
                # Get the sale/week identifier from the manifest file
                manifest_sale = extract_sale_from_filename(manifest_file.name)
                manifest_year = extract_year_from_filename(manifest_file.name)
                
                # Use glob to find JSON files matching this sale/week
                for data_file in source_path.glob('*.json'):
                    if data_file.name.startswith('manifest'):
                        continue  # Skip all manifest files
                    
                    # Check if this data file belongs to the same sale/week/year as the manifest
                    data_file_sale = extract_sale_from_filename(data_file.name)
                    data_file_year = extract_year_from_filename(data_file.name)
                    
                    # Include file if:
                    # 1. It matches the manifest's sale number AND year, OR
                    # 2. No sale/year info available (legacy files)
                    should_include = (
                        (manifest_sale and data_file_sale and manifest_sale == data_file_sale and 
                         manifest_year and data_file_year and manifest_year == data_file_year) or
                        (not manifest_sale or not data_file_sale or not manifest_year or not data_file_year)
                    )
                    
                    if should_include:
                        try:
                            with open(data_file, 'r', encoding='utf-8') as df:
                                content = json.load(df)

                                # --- Enhanced Suffix Stripping Logic ---
                                # Remove year-aware suffixes first, then legacy suffixes
                                base_key = YEAR_SUFFIX_PATTERN.sub('', data_file.stem)
                                base_key = SUFFIX_PATTERN.sub('', base_key)

                                report_data_amalgamated[base_key] = content
                                print(f"      Included: {data_file.name} -> {base_key}")
                                
                        except json.JSONDecodeError:
                            print(f"      Warning: Skipping malformed JSON file: {data_file.name}")

                # Add or Update the complete entry in the library
                consolidated_data[location][title] = {
                    "description": description,
                    "manifest": manifest,  # Include full manifest for reference
                    "data": report_data_amalgamated
                }
                
                total_reports_processed += 1

    if not found_reports:
        print("\nNote: No manifest files found in source_reports subdirectories. No changes made.")
        return

    # 3. WRITE: Save the entire, updated library back to the file.
    print(f"\nSaving updated library to '{OUTPUT_FILE}'...")
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(consolidated_data, f, ensure_ascii=False, indent=4)

    # 4. SUMMARY: Print consolidation statistics
    print("\n" + "="*50)
    print("CONSOLIDATION COMPLETE!")
    print("="*50)
    print(f"Total reports processed: {total_reports_processed}")
    print(f"New reports added: {new_reports_added}")
    print(f"Existing reports updated: {updated_reports}")
    
    # Print summary by location
    print(f"\nReports by location:")
    for location, reports in consolidated_data.items():
        print(f"  {location}: {len(reports)} reports")
        
        # Show year distribution for each location
        years = {}
        for report_title in reports.keys():
            year_match = re.search(r'\((\d{4})\)', report_title)
            if year_match:
                year = year_match.group(1)
                years[year] = years.get(year, 0) + 1
        
        if years:
            year_summary = ", ".join([f"{year}: {count}" for year, count in sorted(years.items(), reverse=True)])
            print(f"    Years: {year_summary}")

    print(f"\nLibrary saved to: {OUTPUT_FILE}")
    print("="*50)

def validate_library():
    """Optional: Validate the consolidated library structure."""
    try:
        with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        print(f"\nLibrary validation:")
        print(f"  Total locations: {len(data)}")
        
        total_reports = sum(len(reports) for reports in data.values())
        print(f"  Total reports across all locations: {total_reports}")
        
        # Check for potential duplicates (same sale number, different years)
        for location, reports in data.items():
            sale_numbers = {}
            for title in reports.keys():
                sale_match = re.search(r'Sale (\d+)', title)
                if sale_match:
                    sale_num = sale_match.group(1)
                    if sale_num not in sale_numbers:
                        sale_numbers[sale_num] = []
                    sale_numbers[sale_num].append(title)
            
            multi_year_sales = {k: v for k, v in sale_numbers.items() if len(v) > 1}
            if multi_year_sales:
                print(f"    {location} - Multi-year sales detected:")
                for sale_num, titles in multi_year_sales.items():
                    print(f"      Sale {sale_num}: {len(titles)} versions ({', '.join([re.search(r'\((\d{4})\)', t).group(1) if re.search(r'\((\d{4})\)', t) else 'unknown' for t in titles])})")
        
    except Exception as e:
        print(f"Validation error: {e}")

# --- Run the main function ---
if __name__ == "__main__":
    consolidate_reports_enhanced()
    validate_library()
