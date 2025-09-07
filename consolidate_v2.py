import json
from pathlib import Path
import re # Import the regular expression module

# --- Configuration ---
SOURCE_DIR = Path("source_reports")
OUTPUT_FILE = Path("market-reports-library.json")

# Regex to find unique suffixes like _S37, _W37, etc. at the end of a filename stem
# This pattern looks for _S or _W followed by digits at the end of the string.
SUFFIX_PATTERN = re.compile(r'(_[SW]\d+)$', re.IGNORECASE)

def consolidate_reports_overwrite():
    """
    Scans for manifest.json files, consolidates the data (stripping suffixes), 
    and merges it into the output file.
    If a report entry already exists, it OVERWRITES it with the fresh data.
    """

    # 1. READ: Load the existing library, or start with an empty one.
    try:
        with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
            consolidated_data = json.load(f)
        print(f"Loaded existing library from '{OUTPUT_FILE}'")
    except FileNotFoundError:
        consolidated_data = {}
        print(f"'{OUTPUT_FILE}' not found. Starting with a new library.")

    # 2. MERGE & PROCESS: Scan source folders and merge/update reports.
    found_reports = False
    for source_path in SOURCE_DIR.iterdir():
        # Ignore hidden files/folders (like .DS_Store)
        if source_path.name.startswith('.'):
            continue
            
        if source_path.is_dir():
            manifest_file = source_path / "manifest.json"

            if manifest_file.exists():
                found_reports = True
                try:
                    with open(manifest_file, 'r', encoding='utf-8') as f:
                        manifest = json.load(f)
                except json.JSONDecodeError:
                    print(f"  Error: Could not read manifest.json in {source_path.name}. Skipping.")
                    continue

                # --- Build dynamic title and description ---
                try:
                    title = f"{manifest['location']} {manifest['report_type']} - Sale {manifest['sale_number']} ({manifest['year']})"
                    description = (f"Week: {manifest['week_number']} | Dates: {manifest['date_range']} | "
                                   f"Prompt Date: {manifest['prompt_date']} | Currency: {manifest['currency']}")
                    location = manifest['location']
                except KeyError as e:
                    print(f"  Error: Missing key {e} in manifest.json in {source_path.name}. Skipping.")
                    continue
                
                # Ensure the location key exists
                if location not in consolidated_data:
                    consolidated_data[location] = {}

                # --- Overwrite Logic ---
                if title in consolidated_data[location]:
                    print(f"  * Updating existing report: '{title}'. Processing...")
                else:
                    print(f"  + New report found: '{title}'. Processing...")
                
                # Consolidate all *other* json files
                report_data_amalgamated = {}
                # Use glob to find JSON files. This does not search subdirectories (like 'archive').
                for data_file in source_path.glob('*.json'):
                    if data_file.name != 'manifest.json':
                        try:
                            with open(data_file, 'r', encoding='utf-8') as df:
                                content = json.load(df)
                                
                                # --- Strip Suffix Logic (The Fix) ---
                                # Use regex to remove the suffix if it exists
                                base_key = SUFFIX_PATTERN.sub('', data_file.stem)
                                
                                report_data_amalgamated[base_key] = content
                        except json.JSONDecodeError:
                            print(f"    Warning: Skipping malformed JSON file: {data_file.name}")

                
                # Add or Update the complete entry in the library
                consolidated_data[location][title] = {
                    "description": description,
                    "data": report_data_amalgamated
                }

    if not found_reports:
        print("\nNote: No manifest.json files found in source_reports subdirectories. No changes made.")
        return

    # 3. WRITE: Save the entire, updated library back to the file.
    print(f"\nSaving updated library to '{OUTPUT_FILE}'...")
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(consolidated_data, f, ensure_ascii=False, indent=4)
        
    print("Consolidation complete!")

# --- Run the main function ---
# We rename the function call to match the updated function name
if __name__ == "__main__":
    consolidate_reports_overwrite()
