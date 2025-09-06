import json
from pathlib import Path

# --- Configuration ---
SOURCE_DIR = Path("source_reports")
OUTPUT_FILE = Path("market-reports-library.json")

def consolidate_reports_append_only():
    """
    Scans for manifest.json files, consolidates the data, and merges it
    into the existing output file without overwriting existing entries.
    """
    
    # 1. READ: Load the existing library, or start with an empty one if it doesn't exist.
    try:
        with open(OUTPUT_FILE, 'r', encoding='utf-8') as f:
            consolidated_data = json.load(f)
        print(f"Loaded existing library from '{OUTPUT_FILE}'")
    except FileNotFoundError:
        consolidated_data = {}
        print(f"'{OUTPUT_FILE}' not found. Starting with a new library.")

    # 2. MERGE: Scan source folders and merge new reports.
    for source_path in SOURCE_DIR.iterdir():
        if source_path.is_dir():
            manifest_file = source_path / "manifest.json"
            
            if manifest_file.exists():
                with open(manifest_file, 'r', encoding='utf-8') as f:
                    manifest = json.load(f)
                
                title = f"{manifest['location']} {manifest['report_type']} - Sale {manifest['sale_number']} ({manifest['year']})"
                description = (f"Week: {manifest['week_number']} | Dates: {manifest['date_range']} | "
                               f"Prompt Date: {manifest['prompt_date']} | Currency: {manifest['currency']}")
                
                location = manifest['location']
                
                if location not in consolidated_data:
                    consolidated_data[location] = {}

                if title not in consolidated_data[location]:
                    print(f"  + New report found: '{title}'. Processing...")
                    
                    report_data_amalgamated = {}
                    for data_file in source_path.glob('*.json'):
                        if data_file.name != 'manifest.json':
                            with open(data_file, 'r', encoding='utf-8') as df:
                                report_data_amalgamated[data_file.stem] = json.load(df)
                    
                    consolidated_data[location][title] = {
                        "title": title,
                        "description": description,
                        "location": location,
                        "auction_centre": location,
                        "year": manifest.get('year'),
                        "week_number": manifest.get('week_number'),
                        "source": manifest.get('source', 'Consolidated'),
                        "report_link": f"report-viewer.html?dataset={location}_{manifest.get('sale_number', 'report')}",
                        "data": report_data_amalgamated
                    }
                else:
                    print(f"  - Skipping report '{title}' as it already exists in the library.")

    # 3. WRITE: Save the entire, updated library back to the file.
    # This block is now CORRECTLY INSIDE the function.
    print(f"\nSaving updated library to '{OUTPUT_FILE}'...")
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        json.dump(consolidated_data, f, ensure_ascii=False, indent=4)
        
    print("Consolidation complete!")

# --- Run the main function ---
if __name__ == "__main__":
    consolidate_reports_append_only()
