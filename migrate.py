import json
from pathlib import Path

OLD_FILE = 'MASTER_REPORTS_OLD.json'
NEW_FILE = 'market-reports-library.json'

def migrate_data_definitive():
    print(f"Loading old data from {OLD_FILE}...")
    try:
        with open(OLD_FILE, 'r', encoding='utf-8') as f:
            old_data = json.load(f)
    except FileNotFoundError:
        print(f"Error: Could not find {OLD_FILE}.")
        return

    if not isinstance(old_data, list):
        print(f"Error: {OLD_FILE} is not a JSON list. Cannot migrate.")
        return

    new_data_structure = {}
    print("Standardizing all historical reports into the new data structure...")

    for report in old_data:
        location = report.get('location') or report.get('auction_centre', 'Uncategorized') 
        title = report.get('title', 'Untitled Report')

        if location not in new_data_structure:
            new_data_structure[location] = {}

        # Create a new, clean report object in the standard format
        standardized_report = {
            "title": title,
            "description": report.get('description', ''),
            "location": location,
            "auction_centre": location,
            "year": report.get('year'),
            "week_number": report.get('week_number'),
            "sale_number": report.get('sale_number'),
            "source": report.get('source', 'Historical'),
            "currency": report.get('currency'),
            "data": {} # Create a sub-dictionary for the main data
        }

        # Move all other keys into the 'data' sub-dictionary
        for key, value in report.items():
            if key not in standardized_report:
                standardized_report['data'][key] = value

        # Generate the new, correct link
        standardized_report['report_link'] = f"report-viewer.html?title={title}"

        # Save the fully standardized report object
        new_data_structure[location][title] = standardized_report

    print(f"Conversion complete. Saving standardized library to {NEW_FILE}...")
    with open(NEW_FILE, 'w', encoding='utf-8') as f:
        json.dump(new_data_structure, f, indent=4, ensure_ascii=False)

    print("Definitive migration successful.")

if __name__ == "__main__":
    migrate_data_definitive()
