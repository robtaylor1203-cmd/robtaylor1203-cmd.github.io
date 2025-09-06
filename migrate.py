# This is a one-time use script to convert the old report library to the new structure.
import json

OLD_FILE = 'MASTER_REPORTS_OLD.json'
NEW_FILE = 'market-reports-library.json'

def migrate_data():
    print(f"Loading old data from {OLD_FILE}...")
    try:
        with open(OLD_FILE, 'r', encoding='utf-8') as f:
            # This assumes your old file is a LIST of reports
            old_data = json.load(f)
    except FileNotFoundError:
        print(f"Error: Could not find {OLD_FILE}. Please make sure it's in the project folder.")
        return
    except json.JSONDecodeError:
        print(f"Error: Could not read {OLD_FILE}. It might not be a valid JSON file.")
        return

    if not isinstance(old_data, list):
        print(f"Error: {OLD_FILE} is not a JSON list. Cannot migrate automatically.")
        return

    new_data_structure = {}
    print("Converting old list structure to new dictionary structure...")

    for report in old_data:
        # === YOU MAY NEED TO EDIT THESE TWO LINES ===
        # Adjust 'location' and 'title' to match the actual key names in your old master file.
        # For example, if your old file used 'report_location', change 'location' to 'report_location'.
        location = report.get('location', 'Uncategorized') 
        title = report.get('title', 'Untitled Report')

        if location not in new_data_structure:
            new_data_structure[location] = {}

        # We will store the whole original report object under the new key structure
        new_data_structure[location][title] = report

    print(f"Conversion complete. Saving new structure to {NEW_FILE}...")
    with open(NEW_FILE, 'w', encoding='utf-8') as f:
        json.dump(new_data_structure, f, indent=4, ensure_ascii=False)

    print("Migration successful.")

if __name__ == "__main__":
    migrate_data()
