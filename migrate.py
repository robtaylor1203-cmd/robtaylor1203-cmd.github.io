# This is a one-time use script to convert the old report library to the new structure.
import json

OLD_FILE = 'MASTER_REPORTS_OLD.json'
NEW_FILE = 'market-reports-library.json'

def migrate_data():
    print(f"Loading old data from {OLD_FILE}...")
    try:
        with open(OLD_FILE, 'r', encoding='utf-8') as f:
            old_data = json.load(f)
    except FileNotFoundError:
        print(f"Error: Could not find {OLD_FILE}.")
        return
    except json.JSONDecodeError:
        print(f"Error: Could not read {OLD_FILE}. It might not be a valid JSON file.")
        return

    if not isinstance(old_data, list):
        print(f"Error: {OLD_FILE} is not a JSON list. Cannot migrate.")
        return

    new_data_structure = {}
    print("Converting old data and generating new links...")

    for report in old_data:
        location = report.get('location', 'Uncategorized') 
        title = report.get('title', 'Untitled Report')

        if location not in new_data_structure:
            new_data_structure[location] = {}

        # --- THIS IS THE UPGRADE ---
        # Create a copy of the original report object
        new_report_entry = report.copy()
        # Now, add the new, correct link to it
        new_report_entry['report_link'] = f"report-viewer.html?title={title}"

        # Save the MODIFIED report object
        new_data_structure[location][title] = new_report_entry
        # --- END OF UPGRADE ---

    print(f"Conversion complete. Saving new, consistent structure to {NEW_FILE}...")
    with open(NEW_FILE, 'w', encoding='utf-8') as f:
        json.dump(new_data_structure, f, indent=4, ensure_ascii=False)

    print("Migration successful. All historical reports now have correct links.")

if __name__ == "__main__":
    migrate_data()
