import json
import datetime
from pathlib import Path

def generate_manifest(output_dir: Path, location: str, sale_week_number: str, currency: str, report_type="Consolidated Auction Report"):
    """Generates the manifest.json file required for consolidation."""
    print(f"Generating manifest for {location} Sale/Week {sale_week_number}...")

    # Heuristics to determine metadata for automation
    today = datetime.date.today()
    year = today.year
    
    # Clean the sale/week number
    try:
        numeric_sale_week = int(sale_week_number)
    except (ValueError, TypeError):
        print(f"Warning: Sale/Week number '{sale_week_number}' is invalid. Using 0.")
        numeric_sale_week = 0
    
    # Simplified date estimation - these are placeholders for automation
    date_range = f"{today.strftime('%Y-%m-%d')} (Auto-Generated)"
    prompt_date = (today + datetime.timedelta(days=14)).strftime('%Y-%m-%d')

    manifest_data = {
        "location": location.capitalize(),
        "report_type": report_type,
        "sale_number": numeric_sale_week,
        "year": year,
        "currency": currency,
        "week_number": numeric_sale_week,
        "date_range": date_range,
        "prompt_date": prompt_date
    }

    manifest_path = output_dir / "manifest.json"
    try:
        # This overwrites the manifest if multiple scripts run for the same location.
        with open(manifest_path, 'w', encoding='utf-8') as f:
            json.dump(manifest_data, f, ensure_ascii=False, indent=4)
        print(f"Successfully generated/updated manifest at {manifest_path}")
    except Exception as e:
        print(f"Error generating manifest: {e}")
