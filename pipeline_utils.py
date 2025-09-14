import json
import datetime
from pathlib import Path

# Updated function signature to take repo_root instead of output_dir
def generate_manifest(repo_root: Path, location: str, sale_week_or_period: str, currency: str, report_type="Consolidated Auction Report"):
    """Generates a uniquely named manifest.json file required for consolidation."""
    
    # Ensure the output directory exists for the specific location
    output_dir = repo_root / "source_reports" / location.lower()
    output_dir.mkdir(parents=True, exist_ok=True)

    # Determine if this is a Sale/Week Number (numeric) or a Period (YYYY_MM)
    is_period = "_" in sale_week_or_period and len(sale_week_or_period) == 7
    
    print(f"Generating manifest for {location} - {'Period' if is_period else 'Sale/Week'} {sale_week_or_period}...")

    # Heuristics for automation
    today = datetime.date.today()
    year = today.year
    
    if is_period:
        sale_number = 0
        week_number = 0
        date_range = f"{sale_week_or_period} (Monthly)"
        try:
            year = int(sale_week_or_period.split('_')[0])
        except ValueError:
            pass
    else:
        try:
            numeric_sale_week = int(sale_week_or_period)
            sale_number = numeric_sale_week
            week_number = numeric_sale_week
        except (ValueError, TypeError):
            print(f"Warning: Sale/Week number '{sale_week_or_period}' is invalid. Using 0.")
            sale_number = 0
            week_number = 0
        date_range = f"{today.strftime('%Y-%m-%d')} (Auto-Generated)"

    prompt_date = (today + datetime.timedelta(days=14)).strftime('%Y-%m-%d')

    manifest_data = {
        "location": location.capitalize(),
        "report_type": report_type,
        "sale_number": sale_number,
        "year": year,
        "currency": currency,
        "week_number": week_number,
        "date_range": date_range,
        "prompt_date": prompt_date
    }
    
    # Use unique manifest file names
    if is_period:
        manifest_filename = f"manifest_{sale_week_or_period}.json"
    else:
        manifest_filename = f"manifest_S{str(sale_number).zfill(2)}.json"
        
    manifest_path = output_dir / manifest_filename
    
    try:
        with open(manifest_path, 'w', encoding='utf-8') as f:
            json.dump(manifest_data, f, ensure_ascii=False, indent=4)
        print(f"Successfully generated/updated manifest at {manifest_path}")
    except Exception as e:
        print(f"Error generating manifest: {e}")
