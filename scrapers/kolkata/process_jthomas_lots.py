import json
import pandas as pd
from pathlib import Path
import re

# Define directories
REPO_BASE = Path(__file__).resolve().parent.parent.parent
RAW_DIR = REPO_BASE / "raw_downloads" / "kolkata"
OUTPUT_DIR = REPO_BASE / "source_reports" / "kolkata"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def process_latest_raw_data():
    print("Starting J Thomas Lot Processor...")

    # 1. Find the latest raw data file
    # Glob finds all files matching the pattern, sorted() puts the latest last
    raw_files = sorted(RAW_DIR.glob('raw_lots_S*.json'))
    
    if not raw_files:
        print("ERROR: No raw data files found in raw_downloads/kolkata.")
        return

    latest_file = raw_files[-1]
    print(f"Processing file: {latest_file.name}")

    # Extract Sale Number from filename (e.g., S37)
    match = re.search(r'raw_lots_(S\d+)_', latest_file.name)
    if not match:
        print("ERROR: Could not determine Sale Number from filename.")
        return
    sale_suffix = match.group(1)

    # 2. Load data into Pandas DataFrame
    try:
        df = pd.read_json(latest_file)
    except Exception as e:
        print(f"ERROR: Could not load JSON file. Details: {e}")
        return

    # 3. Data Cleaning and Preparation
    # Convert price and packages to numeric, coercing errors (e.g., 'N/A') to NaN
    # We create new columns for the numeric values to preserve the originals
    df['price_inr_num'] = pd.to_numeric(df['price_inr'], errors='coerce')
    df['packages_num'] = pd.to_numeric(df['packages'], errors='coerce')
    
    # Drop rows where price or packages could not be converted
    df_clean = df.dropna(subset=['price_inr_num', 'packages_num'])

    print(f"Loaded {len(df)} rows. Cleaned data contains {len(df_clean)} rows.")

    # 4. Analysis: Summary Statistics
    total_packages = df_clean['packages_num'].sum()
    # Calculate weighted average price (price weighted by packages)
    # We use assign to safely create the 'total_value' column on the copy
    df_clean = df_clean.assign(total_value=df_clean['price_inr_num'] * df_clean['packages_num'])
    total_value = df_clean['total_value'].sum()
    
    weighted_avg_price = (total_value / total_packages) if total_packages > 0 else 0

    # Generate Summary Statistics JSON
    summary_stats = [
        {"metric": "Total Lots Sold", "value": f"{len(df_clean):,}"},
        {"metric": "Total Packages Sold", "value": f"{int(total_packages):,}"},
        {"metric": "Auction Average (INR/Kg)", "value": f"{weighted_avg_price:.2f}"}
    ]
    
    # Define output filename (e.g., summary_statistics_S37.json)
    output_stats_file = OUTPUT_DIR / f"summary_statistics_{sale_suffix}.json"
    with open(output_stats_file, 'w') as f:
        json.dump(summary_stats, f, indent=4)
    print(f"Generated summary statistics.")

    # 5. Analysis: Grade Category Statistics (Weighted Averages by Grade)
    # Calculate weighted average price per grade using the cleaned data
    # We use a lambda function within apply to perform the weighted calculation per group
    grade_stats = df_clean.groupby('grade').apply(
        lambda x: (x['total_value'].sum() / x['packages_num'].sum()) if x['packages_num'].sum() > 0 else 0
    ).reset_index()
    
    grade_stats.columns = ['grade', 'avg_price_inr']
    # Format the price
    grade_stats['avg_price_inr'] = grade_stats['avg_price_inr'].map('{:.2f}'.format)

    # Convert DataFrame to list of dictionaries (JSON format)
    grade_stats_list = grade_stats.to_dict(orient='records')

    # Define output filename (e.g., grade_category_statistics_S37.json)
    output_grade_file = OUTPUT_DIR / f"grade_category_statistics_{sale_suffix}.json"
    with open(output_grade_file, 'w') as f:
        json.dump(grade_stats_list, f, indent=4)
        
    print(f"Generated grade category statistics ({len(grade_stats_list)} grades).")
    print(f"\nSUCCESS: Processing complete. Data saved to {OUTPUT_DIR}")
    print("NEXT STEP: Create the manifest.json and run consolidation.")

if __name__ == "__main__":
    process_latest_raw_data()
