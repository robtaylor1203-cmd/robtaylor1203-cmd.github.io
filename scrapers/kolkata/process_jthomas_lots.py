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
    print("Starting J Thomas Lot Processor (Pandas)...")

    # 1. Find the latest raw data file
    raw_files = sorted(RAW_DIR.glob('raw_lots_S*.json'))
    
    if not raw_files:
        print("ERROR: No raw data files found.")
        return

    latest_file = raw_files[-1]
    print(f"Processing file: {latest_file.name}")

    # Extract Sale Number
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
    # Convert price and packages to numeric, coercing errors to NaN
    df['price_inr_num'] = pd.to_numeric(df['price_inr'], errors='coerce')
    df['packages_num'] = pd.to_numeric(df['packages'], errors='coerce')
    
    # Drop rows where conversion failed (NaN)
    df_cleaned_stage1 = df.dropna(subset=['price_inr_num', 'packages_num']).copy()
    
    # !!! THE FIX: Explicitly filter out zero prices (Unsold Lots)
    # We only consider lots sold (price > 0) for averages and statistics.
    df_clean = df_cleaned_stage1[df_cleaned_stage1['price_inr_num'] > 0].copy()

    print(f"Loaded {len(df)} rows.")
    print(f"Rows after initial cleaning: {len(df_cleaned_stage1)}.")
    print(f"Rows after excluding zero prices (Actual Sold Lots): {len(df_clean)}.")

    # 4. Analysis: Summary Statistics (Headline Stats)
    # These are now calculated only on the actual sold lots (price > 0).
    total_packages_sold = df_clean['packages_num'].sum()
    total_lots_sold = len(df_clean)
    
    # Calculate weighted average price
    # Assign the 'total_value' column safely on the copy
    df_clean.loc[:, 'total_value'] = df_clean['price_inr_num'] * df_clean['packages_num']
    total_value = df_clean['total_value'].sum()
    
    weighted_avg_price = (total_value / total_packages_sold) if total_packages_sold > 0 else 0

    # Generate Summary Statistics JSON
    summary_stats = [
        {"metric": "Total Lots Sold", "value": f"{int(total_lots_sold):,}"},
        {"metric": "Total Packages Sold", "value": f"{int(total_packages_sold):,}"},
        {"metric": "Auction Average (INR/Kg)", "value": f"{weighted_avg_price:.2f}"}
    ]
    
    output_stats_file = OUTPUT_DIR / f"summary_statistics_{sale_suffix}.json"
    with open(output_stats_file, 'w') as f:
        json.dump(summary_stats, f, indent=4)
    print(f"Generated summary statistics.")

    # 5. Analysis: Grade Category Statistics (Weighted Averages by Grade)
    # Define an aggregation function for weighted average
    def weighted_average(group):
        if group['packages_num'].sum() > 0:
            return group['total_value'].sum() / group['packages_num'].sum()
        return 0

    # Apply the aggregation function (calculated only on sold lots)
    grade_stats = df_clean.groupby('grade').apply(weighted_average).reset_index()
    
    grade_stats.columns = ['grade', 'avg_price_inr']
    # Format the price
    grade_stats['avg_price_inr'] = grade_stats['avg_price_inr'].map('{:.2f}'.format)

    # Convert DataFrame to list of dictionaries (JSON format)
    grade_stats_list = grade_stats.to_dict(orient='records')

    output_grade_file = OUTPUT_DIR / f"grade_category_statistics_{sale_suffix}.json"
    with open(output_grade_file, 'w') as f:
        json.dump(grade_stats_list, f, indent=4)
        
    print(f"Generated grade category statistics ({len(grade_stats_list)} grades).")
    print(f"\nSUCCESS: Processing complete. Data saved to {OUTPUT_DIR}")

if __name__ == "__main__":
    process_latest_raw_data()
