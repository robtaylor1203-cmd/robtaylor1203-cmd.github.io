import pandas as pd
import json
from pathlib import Path

# --- Configuration ---
REPO_ROOT = Path(__file__).resolve().parent
LIBRARY_FILE = REPO_ROOT / "market-reports-library.json"
# Define output directory for the Data page analysis
ANALYSIS_OUTPUT = REPO_ROOT / "Data" / "analysis"
ANALYSIS_OUTPUT.mkdir(parents=True, exist_ok=True)
# ---------------------

def analyze_market_data():
    print(f"Starting data analysis (Pandas) from {LIBRARY_FILE}...")

    # 1. Load the consolidated library
    try:
        if not LIBRARY_FILE.exists() or LIBRARY_FILE.stat().st_size == 0:
            print("Library file not found or empty. Skipping analysis.")
            return
        with open(LIBRARY_FILE, 'r', encoding='utf-8') as f:
            library_data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"Error decoding library file: {e}")
        return

    # 2. Flatten the data structure for Pandas
    all_records = []
    
    # Iterate through locations (Kolkata, Mombasa, etc.)
    for location, reports in library_data.items():
        # Iterate through specific reports (Sale 37, Sale 36, etc.)
        for report_title, report_content in reports.items():
            
            # Extract metadata from the description block (populated by the manifest)
            description = report_content.get("description", {})
            metadata = {
                "location": location,
                "report_title": report_title,
                "sale_number": description.get("sale_number"),
                "year": description.get("year"),
                "currency": description.get("currency"),
            }
            
            # Process the raw data components
            data_components = report_content.get("data", {})
            
            # --- Data Type Specific Processing ---
            
            # Process J Thomas Auction Lots (Kolkata)
            for key, data in data_components.items():
                # Check if the key indicates auction lots data
                if "JT_auction_lots_stealth" in key and isinstance(data, list):
                    for lot in data:
                        # Combine metadata with lot data
                        record = metadata.copy()
                        record.update(lot)
                        record["data_type"] = "JT_auction_lot"
                        all_records.append(record)

            # (Add processing logic for other structured data types here, e.g., Forbes OCR tables)

    # 3. Create the Master DataFrame (Centralized Database)
    df = pd.DataFrame(all_records)

    if df.empty:
        print("No structured records processed.")
        return

    print(f"Centralized Database: Processed {len(df)} records into the master DataFrame.")

    # 4. Data Cleaning and Transformation
    if "JT_auction_lot" in df["data_type"].unique():
        # Convert relevant columns to numeric types for analysis
        numeric_cols = ['price_inr', 'packages']
        for col in numeric_cols:
            if col in df.columns:
                # Clean data (remove commas, essential for INR)
                if df[col].dtype == 'object':
                     df[col] = df[col].str.replace(',', '', regex=False)
                # Convert to numeric, coercing errors into NaN (Not a Number)
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Ensure Sale Number is numeric
        if 'sale_number' in df.columns:
            df['sale_number'] = pd.to_numeric(df['sale_number'], errors='coerce')


    # 5. Analysis Example: Summary Stats for Data Page
    if "JT_auction_lot" in df["data_type"].unique() and 'grade' in df.columns:
        print("Analyzing JT Auction Lots...")
        
        # Filter for valid data (where price and sale number are known)
        lots_df = df[(df["data_type"] == "JT_auction_lot") & (df['price_inr'].notna()) & (df['sale_number'].notna())].copy()
        
        if not lots_df.empty:
            # Calculate summary statistics by Location, Sale Number, and Grade
            summary_stats = lots_df.groupby(['location', 'sale_number', 'grade']).agg(
                average_price_inr=('price_inr', 'mean'),
                max_price_inr=('price_inr', 'max'),
                lot_count=('lot_no', 'count')
            ).reset_index()

            # Formatting
            summary_stats['average_price_inr'] = summary_stats['average_price_inr'].round(2)

            # Save the analysis output (CSV format for easy use on the Data page)
            output_file = ANALYSIS_OUTPUT / "JT_grade_summary.csv"
            summary_stats.to_csv(output_file, index=False)
            print(f"Analysis complete. Summary saved to {output_file}")

if __name__ == "__main__":
    analyze_market_data()
