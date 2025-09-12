import json
import datetime
import zipfile
import shutil
import fitz  # PyMuPDF for PDFs
import docx  # python-docx for .docx files
import openpyxl  # openpyxl for .xlsx files
from pathlib import Path

# --- Standardized Configuration (Adapted for Proven Script) ---
REPO_ROOT = Path(__file__).resolve().parent
LOCATION = "mombasa"

# Standardized INPUT directory (Note: Corrected spelling from "Momabsa" in original)
INBOX_PATH = REPO_ROOT / "Inbox" / LOCATION
INBOX_PATH.mkdir(parents=True, exist_ok=True)

# Standardized OUTPUT directory
OUTPUT_DIR = REPO_ROOT / "source_reports" / LOCATION
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Archive path (added for robustness, not in original but good practice)
ARCHIVE_PATH = INBOX_PATH / "archive"
ARCHIVE_PATH.mkdir(parents=True, exist_ok=True)

# --- Reference data for calculating the sale number (Restored) ---
REFERENCE_DATE = datetime.date(2025, 9, 1)
REFERENCE_SALE_NO = 34
# ----------------------------------------------------------------

def calculate_upcoming_sale_no():
    """
    Calculates the sale number for the upcoming Monday. (Restored Logic)
    """
    today = datetime.date.today()
    days_until_next_monday = (7 - today.weekday()) % 7
    if days_until_next_monday == 0:
        # If today is Monday, target next Monday
        days_until_next_monday = 7
        
    upcoming_monday = today + datetime.timedelta(days=days_until_next_monday)
    weeks_passed = (upcoming_monday - REFERENCE_DATE).days // 7
    upcoming_sale_no = REFERENCE_SALE_NO + weeks_passed
    
    print(f"Today's Date: {today}")
    print(f"Upcoming Monday: {upcoming_monday}")
    print(f"Calculated Upcoming Sale Number: {upcoming_sale_no}")
    
    return upcoming_sale_no

def extract_text_from_file(file_path: Path):
    """
    Intelligently extracts text from a file based on its extension. (Restored Logic)
    """
    extension = file_path.suffix.lower()
    text = ""

    print(f"  - Reading file: {file_path.name}")

    try:
        if extension == ".txt":
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                text = f.read()
        elif extension == ".pdf":
            with fitz.open(file_path) as doc:
                for page in doc:
                    text += page.get_text()
        elif extension == ".docx":
            doc = docx.Document(file_path)
            for para in doc.paragraphs:
                text += para.text + '\n'
        elif extension == ".xlsx" or extension == ".xls":
            workbook = openpyxl.load_workbook(file_path)
            for sheetname in workbook.sheetnames:
                sheet = workbook[sheetname]
                text += f"--- Sheet: {sheetname} ---\n"
                for row in sheet.iter_rows(values_only=True):
                    # Use str() explicitly for non-string cells (like dates/numbers)
                    text += "\t".join(str(cell) for cell in row if cell is not None) + "\n"
        else:
            text = f"Unsupported file type: {extension}"
            print(f"    Warning: {text}")
    except Exception as e:
        print(f"    Error extracting text from {file_path.name}: {e}")
        text = f"Error processing file: {e}"
        
    return text

def process_local_sale_files():
    """
    Main function to find and process local ZIP, TXT, and PDF files for a given sale. (Restored Logic)
    """
    print(f"--- Starting Local File Processor for Mombasa Sales ---")

    try:
        sale_no = calculate_upcoming_sale_no()
        
        # --- Define file and directory paths based on calculated sale number (Restored Logic) ---
        zip_filename = f"{sale_no}.zip"
        txt_filename = f"Mombasa Sale {sale_no}.txt"
        # Assuming the current year (2025) for the PDF name based on the original script
        current_year = datetime.date.today().year
        pdf_filename = f"Final Market Report Sale {sale_no} {current_year}.pdf"

        zip_filepath = INBOX_PATH / zip_filename
        txt_filepath = INBOX_PATH / txt_filename
        pdf_filepath = INBOX_PATH / pdf_filename
        
        temp_extract_path = INBOX_PATH / f"temp_{sale_no}"

        final_data = {
            "sale_number": sale_no,
            "retrieval_date": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "main_report_text": None,
            "final_market_report_pdf_text": None,
            "zip_attachments_content": {}
        }

        # --- PART 1: Process the Main Text File ---
        print(f"\n[1/3] Processing main text file: {txt_filename}")
        if txt_filepath.exists():
            final_data["main_report_text"] = extract_text_from_file(txt_filepath)
            print("  -> Success.")
            # Move to archive
            shutil.move(str(txt_filepath), ARCHIVE_PATH / txt_filepath.name)
        else:
            print(f"  -> Warning: File not found at {txt_filepath}")
            final_data["main_report_text"] = "File not found."

        # --- PART 2: Process the Final Market Report PDF ---
        print(f"\n[2/3] Processing Final Market Report PDF: {pdf_filename}")
        if pdf_filepath.exists():
            final_data["final_market_report_pdf_text"] = extract_text_from_file(pdf_filepath)
            print("  -> Success.")
            # Move to archive
            shutil.move(str(pdf_filepath), ARCHIVE_PATH / pdf_filepath.name)
        else:
            print(f"  -> Warning: File not found at {pdf_filepath}")
            final_data["final_market_report_pdf_text"] = "File not found."

        # --- PART 3: Process the ZIP File ---
        print(f"\n[3/3] Processing ZIP file: {zip_filename}")
        if zip_filepath.exists():
            temp_extract_path.mkdir(parents=True, exist_ok=True)
            print(f"  - Extracting to temporary directory...")
            with zipfile.ZipFile(zip_filepath, 'r') as zip_ref:
                zip_ref.extractall(temp_extract_path)
            
            print(f"  - Reading contents of extracted files...")
            for filepath in temp_extract_path.iterdir():
                if filepath.is_file():
                    content = extract_text_from_file(filepath)
                    final_data["zip_attachments_content"][filepath.name] = content
            
            print("  - Cleaning up temporary files...")
            shutil.rmtree(temp_extract_path)
            print("  -> Success.")
            # Move to archive
            shutil.move(str(zip_filepath), ARCHIVE_PATH / zip_filepath.name)
        else:
            print(f"  -> Warning: ZIP file not found at {zip_filepath}")
            final_data["zip_attachments_content"] = {"error": "File not found."}

        # --- Standardized Saving Mechanism ---
        # Note: Using 'W' (Week) prefix for consistency with Mombasa
        file_prefix = "mombasa_processed_calculated"
        sale_suffix = str(sale_no).zfill(2)
        output_filename = f"{file_prefix}_W{sale_suffix}.json"
        output_path = OUTPUT_DIR / output_filename
        
        print(f"\nSaving all extracted data to: {output_path}")
        with open(output_path, 'w', encoding='utf-8') as f:
            # Use default=str just in case any datetime objects slipped through from Excel parsing
            json.dump(final_data, f, indent=4, ensure_ascii=False, default=str)

        print("\n--- PROCESS COMPLETE ---")

    except Exception as e:
        print(f"!!! An unexpected error occurred: {e}")

if __name__ == "__main__":
    process_local_sale_files()
