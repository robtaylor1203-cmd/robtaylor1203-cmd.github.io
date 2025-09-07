import pytesseract
from PIL import Image
import os
import datetime

INPUT_DIR = "source_reports/mombasa_raw_data"

def parse_atb_multipage_ocr():
    # We'll look for all images with today's date in the filename
    date_str = datetime.datetime.now().strftime('%Y-%m-%d')
    print(f"Searching for report images from today ({date_str}) in '{INPUT_DIR}'...")

    try:
        # Find all image files that match today's date string
        todays_image_files = [
            os.path.join(INPUT_DIR, f) 
            for f in os.listdir(INPUT_DIR) 
            if date_str in f and f.lower().endswith(('.png', '.jpg', '.jpeg'))
        ]

        if not todays_image_files:
            print(f"Error: No report images for today found in '{INPUT_DIR}'.")
            return

        # Sort the files alphabetically to ensure "page_1" comes before "page_2"
        todays_image_files.sort()
        print(f"Found {len(todays_image_files)} pages to process: {todays_image_files}")

        combined_text = ""
        for image_file in todays_image_files:
            print(f"\n--- Processing {os.path.basename(image_file)} ---")
            try:
                page_text = pytesseract.image_to_string(Image.open(image_file))
                combined_text += page_text + "\n\n--- End of Page ---\n\n"
            except Exception as e:
                print(f"An error occurred during OCR on {image_file}: {e}")

        print("\n--- Successfully extracted combined text from all pages ---")
        print(combined_text)
        print("----------------------------------------------------------")

    except FileNotFoundError:
        print(f"Error: The directory '{INPUT_DIR}' does not exist.")
        return

if __name__ == "__main__":
    parse_atb_multipage_ocr()
