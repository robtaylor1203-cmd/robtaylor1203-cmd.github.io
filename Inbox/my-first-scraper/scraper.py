import json
from playwright.sync_api import sync_playwright

def scrape_quotes():
    # The data we collect will be stored in this list
    scraped_data = []

    with sync_playwright() as p:
        # Launch a headless browser (headless=True means no visible window)
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        # Go to the target website
        page.goto("http://quotes.toscrape.com")
        
        # Find all elements that match the CSS selector for a quote
        quotes = page.locator("div.quote").all()
        
        # Loop through each quote element we found
        for quote_element in quotes:
            text = quote_element.locator("span.text").text_content()
            author = quote_element.locator("small.author").text_content()
            
            # Add the data as a dictionary to our list
            scraped_data.append({
                "text": text,
                "author": author
            })
        
        # Close the browser
        browser.close()
        
    # --- This is the part that saves the file ---
    output_file_name = "scraped_quotes.json"
    
    # Open the file in write mode ('w')
    with open(output_file_name, 'w', encoding='utf-8') as f:
        # Use json.dump() to write the list of data to the file
        # indent=4 makes the JSON file easy for humans to read
        json.dump(scraped_data, f, ensure_ascii=False, indent=4)
        
    print(f"Success! Data has been scraped and saved to {output_file_name}")

# Run the function
if __name__ == "__main__":
    scrape_quotes()
