#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
TitlePro247 Foreclosure Data Fetcher
------------------------------------
- Reads Excel with Address, City columns
- Logs into TitlePro247 and searches for foreclosure data
- Saves results with Loan Amount, NOD, Sale Date, etc.
"""

import argparse
import time
import pandas as pd
import re
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
from tqdm import tqdm

def login_titlepro(page, username, password):
    """Simple but robust login"""
    print("🔐 Logging into TitlePro247...")
    
    # Navigate to login page
    page.goto("https://www.titlepro247.com", timeout=30000)
    page.wait_for_load_state("domcontentloaded")
    
    # Handle cookies if present
    try:
        cookie_selectors = [
            "button:has-text('Accept')",
            "button:has-text('Agree')", 
            "#onetrust-accept-btn-handler",
            "button[id*='accept']"
        ]
        for selector in cookie_selectors:
            if page.locator(selector).count() > 0:
                page.locator(selector).first.click(timeout=2000)
                print("✅ Accepted cookies")
                break
    except Exception:
        pass
    
    # Fill login form
    try:
        # Try multiple username selectors
        username_selectors = [
            "input[name='UserName']",
            "input[name='username']", 
            "input#username",
            "input[type='text']"
        ]
        
        password_selectors = [
            "input[name='Password']",
            "input[name='password']",
            "input#password", 
            "input[type='password']"
        ]
        
        submit_selectors = [
            "button[type='submit']",
            "input[type='submit']",
            "button:has-text('Login')",
            "button:has-text('Sign In')"
        ]
        
        # Fill username
        for selector in username_selectors:
            if page.locator(selector).count() > 0:
                page.locator(selector).first.fill(username)
                break
        
        # Fill password  
        for selector in password_selectors:
            if page.locator(selector).count() > 0:
                page.locator(selector).first.fill(password)
                break
        
        # Submit
        for selector in submit_selectors:
            if page.locator(selector).count() > 0:
                page.locator(selector).first.click()
                break
        
        print("✅ Submitted login form")
        
    except Exception as e:
        print(f"❌ Login form error: {e}")
        return False
    
    # Wait for login to complete
    try:
        page.wait_for_load_state("networkidle", timeout=30000)
        time.sleep(3)  # Additional wait
        
        # Check if we can see search elements (indicates successful login)
        if (page.locator("#Address").count() > 0 or 
            page.locator("#CityStateZip").count() > 0 or
            "search" in page.url.lower()):
            print("✅ Login successful!")
            return True
        else:
            print("❌ Login failed - search UI not found")
            return False
            
    except Exception as e:
        print(f"❌ Login timeout: {e}")
        return False

def search_property(page, address, city):
    """Search for a property and return foreclosure data"""
    try:
        # Clear the search form (don't navigate away from current page)
        # Wait for search elements to be available
        page.wait_for_selector("#Address", timeout=15000)
        page.wait_for_selector("#CityStateZip", timeout=15000)
        
        # Set search type to Property Address if available
        try:
            if page.locator("#PDVSearchType").count() > 0:
                page.select_option("#PDVSearchType", value="1")
        except Exception:
            pass
        
        # Clear and fill search form
        page.fill("#Address", "")  # Clear first
        page.fill("#Address", address)
        time.sleep(0.2)
        page.fill("#CityStateZip", "")  # Clear first
        page.fill("#CityStateZip", city)
        time.sleep(0.2)
        
        # Submit search
        search_button = page.locator("#btnsearch").first
        if search_button.count() > 0:
            search_button.click()
        else:
            page.keyboard.press("Enter")
        
        # Wait for results
        page.wait_for_load_state("networkidle", timeout=30000)
        time.sleep(2)
        
        # Extract foreclosure data
        return extract_foreclosure_data(page)
        
    except Exception as e:
        print(f"  ❌ Search error: {e}")
        return {}

def extract_foreclosure_data(page):
    """Extract foreclosure-related fields from the page"""
    fields = {
        "Loan Amount": "",
        "NOD": "",
        "Sale Date": "",
        "Back to Beneficiary On": "",
        "Bene/Client Name": ""
    }
    
    try:
        # Get page content for pattern matching
        content = page.content()
        
        # Look for foreclosure information using regex patterns
        patterns = {
            "Loan Amount": [
                r"loan\s+amount[:\s]*\$?([0-9,]+)",
                r"principal\s+balance[:\s]*\$?([0-9,]+)",
                r"amount\s+due[:\s]*\$?([0-9,]+)"
            ],
            "NOD": [
                r"notice\s+of\s+default[:\s]*([0-9/]+)",
                r"nod[:\s]*([0-9/]+)",
                r"default\s+date[:\s]*([0-9/]+)"
            ],
            "Sale Date": [
                r"sale\s+date[:\s]*([0-9/]+)",
                r"auction\s+date[:\s]*([0-9/]+)",
                r"foreclosure\s+date[:\s]*([0-9/]+)"
            ],
            "Back to Beneficiary On": [
                r"back\s+to\s+beneficiary[:\s]*([0-9/]+)",
                r"rtb[:\s]*([0-9/]+)"
            ],
            "Bene/Client Name": [
                r"beneficiary[:\s]*([^<\n]+)",
                r"client\s+name[:\s]*([^<\n]+)",
                r"lender[:\s]*([^<\n]+)"
            ]
        }
        
        # Apply patterns
        for field_name, field_patterns in patterns.items():
            for pattern in field_patterns:
                match = re.search(pattern, content, re.IGNORECASE)
                if match:
                    fields[field_name] = match.group(1).strip()
                    break
        
        # Also try to find data in tables
        try:
            tables = page.locator("table")
            for i in range(min(tables.count(), 5)):  # Check first 5 tables
                table = tables.nth(i)
                rows = table.locator("tr")
                for j in range(rows.count()):
                    row = rows.nth(j)
                    cells = row.locator("td")
                    if cells.count() >= 2:
                        label = cells.first.inner_text().strip().lower()
                        value = cells.nth(1).inner_text().strip()
                        
                        # Match foreclosure fields
                        for field_name in fields.keys():
                            if field_name.lower().replace(" ", "") in label.replace(" ", ""):
                                if not fields[field_name]:  # Only if not already found
                                    fields[field_name] = value
                                break
        except Exception:
            pass
            
    except Exception as e:
        print(f"  ⚠️  Data extraction error: {e}")
    
    return fields

def main():
    parser = argparse.ArgumentParser(description="TitlePro247 Foreclosure Data Fetcher")
    parser.add_argument("-i", "--input", required=True, help="Input Excel file")
    parser.add_argument("-o", "--output", required=True, help="Output Excel file") 
    parser.add_argument("--username", required=True, help="TitlePro username")
    parser.add_argument("--password", required=True, help="TitlePro password")
    parser.add_argument("--timeout", type=int, default=30, help="Timeout in seconds")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    parser.add_argument("--max-rows", type=int, help="Limit number of rows to process")
    
    args = parser.parse_args()
    
    # Load input data
    try:
        df = pd.read_excel(args.input)
        print(f"📊 Loaded {len(df)} rows from {args.input}")
    except Exception as e:
        print(f"❌ Error loading input file: {e}")
        return
    
    # Add foreclosure columns if they don't exist
    foreclosure_columns = ["Loan Amount", "NOD", "Sale Date", "Back to Beneficiary On", "Bene/Client Name"]
    for col in foreclosure_columns:
        if col not in df.columns:
            df[col] = ""
    
    # Limit rows if specified
    if args.max_rows:
        df = df.head(args.max_rows)
        print(f"🔢 Processing first {len(df)} rows only")
    
    # Process with Playwright
    with sync_playwright() as p:
        # Launch browser with stealth settings
        browser = p.chromium.launch(
            headless=False,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-web-security",
                "--disable-features=VizDisplayCompositor"
            ]
        )
        
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        
        page = context.new_page()
        
        # Add stealth script
        page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            window.chrome = window.chrome || { runtime: {} };
        """)
        
        # Login
        if not login_titlepro(page, args.username, args.password):
            print("❌ Login failed. Exiting.")
            browser.close()
            return
        
        print(f"🚀 Starting to process {len(df)} properties...")
        
        # Process each property
        successful = 0
        failed = 0
        
        for idx, row in tqdm(df.iterrows(), total=len(df), desc="Processing"):
            address = str(row.get("Address", "")).strip()
            city = str(row.get("City", "")).strip()
            
            if not address or not city or address.lower() == "nan" or city.lower() == "nan":
                if args.verbose:
                    print(f"⏭️  Skipping row {idx}: Missing address or city")
                continue
            
            if args.verbose:
                print(f"🔍 Processing: {address}, {city}")
            
            try:
                # Check if we're still logged in
                if page.locator("#Address").count() == 0:
                    print("🔄 Session expired, re-logging in...")
                    if not login_titlepro(page, args.username, args.password):
                        print("❌ Re-login failed. Stopping.")
                        break
                
                # Search for property
                foreclosure_data = search_property(page, address, city)
                
                # Update dataframe
                for field, value in foreclosure_data.items():
                    df.at[idx, field] = value
                
                if any(foreclosure_data.values()):
                    successful += 1
                    if args.verbose:
                        print(f"  ✅ Found data: {foreclosure_data}")
                else:
                    if args.verbose:
                        print(f"  ⚠️  No foreclosure data found")
                
                # Rate limiting
                time.sleep(1)
                
            except Exception as e:
                failed += 1
                if args.verbose:
                    print(f"  ❌ Error: {e}")
                continue
        
        browser.close()
    
    # Save results
    try:
        df.to_excel(args.output, index=False)
        print(f"💾 Results saved to {args.output}")
        print(f"📈 Summary: {successful} successful, {failed} failed")
    except Exception as e:
        print(f"❌ Error saving output: {e}")

if __name__ == "__main__":
    main()
