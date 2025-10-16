#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
TitlePro247 Enhanced Foreclosure Data Fetcher with PDF Support
-------------------------------------------------------------
- Reads Excel with Address, City columns
- Logs into TitlePro247 and searches for foreclosure data
- Downloads and parses PDFs for additional foreclosure information
- Supports text extraction, OCR, and optional AI parsing
- Saves results with Loan Amount, NOD, Sale Date, etc.
"""

import argparse
import time
import pandas as pd
import re
import os
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple, List
from io import BytesIO
import base64

from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
from tqdm import tqdm

# PDF + OCR + AI helpers
try:
    from pdfminer.high_level import extract_text as pdf_extract_text
    PDFMINER_AVAILABLE = True
except ImportError:
    PDFMINER_AVAILABLE = False

try:
    from pdf2image import convert_from_path
    import pytesseract
    PDF2IMAGE_AVAILABLE = True
except ImportError:
    PDF2IMAGE_AVAILABLE = False

try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False

def normalize_date(date_str: str) -> Optional[str]:
    """Normalize various date formats to YYYY-MM-DD"""
    if not date_str:
        return None
    
    date_str = date_str.strip()
    
    # Common date patterns
    patterns = [
        (r'(\d{1,2})/(\d{1,2})/(\d{2,4})', lambda m: f"{m.group(3).zfill(4)}-{m.group(1).zfill(2)}-{m.group(2).zfill(2)}"),
        (r'(\w{3,})\s+(\d{1,2}),\s+(\d{4})', lambda m: f"{m.group(3)}-{month_to_num(m.group(1))}-{m.group(2).zfill(2)}"),
        (r'(\d{4})-(\d{1,2})-(\d{1,2})', lambda m: f"{m.group(1)}-{m.group(2).zfill(2)}-{m.group(3).zfill(2)}"),
    ]
    
    for pattern, formatter in patterns:
        match = re.search(pattern, date_str, re.IGNORECASE)
        if match:
            try:
                return formatter(match)
            except:
                continue
    
    return date_str

def month_to_num(month: str) -> str:
    """Convert month name to number"""
    months = {
        'jan': '01', 'january': '01',
        'feb': '02', 'february': '02', 
        'mar': '03', 'march': '03',
        'apr': '04', 'april': '04',
        'may': '05',
        'jun': '06', 'june': '06',
        'jul': '07', 'july': '07',
        'aug': '08', 'august': '08',
        'sep': '09', 'september': '09',
        'oct': '10', 'october': '10',
        'nov': '11', 'november': '11',
        'dec': '12', 'december': '12'
    }
    return months.get(month.lower(), '01')

def normalize_currency(amount_str: str) -> Optional[str]:
    """Normalize currency to $X,XXX.XX format"""
    if not amount_str:
        return None
    
    # Extract numbers and clean up
    numbers = re.findall(r'[\d,]+\.?\d*', amount_str)
    if not numbers:
        return None
    
    try:
        # Take the largest number found
        amount = max([float(num.replace(',', '')) for num in numbers])
        return f"${amount:,.2f}"
    except:
        return amount_str

def parse_text_for_fields(text: str) -> Dict[str, Optional[str]]:
    """
    Heuristic regex parsing from PDF text.
    Try to capture: Loan Amount, NOD, Sale Date, Back to Beneficiary On, Bene/Client Name
    """
    out = {"Loan Amount": None, "NOD": None, "Sale Date": None,
           "Back to Beneficiary On": None, "Bene/Client Name": None}

    def _find(patterns, flags=re.I):
        for p in patterns:
            m = re.search(p, text, flags)
            if m:
                return m.group(1).strip()
        return None

    # Loan Amount
    out["Loan Amount"] = _find([
        r"Loan Amount[:\s]+\$?([\d,\.]+)",
        r"original principal(?: amount)?[:\s]+\$?([\d,\.]+)",
        r"principal balance[:\s]+\$?([\d,\.]+)"
    ])

    # NOD (recorded/dated)
    out["NOD"] = normalize_date(_find([
        r"Notice of Default.*?(?:recorded|dated)[:\s]+([A-Za-z]{3,}\s+\d{1,2},\s+\d{4}|\d{1,2}[/-]\d{1,2}[/-]\d{2,4})",
        r"NOD[:\s]+([A-Za-z]{3,}\s+\d{1,2},\s+\d{4}|\d{1,2}[/-]\d{1,2}[/-]\d{2,4})",
        r"default date[:\s]+([A-Za-z]{3,}\s+\d{1,2},\s+\d{4}|\d{1,2}[/-]\d{1,2}[/-]\d{2,4})"
    ]) or "")

    # Sale Date (Trustee's Sale)
    out["Sale Date"] = normalize_date(_find([
        r"(?:Trustee'?s? Sale Date|Sale Date)[:\s]+([A-Za-z]{3,}\s+\d{1,2},\s+\d{4}|\d{1,2}[/-]\d{1,2}[/-]\d{2,4})",
        r"will sell.*?on\s+([A-Za-z]{3,}\s+\d{1,2},\s+\d{4})",
        r"auction date[:\s]+([A-Za-z]{3,}\s+\d{1,2},\s+\d{4}|\d{1,2}[/-]\d{1,2}[/-]\d{2,4})"
    ]) or "")

    # Back to Beneficiary On
    out["Back to Beneficiary On"] = normalize_date(_find([
        r"Back to Beneficiary(?: On)?[:\s]+([A-Za-z]{3,}\s+\d{1,2},\s+\d{4}|\d{1,2}[/-]\d{1,2}[/-]\d{2,4})",
        r"RTB[:\s]+([A-Za-z]{3,}\s+\d{1,2},\s+\d{4}|\d{1,2}[/-]\d{1,2}[/-]\d{2,4})"
    ]) or "")

    # Beneficiary/Bene/Client
    bene = _find([
        r"(?:Beneficiary|Bene/Client Name|Lender|Mortgagee)[:\s]+(.+)",
        r"client name[:\s]+(.+)"
    ])
    if bene:
        out["Bene/Client Name"] = bene.splitlines()[0].strip()

    # Normalize currency formats if found
    if out["Loan Amount"]:
        out["Loan Amount"] = normalize_currency(out["Loan Amount"])

    return out

def pdf_to_text(path: str) -> str:
    """Try text extraction; if empty, OCR all pages."""
    if not PDFMINER_AVAILABLE:
        print("  ⚠️  pdfminer.six not available")
        return ""
    
    try:
        txt = pdf_extract_text(path) or ""
    except Exception as e:
        print(f"  ⚠️  PDF text extraction failed: {e}")
        txt = ""
    
    if txt.strip():
        return txt

    # OCR fallback (scanned PDFs)
    if not PDF2IMAGE_AVAILABLE:
        print("  ⚠️  pdf2image/pytesseract not available for OCR")
        return ""
    
    try:
        pages = convert_from_path(path, dpi=300)
        ocr_texts = []
        for img in pages:
            ocr_texts.append(pytesseract.image_to_string(img))
        return "\n".join(ocr_texts)
    except Exception as e:
        print(f"  ⚠️  OCR failed: {e}")
        return ""

def extract_fields_from_pdf(path: str) -> Dict[str, Optional[str]]:
    """Extract foreclosure fields from PDF using text parsing"""
    text = pdf_to_text(path)
    if not text.strip():
        return {}
    return parse_text_for_fields(text)

def extract_fields_with_openai_from_pdf(path: str, model: str = "gpt-4o-mini") -> Dict[str, Optional[str]]:
    """
    OPTIONAL: Use OpenAI Vision on first 2 pages rendered to PNG, ask for JSON.
    Requires OPENAI_API_KEY env var to be set.
    """
    if not OPENAI_AVAILABLE:
        print("  ⚠️  OpenAI not available")
        return {}
    
    if not PDF2IMAGE_AVAILABLE:
        print("  ⚠️  pdf2image not available for AI processing")
        return {}

    try:
        client = OpenAI()
    except Exception as e:
        print(f"  ⚠️  OpenAI client failed: {e}")
        return {}

    # Render first 2 pages -> base64 PNG
    try:
        imgs = convert_from_path(path, dpi=220, first_page=1, last_page=2)
    except Exception as e:
        print(f"  ⚠️  pdf2image failed: {e}")
        return {}

    images_payload = []
    for img in imgs:
        buf = BytesIO()
        img.save(buf, format="PNG")
        b64 = base64.b64encode(buf.getvalue()).decode()
        images_payload.append({"type": "input_image", "image_url": f"data:image/png;base64,{b64}"})

    prompt = (
        "Extract these fields from the foreclosure/legal notice if present. "
        "Return pure JSON with keys: "
        '["Loan Amount","NOD","Sale Date","Back to Beneficiary On","Bene/Client Name"]. '
        "For dates, use YYYY-MM-DD if you can parse them; else empty string. "
        "For Loan Amount, format like $123,456.78 if available."
    )

    try:
        msg = [
            {"role": "user", "content": [{"type": "input_text", "text": prompt}, *images_payload]}
        ]
        resp = client.chat.completions.create(
            model=model,
            messages=msg,
            temperature=0
        )
        raw = resp.choices[0].message.content
        # Sometimes the model wraps in code fences; strip and parse
        raw = raw.strip().strip("`").replace("json\n", "").replace("JSON\n", "")
        data = json.loads(raw)
        return {
            "Loan Amount": data.get("Loan Amount") or None,
            "NOD": normalize_date(data.get("NOD") or "") or data.get("NOD") or None,
            "Sale Date": normalize_date(data.get("Sale Date") or "") or data.get("Sale Date") or None,
            "Back to Beneficiary On": normalize_date(data.get("Back to Beneficiary On") or "") or data.get("Back to Beneficiary On") or None,
            "Bene/Client Name": (data.get("Bene/Client Name") or "").strip() or None
        }
    except Exception as e:
        print(f"  ⚠️  OpenAI extraction failed: {e}")
        return {}

def download_first_pdf(page, tag: str, timeout_ms: int = 20000) -> Optional[str]:
    """
    Try to click any visible link/button that yields a PDF download.
    Saves to ./tp_debug/pdfs/<tag>_<ts>.pdf and returns its path.
    """
    os.makedirs("tp_debug/pdfs", exist_ok=True)
    
    # More comprehensive PDF link detection
    candidates = [
        # Direct PDF links
        "a[href$='.pdf']", "a[href*='.pdf']",
        # Common PDF link text
        "a:has-text('PDF')", "button:has-text('PDF')",
        "a:has-text('Notice of Default')", "a:has-text('NOD')",
        "a:has-text('Trustee Sale')", "a:has-text('Trustee')",
        "a:has-text('Recorded Document')", "a:has-text('Document')",
        "a:has-text('View')", "button:has-text('View')",
        "a:has-text('Download')", "button:has-text('Download')",
        "a:has-text('Get')", "button:has-text('Get')",
        "a:has-text('Open')", "button:has-text('Open')",
        # Generic links that might be PDFs
        "a[href*='document']", "a[href*='notice']",
        "a[href*='trustee']", "a[href*='default']",
        # Look for any clickable elements with foreclosure-related text
        "a:has-text('Foreclosure')", "a:has-text('Lien')",
        "a:has-text('Mortgage')", "a:has-text('Deed')"
    ]

    print(f"  🔍 Looking for PDF links...")
    
    # First, let's see what links are actually on the page
    try:
        all_links = page.locator("a").all()
        print(f"  📋 Found {len(all_links)} links on page")
        
        # Check each link for potential PDF indicators
        for i, link in enumerate(all_links[:10]):  # Check first 10 links
            try:
                href = link.get_attribute("href") or ""
                text = link.inner_text().strip()
                if any(keyword in (href + text).lower() for keyword in ['pdf', 'document', 'notice', 'trustee', 'default', 'view', 'get', 'download']):
                    print(f"  🔗 Link {i+1}: '{text}' -> '{href}'")
            except Exception:
                continue
    except Exception as e:
        print(f"  ⚠️  Could not scan links: {e}")

    # Try clicking each candidate
    for i, sel in enumerate(candidates):
        try:
            elements = page.locator(sel)
            count = elements.count()
            if count == 0:
                continue
            
            print(f"  🎯 Trying selector {i+1}/{len(candidates)}: {sel} (found {count} elements)")
            
            for j in range(min(count, 3)):  # Try first 3 matching elements
                try:
                    element = elements.nth(j)
                    text = element.inner_text().strip()
                    href = element.get_attribute("href") or ""
                    print(f"    Clicking element {j+1}: '{text}' -> '{href}'")
                    
                    with page.expect_download(timeout=timeout_ms) as dl_info:
                        element.click()
                    
                    dl = dl_info.value
                    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                    out = f"tp_debug/pdfs/{tag}_{ts}.pdf"
                    dl.save_as(out)
                    print(f"  📄 Successfully downloaded PDF: {out}")
                    return out
                    
                except Exception as e:
                    print(f"    Failed to download from element {j+1}: {e}")
                    continue
                    
        except Exception as e:
            print(f"  ⚠️  Error with selector {sel}: {e}")
            continue
    
    print(f"  ❌ No PDF downloads found")
    return None

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
        
        # Handle cookie consent first
        try:
            reject_all = page.locator("button:has-text('Reject All'), button#onetrust-reject-all-handler").first
            if reject_all.count() > 0:
                print("🍪 Handling cookie consent...")
                reject_all.click()
                time.sleep(1)
        except Exception:
            pass  # No cookie dialog, continue
        
        # Submit
        try:
            for selector in submit_selectors:
                if page.locator(selector).count() > 0:
                    page.locator(selector).first.click()
                    break
            print("✅ Submitted login form")
        except Exception as submit_error:
            print(f"⚠️  Normal submit failed: {submit_error}")
            # Try JavaScript click
            try:
                page.evaluate("document.getElementById('login-submit')?.click()")
                print("✅ Submitted login form via JavaScript")
            except Exception as js_error:
                print(f"❌ Login form error: {js_error}")
                return False
        
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
        
        # Extract foreclosure data from page
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
    parser = argparse.ArgumentParser(description="TitlePro247 Enhanced Foreclosure Data Fetcher")
    parser.add_argument("-i", "--input", required=True, help="Input Excel file")
    parser.add_argument("-o", "--output", required=True, help="Output Excel file") 
    parser.add_argument("--username", required=True, help="TitlePro username")
    parser.add_argument("--password", required=True, help="TitlePro password")
    parser.add_argument("--timeout", type=int, default=30, help="Timeout in seconds")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    parser.add_argument("--max-rows", type=int, help="Limit number of rows to process")
    parser.add_argument("--use-ai", action="store_true", help="Use OpenAI Vision fallback if PDF is scanned/messy (requires OPENAI_API_KEY)")
    parser.add_argument("--openai-model", default="gpt-4o-mini", help="OpenAI model for PDF vision")
    parser.add_argument("--save-debug", action="store_true", help="Save debug files and PDFs")
    
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
                
                # Debug: Save page HTML to see what's available
                if args.save_debug:
                    try:
                        os.makedirs("tp_debug", exist_ok=True)
                        tag = re.sub(r"[^A-Za-z0-9_-]+", "_", address)[:50]
                        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                        html_path = f"tp_debug/{tag}_{ts}_after_search.html"
                        with open(html_path, 'w', encoding='utf-8') as f:
                            f.write(page.content())
                        print(f"  💾 Saved page HTML: {html_path}")
                    except Exception as e:
                        print(f"  ⚠️  Could not save HTML: {e}")
                
                # Always try PDF route since HTML extraction is limited
                print(f"  🔍 Attempting to get PDF data...")
                
                # Step 1: Ensure property is selected and click "Get Now" button
                try:
                    print(f"  🎯 Looking for 'Get Now' button...")
                    get_now_button = page.locator("button:has-text('Get Now')").first
                    
                    if get_now_button.count() > 0:
                        # Check if button is disabled
                        is_disabled = get_now_button.is_disabled()
                        print(f"  📋 Get Now button disabled: {is_disabled}")
                        
                        if is_disabled:
                            # Try to select the property first
                            print(f"  🎯 Button disabled, trying to select property...")
                            
                            # Look for property selection radio button or row
                            # Use the actual address from our data
                            address_parts = address.split()
                            city_parts = city.split()
                            property_row = page.locator(f"tr:has-text('{address_parts[0]}'), tr:has-text('{city_parts[0]}')").first
                            if property_row.count() > 0:
                                print(f"  ✅ Found property row, selecting...")
                                property_row.click()
                                time.sleep(1)
                                
                                # Check if button is now enabled
                                is_disabled = get_now_button.is_disabled()
                                print(f"  📋 Get Now button disabled after selection: {is_disabled}")
                            
                            # Also try clicking the radio button directly
                            radio_button = page.locator("input[type='radio']:checked").first
                            if radio_button.count() > 0:
                                print(f"  ✅ Found selected radio button")
                                time.sleep(1)
                        
                        # Try clicking the button (even if disabled, force click)
                        if not get_now_button.is_disabled() or True:  # Always try
                            print(f"  ✅ Attempting to click 'Get Now' button...")
                            
                            # First try to scroll the button into view
                            try:
                                print(f"  📜 Scrolling button into view...")
                                get_now_button.scroll_into_view_if_needed(timeout=5000)
                                time.sleep(1)
                            except Exception as scroll_error:
                                print(f"  ⚠️  Scroll failed: {scroll_error}")
                            
                            # Try normal click first
                            try:
                                print(f"  🎯 Trying normal click...")
                                get_now_button.click(timeout=5000)
                                time.sleep(2)  # Wait for cart update
                                print(f"  ✅ Normal click successful!")
                            except Exception as click_error:
                                print(f"  ⚠️  Normal click failed: {click_error}")
                                
                                # Try force click
                                try:
                                    print(f"  🔧 Trying force click...")
                                    get_now_button.click(force=True, timeout=5000)
                                    time.sleep(2)
                                    print(f"  ✅ Force click successful!")
                                except Exception as force_error:
                                    print(f"  ❌ Force click also failed: {force_error}")
                                    
                                    # Final fallback: JavaScript click using ID
                                    try:
                                        print(f"  💻 Trying JavaScript click by ID...")
                                        page.evaluate("document.getElementById('buyNowMap')?.click()")
                                        time.sleep(2)
                                        print(f"  ✅ JavaScript click successful!")
                                    except Exception as js_error:
                                        print(f"  ❌ JavaScript click failed: {js_error}")
                                        
                                        # Try alternative JavaScript approaches
                                        try:
                                            print(f"  💻 Trying alternative JavaScript click...")
                                            page.evaluate("""
                                                const buttons = Array.from(document.querySelectorAll('button'));
                                                const getNowButton = buttons.find(btn => btn.textContent.includes('Get Now'));
                                                if (getNowButton) getNowButton.click();
                                            """)
                                            time.sleep(2)
                                            print(f"  ✅ Alternative JavaScript click successful!")
                                        except Exception as alt_js_error:
                                            print(f"  ❌ Alternative JavaScript click failed: {alt_js_error}")
                                            raise alt_js_error
                        
                        # Step 2: Look for "Continue" button (for comparables selection)
                        continue_button = page.locator("button:has-text('Continue')").first
                        if continue_button.count() > 0:
                            print(f"  ✅ Found 'Continue' button, clicking...")
                            try:
                                continue_button.click(timeout=5000)
                                time.sleep(3)  # Wait for processing
                            except Exception as continue_error:
                                print(f"  ⚠️  Continue click failed: {continue_error}")
                                print(f"  💻 Trying JavaScript click for Continue...")
                                try:
                                    page.evaluate("""
                                        const buttons = Array.from(document.querySelectorAll('button'));
                                        const continueButton = buttons.find(btn => btn.textContent.includes('Continue'));
                                        if (continueButton) continueButton.click();
                                    """)
                                    time.sleep(3)
                                    print(f"  ✅ JavaScript Continue click successful!")
                                except Exception as js_continue_error:
                                    print(f"  ❌ JavaScript Continue click failed: {js_continue_error}")
                                    # Continue anyway, might not be needed
                        
                        # Step 3: Go to cart to view the generated report
                        print(f"  🛒 Going to cart to view generated report...")
                        page.goto("https://www.titlepro247.com/Cart")
                        time.sleep(5)  # Wait longer for order to be processed
                        
                        # Debug: Check what's in the cart
                        if args.save_debug:
                            try:
                                tag = re.sub(r"[^A-Za-z0-9_-]+", "_", address)[:50]
                                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                                cart_html_path = f"tp_debug/{tag}_{ts}_cart.html"
                                with open(cart_html_path, 'w', encoding='utf-8') as f:
                                    f.write(page.content())
                                print(f"  💾 Saved cart HTML: {cart_html_path}")
                            except Exception as e:
                                print(f"  ⚠️  Could not save cart HTML: {e}")
                        
                        # Step 4: Look for "View Order" link or similar
                        print(f"  🔍 Looking for order links in cart...")
                        
                        # Try different selectors for order links
                        order_selectors = [
                            "a[href*='/Orders/']",
                            "a:has-text('View')",
                            "a:has-text('Order')",
                            "a:has-text('Report')",
                            "a[href*='Order']",
                            "a[href*='Report']"
                        ]
                        
                        order_links = []
                        for selector in order_selectors:
                            links = page.locator(selector).all()
                            if links:
                                order_links.extend(links)
                                print(f"  ✅ Found {len(links)} links with selector: {selector}")
                        
                        if order_links:
                            print(f"  📄 Found {len(order_links)} total order links, clicking first one...")
                            order_links[0].click()
                            time.sleep(5)  # Wait longer for order page to load
                            
                            # Debug: Save order page HTML
                            if args.save_debug:
                                try:
                                    tag = re.sub(r"[^A-Za-z0-9_-]+", "_", address)[:50]
                                    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                                    order_html_path = f"tp_debug/{tag}_{ts}_order.html"
                                    with open(order_html_path, 'w', encoding='utf-8') as f:
                                        f.write(page.content())
                                    print(f"  💾 Saved order page HTML: {order_html_path}")
                                except Exception as e:
                                    print(f"  ⚠️  Could not save order HTML: {e}")
                            
                            # Step 5: Look for "View PDF" link
                            pdf_link = page.locator("a:has-text('View PDF'), a:has-text('PDF')").first
                            if pdf_link.count() > 0:
                                print(f"  📄 Found PDF link, clicking...")
                                
                                # Handle PDF download
                                try:
                                    with page.expect_download(timeout=30000) as download_info:
                                        pdf_link.click()
                                    
                                    download = download_info.value
                                    tag = re.sub(r"[^A-Za-z0-9_-]+", "_", address)[:50]
                                    os.makedirs("tp_debug/pdfs", exist_ok=True)
                                    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                                    pdf_path = f"tp_debug/pdfs/{tag}_{ts}.pdf"
                                    download.save_as(pdf_path)
                                    print(f"  💾 Downloaded PDF: {pdf_path}")
                                    
                                except Exception as e:
                                    print(f"  ⚠️  Download failed, trying direct URL access: {e}")
                                    # Fallback: try to get PDF content directly from URL
                                    pdf_url = pdf_link.get_attribute("href")
                                    if pdf_url:
                                        try:
                                            response = page.request.get(pdf_url)
                                            tag = re.sub(r"[^A-Za-z0-9_-]+", "_", address)[:50]
                                            os.makedirs("tp_debug/pdfs", exist_ok=True)
                                            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                                            pdf_path = f"tp_debug/pdfs/{tag}_{ts}_direct.pdf"
                                            with open(pdf_path, 'wb') as f:
                                                f.write(response.body())
                                            print(f"  💾 Saved PDF directly: {pdf_path}")
                                        except Exception as e2:
                                            print(f"  ⚠️  Direct PDF access failed: {e2}")
                                            pdf_path = None
                                    else:
                                        pdf_path = None
                            else:
                                print(f"  ⚠️  No PDF link found")
                                pdf_path = None
                        else:
                            print(f"  ⚠️  No order links found")
                            pdf_path = None
                    else:
                        print(f"  ⚠️  No 'Get Now' button found")
                        pdf_path = None
                        
                except Exception as e:
                    print(f"  ⚠️  Error in PDF workflow: {e}")
                    pdf_path = None
                
                # Step 2: Extract data from PDF if we got one
                if pdf_path and os.path.exists(pdf_path):
                    print(f"  🔍 Parsing PDF for foreclosure data...")
                    
                    # Try text extraction first
                    parsed = extract_fields_from_pdf(pdf_path)
                    
                    # If no data found or we want AI enhancement, try AI
                    if (not parsed or not any(parsed.values())) and args.use_ai:
                        print(f"  🤖 Text extraction failed, trying AI parsing...")
                        ai_parsed = extract_fields_with_openai_from_pdf(pdf_path, model=args.openai_model)
                        if ai_parsed:
                            parsed = ai_parsed
                    
                    # Merge PDF data into foreclosure_data
                    if parsed:
                        print(f"  ✅ PDF extraction found: {parsed}")
                        for k in foreclosure_columns:
                            if parsed.get(k) and not (foreclosure_data.get(k) or "").strip():
                                foreclosure_data[k] = parsed[k]
                    else:
                        print(f"  ⚠️  No data extracted from PDF")
                else:
                    print(f"  ⚠️  No PDF available for extraction")
                
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
