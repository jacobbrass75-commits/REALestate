import argparse, time, pandas as pd
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
from tqdm import tqdm

def login_titlepro(page, username, password, timeout_ms=30000, verbose=True):
    """
    Robust TitlePro247 login using the working approach from the example
    """
    BASE = "https://www.titlepro247.com"
    
    # Broad selectors for varied markup
    SEL_FORM         = "form"
    SEL_USERNAME     = "input#username, input#UserName, input[name*='user' i], input[name*='email' i]"
    SEL_PASSWORD     = "input#Password, input[type='password'], input[name*='pass' i]"
    SEL_SUBMIT       = "button[type='submit'], input[type='submit'], button:has-text('Sign In'), button:has-text('Login')"
    SEL_LOGIN_ENTRY  = "a:has-text('Login'), a:has-text('Sign In'), button:has-text('Login'), button:has-text('Sign In')"
    
    # Concrete search UI detection
    SEL_SEARCH_TYPE  = "#PDVSearchType"
    SEL_ADDR_INPUT   = "#Address"
    SEL_CITY_INPUT   = "#CityStateZip"

    def _dismiss_cookies():
        for sel in [
            "button:has-text('Accept')",
            "button:has-text('Agree')",
            "#onetrust-accept-btn-handler",
            "[id*='accept'][id*='cookie' i]",
        ]:
            try:
                if page.locator(sel).count() > 0:
                    page.locator(sel).first.click(timeout=2000)
                    if verbose: print("[login] Cookie banner dismissed")
                    break
            except Exception:
                continue

    if verbose:
        print(f"[login] Navigating to {BASE}")

    # Go to the base page
    page.goto(BASE, timeout=timeout_ms, wait_until="domcontentloaded")
    _dismiss_cookies()

    # If no form yet, click a launcher if present
    try:
        if page.locator(SEL_FORM).count() == 0:
            if page.locator(SEL_LOGIN_ENTRY).count() > 0:
                page.locator(SEL_LOGIN_ENTRY).first.click()
                if verbose: print("[login] Clicked Login/Sign In entry")
                try:
                    page.wait_for_load_state("domcontentloaded", timeout=8000)
                except PWTimeout:
                    pass
    except Exception:
        pass

    # Try main page first; if not visible, try iframes
    def _find_form_on_page():
        try:
            if page.locator(SEL_FORM).count() > 0:
                f = page.locator(SEL_FORM).first
                if f.is_visible():
                    return f
        except Exception:
            pass
        # iframes fallback
        for fr in page.frames:
            try:
                if fr.locator(SEL_FORM).count() > 0:
                    return fr.locator(SEL_FORM).first
            except Exception:
                continue
        return None

    form = _find_form_on_page()
    if not form:
        try:
            page.wait_for_selector(SEL_FORM, timeout=timeout_ms)
            form = _find_form_on_page()
        except PWTimeout:
            form = _find_form_on_page()

    if not form:
        print("[login] Could not find a login form.")
        return False

    # Fill username/password
    try:
        form.locator(SEL_USERNAME).first.fill(username, timeout=5000)
        form.locator(SEL_PASSWORD).first.fill(password, timeout=5000)
        if verbose: print(f"[login] Filled credentials for '{username}'")
    except Exception as e:
        print(f"[login] Failed to fill credentials: {e}")
        return False

    # Submit
    try:
        if form.locator(SEL_SUBMIT).count() > 0:
            form.locator(SEL_SUBMIT).first.click(timeout=5000)
        else:
            form.press(SEL_PASSWORD, "Enter")
        if verbose: print("[login] Submitted login form")
    except Exception:
        try:
            form.press(SEL_PASSWORD, "Enter")
        except Exception:
            pass

    # Wait to settle
    try:
        page.wait_for_load_state("networkidle", timeout=timeout_ms)
    except PWTimeout:
        pass

    # After login, try to reach search surface
    for _ in range(2):
        try:
            # If the concrete search elements are already there, success
            if (page.locator(SEL_SEARCH_TYPE).count() > 0 and
                page.locator(SEL_ADDR_INPUT).count() > 0 and
                page.locator(SEL_CITY_INPUT).count() > 0):
                if verbose: print("[login] Search UI present — login OK")
                return True
        except Exception:
            pass

        # Try obvious top-nav entries to reach search/home
        for nav_sel in [
            "a:has-text('Property Search')",
            "a:has-text('Home')",
            "a[href*='Index']",
            "a[href='/']",
        ]:
            try:
                if page.locator(nav_sel).count() > 0:
                    page.locator(nav_sel).first.click()
                    try:
                        page.wait_for_load_state("networkidle", timeout=8000)
                    except PWTimeout:
                        pass
                    break
            except Exception:
                continue

        # As a simple fallback, reload BASE
        try:
            page.goto(BASE, timeout=timeout_ms, wait_until="domcontentloaded")
        except Exception:
            pass
        _dismiss_cookies()

    # Final check
    try:
        page.wait_for_selector(SEL_ADDR_INPUT, timeout=4000)
        page.wait_for_selector(SEL_CITY_INPUT, timeout=4000)
        if page.locator(SEL_SEARCH_TYPE).count() > 0:
            if verbose: print("[login] Search UI present — login OK")
            return True
    except Exception:
        pass

    print("[login] Login flow completed, but search UI not detected.")
        return False

def scrape_foreclosure_fields(page):
    """
    Scrape foreclosure-related fields from the property details page.
    Look for common foreclosure field patterns in tables or text.
    """
    fields = {
        "Loan Amount": None,
        "NOD": None,
        "Sale Date": None,
        "Back to Beneficiary On": None,
        "Bene/Client Name": None
    }
    
    # Try to find foreclosure information in various ways
    page_content = page.content()
    
    # Look for foreclosure-related text patterns
    foreclosure_patterns = {
        "Loan Amount": [r"loan\s+amount[:\s]*\$?([0-9,]+)", r"principal\s+balance[:\s]*\$?([0-9,]+)"],
        "NOD": [r"notice\s+of\s+default[:\s]*([0-9/]+)", r"nod[:\s]*([0-9/]+)"],
        "Sale Date": [r"sale\s+date[:\s]*([0-9/]+)", r"auction\s+date[:\s]*([0-9/]+)"],
        "Back to Beneficiary On": [r"back\s+to\s+beneficiary[:\s]*([0-9/]+)"],
        "Bene/Client Name": [r"beneficiary[:\s]*([^<\n]+)", r"client\s+name[:\s]*([^<\n]+)"]
    }
    
    for field_name, patterns in foreclosure_patterns.items():
        for pattern in patterns:
            import re
            match = re.search(pattern, page_content, re.IGNORECASE)
            if match:
                fields[field_name] = match.group(1).strip()
                break
    
    # Also try table-based extraction as fallback
    if not any(fields.values()):
        try:
            # Look for tables that might contain foreclosure info
            tables = page.locator("table")
            for i in range(tables.count()):
                table = tables.nth(i)
                rows = table.locator("tr")
                for j in range(rows.count()):
                    row = rows.nth(j)
                    cells = row.locator("td")
                    if cells.count() >= 2:
                        label_cell = cells.first.inner_text().strip().lower()
                        value_cell = cells.nth(1).inner_text().strip()
                        
                        # Match known foreclosure field names
                        for field_name in fields.keys():
                            if field_name.lower() in label_cell:
                                fields[field_name] = value_cell
                                break
        except Exception:
            pass
    
    # Clean up empty values
    for key in fields:
        if not fields[key]:
            fields[key] = ""
    
    return fields

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--input", required=True)
    parser.add_argument("-o", "--output", required=True)
    parser.add_argument("--username", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--cache", action="store_true")
    parser.add_argument("--save-debug", action="store_true")
    parser.add_argument("-v", action="store_true")
    args = parser.parse_args()

    try:
        df = pd.read_excel(args.input)
        print(f"✅ Loaded {len(df)} rows from input file")
    except Exception as e:
        print(f"[ERROR] Failed to load input file: {e}")
        return

    # Helper to create a hardened browser/page each time we need one
    def _make_page(pwt):
        ua = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        browser = pwt.chromium.launch(
            headless=False,
            args=["--disable-blink-features=AutomationControlled"]
        )
        ctx = browser.new_context(user_agent=ua)

        # Block any hard logout calls the site might try to fire
        def _route_logout(route, request):
            url = request.url.lower()
            if "logout" in url:
                return route.abort()
            return route.continue_()
        try:
            ctx.route("**/*", _route_logout)
        except Exception:
            pass

        page = ctx.new_page()
        # Small evasion script
        try:
            page.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {get: () => false});
                window.chrome = window.chrome || { runtime: {} };
            """)
        except Exception:
            pass
        return browser, ctx, page

    # Session health check
    def _session_alive(pg) -> bool:
        try:
            return (pg.locator("#Address").count() > 0 and pg.locator("#CityStateZip").count() > 0)
        except Exception:
            return False

    with sync_playwright() as p:
        browser, ctx, page = _make_page(p)
        
        # Set a longer default timeout
        page.set_default_timeout(60000)
        
        if not login_titlepro(page, args.username, args.password, timeout_ms=args.timeout * 1000, verbose=args.v):
            print("Login failed, closing browser...")
            browser.close()
            return

        print(f"Starting to process {len(df)} properties...")
        
        for idx, row in tqdm(df.iterrows(), total=len(df), desc="Processing properties"):
            if not row.get("Address") or not row.get("City"):
                print(f"Skipping row {idx}: Missing Address or City")
                continue
                
            address, city = row["Address"], row["City"]
            print(f"Processing: {address}, {city}")
            
            # Re-auth if needed
            if not _session_alive(page):
                print("[session] Search UI missing — attempting re-login.")
                if not login_titlepro(page, args.username, args.password, timeout_ms=args.timeout * 1000, verbose=args.v):
                    print("[session] Re-login failed; stopping.")
                    break
            
            try:
                # Use the proper search flow
                page.goto("https://www.titlepro247.com", timeout=30000)
                page.wait_for_selector("#Address", timeout=30000)
                page.wait_for_selector("#CityStateZip", timeout=30000)
                
                # Set search type to Property Address
                try:
                    if page.locator("#PDVSearchType").count() > 0:
                        page.select_option("#PDVSearchType", value="1")
                except Exception:
                    pass
                
                page.fill("#Address", address)
                time.sleep(0.15)
                page.fill("#CityStateZip", city)
                time.sleep(0.15)
                
                page.click("#btnsearch")
                page.wait_for_load_state("networkidle", timeout=30000)
                page.wait_for_timeout(3000)
                
                data = scrape_foreclosure_fields(page)
                for k, v in data.items():
                    df.at[idx, k] = v
                    
                print(f"  Found data: {data}")
                
            except Exception as e:
                print(f"  Error processing {address}, {city}: {e}")
                continue
                
        browser.close()

    df.to_excel(args.output, index=False)
    print(f"✅ Done. Output saved to: {args.output}")

if __name__ == "__main__":
    main()