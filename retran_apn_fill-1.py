#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Retran APN Filler - Enhanced Version 2.0
----------------------------------------
Reads an Excel/CSV with columns: Address, APN
For rows where APN is blank, posts the Address to Retran's search form
and parses the APN from results. APN is found in the third column of each
search result row, as the first quoted text before any <br> tag.

NEW FEATURES:
- Progress bar and ETA display
- Automatic resume capability
- Batch processing with smart rate limiting
- Enhanced error recovery
- Results caching to avoid duplicate lookups
- Export of failed lookups for manual review
- Better column detection and mapping
- Parallel processing option (experimental)

Usage examples:
  python retran_apn_fill.py --input "/Users/mattwaeldner/Desktop/jacobsProperties.xlsx" \
                            --output "/Users/mattwaeldner/Desktop/jacobsProperties_with_apn.xlsx" \
                            --username "your_username" --password "your_password"

  python retran_apn_fill.py --input "/path/to/addresses.csv" --output "/path/to/output.csv" \
                            --resume --cache --parallel 2

Options:
  --input, -i         Path to input XLSX/CSV (must have columns: Address, APN)
  --output, -o        Path to output XLSX/CSV (extension determines format)
  --username          Login username (if authentication required)
  --password          Login password (if authentication required, omit to be prompted)
  --sheet             Excel sheet name (default: first sheet)
  --rate              Seconds to sleep between requests (default: 1.0)
  --max-retries       Max retries per address (default: 3)
  --timeout           Request timeout seconds (default: 30)
  --verbose, -v       Print detailed logs
  --debug-first       Only process first N rows for debugging
  --resume            Resume from previous run (requires --cache)
  --cache             Enable result caching to avoid duplicate lookups
  --cache-file        Custom cache file path (default: .retran_cache.json)
  --failed-output     Export failed lookups to separate file
  --parallel          Number of parallel workers (experimental, default: 1)
  --progress          Show progress bar (default: True)
  --backup            Create backup of input file before processing
"""
import argparse
import getpass
import json
import logging
import os
import re
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False
    print("Note: Install 'tqdm' for progress bars: pip install tqdm")

RETRAN_BASE_URL = "https://retran.net"
RETRAN_SEARCH_URL = "https://retran.net/reports/list.asp"
RETRAN_LOGIN_URL = "https://retran.net/login4scvb2.asp"

# Cache for storing results
_result_cache = {}
_cache_lock = threading.Lock()

# Prefer dashed APNs like 763-160-012
APN_DASHED_RE = re.compile(r'\b\d{2,4}-\d{3,5}-\d{2,3}\b')

# Conservative fallback: must contain at least one digit; 6–20 chars of [A-Z0-9-]
# (prevents picking plain words like "Account")
APN_ALNUM_RE  = re.compile(r'\b(?=[A-Z0-9-]{6,20}\b)(?=.*\d)[A-Z0-9-]+\b', re.I)

class APNSearcher:
    """Enhanced APN searcher with caching and better session management"""
    
    def __init__(self, cache_enabled=False, cache_file=".retran_cache.json"):
        self.session = requests.Session()
        self.cache_enabled = cache_enabled
        self.cache_file = cache_file
        self.cache = {}
        self.stats = {
            'found': 0, 'not_found': 0, 'errors': 0, 
            'cache_hits': 0, 'requests_made': 0
        }
        self.load_cache()
        
    def load_cache(self):
        """Load cache from file if it exists"""
        if self.cache_enabled and os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, 'r') as f:
                    self.cache = json.load(f)
                logging.info(f"Loaded {len(self.cache)} cached results from {self.cache_file}")
            except Exception as e:
                logging.warning(f"Failed to load cache: {e}")
                
    def save_cache(self):
        """Save cache to file"""
        if self.cache_enabled:
            try:
                with open(self.cache_file, 'w') as f:
                    json.dump(self.cache, f, indent=2)
                logging.debug(f"Cache saved with {len(self.cache)} entries")
            except Exception as e:
                logging.warning(f"Failed to save cache: {e}")
                
    def get_cached_result(self, address: str) -> Optional[str]:
        """Get cached result for address"""
        if not self.cache_enabled:
            return None
        normalized_addr = self.normalize_address(address)
        return self.cache.get(normalized_addr)
        
    def cache_result(self, address: str, apn: Optional[str]):
        """Cache the result for an address"""
        if self.cache_enabled:
            normalized_addr = self.normalize_address(address)
            self.cache[normalized_addr] = apn
            
    @staticmethod
    def normalize_address(address: str) -> str:
        """Normalize address for consistent caching"""
        return re.sub(r'\s+', ' ', address.strip().upper())


def dump_results_tables(
    html: str,
    address: str,
    out_dir: str = "debug_html",
    max_tables: int = 3
) -> Optional[str]:
    """
    Extract the most relevant results table(s) from the given HTML and write a
    minimal standalone HTML file containing only those tables.

    Returns the path to the saved HTML (or None on failure).
    """
    try:
        os.makedirs(out_dir, exist_ok=True)
        soup = BeautifulSoup(html, "html.parser")
        tables = soup.find_all("table")

        # If no tables at all, dump the full HTML for inspection
        if not tables:
            filename = re.sub(r"[^\w\s-]", "", (address or "no_address")).strip().replace(" ", "_")[:50]
            path = os.path.join(out_dir, f"{filename}_NO_TABLES.html")
            with open(path, "w", encoding="utf-8") as f:
                f.write(html)
            logging.info(f"[dump_results_tables] Saved full HTML (no <table> found) to: {path}")
            return path

        # Score tables by “result-ish” signals
        def score_table(tb):
            rows = tb.find_all("tr")
            cells = tb.find_all(["td", "th"])
            # header text (prefer <thead>, else first row)
            thead = tb.find("thead")
            if thead:
                header_text = thead.get_text(" ", strip=True).upper()
            elif rows:
                header_text = rows[0].get_text(" ", strip=True).upper()
            else:
                header_text = ""

            signals = 0
            for key in ("APN", "ASSESSOR", "PARCEL", "ADDRESS", "STREET", "CITY", "OWNER", "TRUSTOR", "ZIP"):
                if key in header_text:
                    signals += 1

            has_struct_ids = 1 if tb.find("tr", id=re.compile(r"^theRow\d+$")) else 0
            return (signals * 1000) + (has_struct_ids * 500) + (len(rows) * 10) + len(cells)

        ranked = sorted(tables, key=score_table, reverse=True)
        selected = [t for t in ranked[:max_tables] if score_table(t) > 0]

        # Fallback: just pick the biggest few tables if scoring didn't find anything
        if not selected:
            selected = [
                t for _, t in sorted(
                    ((len(t.find_all("tr")), t) for t in tables),
                    key=lambda x: x[0],
                    reverse=True
                )[:max_tables]
            ]

        # Save a tiny standalone page containing only the selected tables
        safe_addr = re.sub(r"[^\w\s-]", "", (address or "results")).strip().replace(" ", "_")[:60]
        ts = datetime.now().strftime("%H%M%S")
        path = os.path.join(out_dir, f"{safe_addr}_TABLES_{ts}.html")

        minimal_html = (
            "<!doctype html><html><head><meta charset='utf-8'>"
            "<title>Retran Results Table Dump</title>"
            "<style>body{font-family:Arial,Helvetica,sans-serif;font-size:13px;padding:12px}"
            "table{border-collapse:collapse;margin:12px 0;max-width:100%}"
            "td,th{border:1px solid #ccc;padding:6px;vertical-align:top}"
            "th{background:#f3f3f3;font-weight:bold}</style></head><body>"
            f"<h3>Results table dump for: {address}</h3>"
            + "\n<hr/>\n".join(str(t) for t in selected)
            + "</body></html>"
        )

        with open(path, "w", encoding="utf-8") as f:
            f.write(minimal_html)

        logging.info(f"[dump_results_tables] Saved {len(selected)} result table(s) to: {path}")
        return path

    except Exception as e:
        logging.exception(f"[dump_results_tables] Failed to dump tables: {e}")
        return None



def read_table(path: str, sheet: Optional[str] = None) -> pd.DataFrame:
    """Read Excel or CSV file into DataFrame with better error handling"""
    if not os.path.exists(path):
        raise FileNotFoundError(f"Input file not found: {path}")
        
    ext = os.path.splitext(path.lower())[1]
    
    try:
        if ext in [".xlsx", ".xlsm", ".xls"]:
            if sheet is None:
                excel_file = pd.ExcelFile(path)
                sheet_names = excel_file.sheet_names
                if not sheet_names:
                    raise ValueError(f"No sheets found in Excel file: {path}")
                sheet = sheet_names[0]
                logging.info(f"No sheet specified, using first sheet: '{sheet}'")
            df = pd.read_excel(path, sheet_name=sheet)
        elif ext in [".csv", ".tsv"]:
            sep = "," if ext == ".csv" else "\t"
            # Try different encodings
            for encoding in ['utf-8', 'latin1', 'cp1252']:
                try:
                    df = pd.read_csv(path, sep=sep, encoding=encoding)
                    logging.debug(f"Successfully read CSV with encoding: {encoding}")
                    break
                except UnicodeDecodeError:
                    continue
            else:
                raise ValueError(f"Could not read CSV file with any supported encoding")
        else:
            raise ValueError(f"Unsupported file extension: {ext}. Use .xlsx/.xlsm/.xls or .csv/.tsv")
        
        logging.info(f"Successfully loaded {len(df)} rows from {path}")
        return df
        
    except Exception as e:
        logging.error(f"Failed to read file {path}: {e}")
        raise


def write_table(df: pd.DataFrame, path: str) -> None:
    """Write DataFrame to Excel or CSV file with backup"""
    ext = os.path.splitext(path.lower())[1]
    
    try:
        if ext in [".xlsx", ".xlsm", ".xls"]:
            df.to_excel(path, index=False)
        elif ext in [".csv", ".tsv"]:
            sep = "," if ext == ".csv" else "\t"
            df.to_csv(path, index=False, sep=sep, encoding='utf-8')
        else:
            raise ValueError(f"Unsupported file extension for output: {ext}")
        
        logging.info(f"Successfully wrote {len(df)} rows to {path}")
        
    except Exception as e:
        logging.error(f"Failed to write file {path}: {e}")
        raise


def create_backup(file_path: str) -> str:
    """Create a timestamped backup of the input file"""
    if not os.path.exists(file_path):
        return ""
        
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path_obj = Path(file_path)
    backup_path = path_obj.parent / f"{path_obj.stem}_backup_{timestamp}{path_obj.suffix}"
    
    try:
        import shutil
        shutil.copy2(file_path, backup_path)
        logging.info(f"Backup created: {backup_path}")
        return str(backup_path)
    except Exception as e:
        logging.warning(f"Failed to create backup: {e}")
        return ""


def detect_columns(df: pd.DataFrame) -> Tuple[Optional[str], Optional[str]]:
    """Enhanced column detection with fuzzy matching"""
    columns = df.columns.tolist()
    address_col = None
    apn_col = None
    
    # Address column detection (in order of preference)
    address_patterns = [
        r'^address$',
        r'^property.?address$',
        r'^street.?address$',
        r'^full.?address$',
        r'address',
        r'addr',
        r'location',
        r'property'
    ]
    
    for pattern in address_patterns:
        for col in columns:
            if re.match(pattern, str(col).lower().strip()):
                address_col = col
                break
        if address_col:
            break
    
    # APN column detection (in order of preference)
    apn_patterns = [
        r'^apn$',
        r'^assessor.?parcel.?number$',
        r'^parcel.?number$',
        r'^parcel.?id$',
        r'apn',
        r'parcel',
        r'assessor'
    ]
    
    for pattern in apn_patterns:
        for col in columns:
            if re.match(pattern, str(col).lower().strip()):
                apn_col = col
                break
        if apn_col:
            break
    
    return address_col, apn_col


def login_to_retran(session: requests.Session, username: str, password: str, 
    login_url: str = RETRAN_LOGIN_URL, timeout: int = 30) -> bool:
    """Enhanced login with better error handling"""
    try:
        logging.info(f"Attempting to log in as '{username}'...")
        
        # Set session headers
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        })
        
        # Get login page
        login_page_resp = session.get(login_url, timeout=timeout)
        login_page_resp.raise_for_status()
        
        soup = BeautifulSoup(login_page_resp.text, "html.parser")
        login_form = soup.find("form")
        
        if not login_form:
            logging.error("No login form found on the page")
            return False
        
        # Build form data
        form_data = {}
        
        # Get hidden fields and other inputs
        for input_field in login_form.find_all("input"):
            field_name = input_field.get("name")
            field_type = input_field.get("type", "").lower()
            field_value = input_field.get("value", "")
            
            if field_name and field_type == "hidden":
                form_data[field_name] = field_value
        
        # Add credentials with common field name variations
        common_user_fields = ["username", "user", "login", "email"]
        common_pass_fields = ["password", "pass", "pwd"]
        
        for field in common_user_fields:
            form_data[field] = username
        for field in common_pass_fields:
            form_data[field] = password
        
        # Determine form action
        form_action = login_form.get("action", "")
        if form_action.startswith("http"):
            post_url = form_action
        else:
            post_url = urljoin(login_url, form_action) if form_action else login_url
        
        # Submit login
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
            "Origin": RETRAN_BASE_URL,
            "Referer": login_url,
        }
        
        login_resp = session.post(post_url, data=form_data, headers=headers, 
                                 allow_redirects=True, timeout=timeout)
        login_resp.raise_for_status()
        
        # Check for success
        response_text = login_resp.text.lower()
        failure_indicators = ["invalid", "incorrect", "failed", "error", "denied"]
        success_indicators = ["welcome", "dashboard", "logout", "search"]
        
        if any(indicator in response_text for indicator in failure_indicators):
            logging.error("Login failed - found failure indicator in response")
            return False
        
        if any(indicator in response_text for indicator in success_indicators):
            logging.info("Login successful!")
            return True
        
        # Test access to search page
        try:
            test_resp = session.get(RETRAN_SEARCH_URL, timeout=timeout)
            if test_resp.status_code == 200 and "search" in test_resp.text.lower():
                logging.info("Login verified by accessing search page")
                return True
        except Exception:
            pass
        
        logging.warning("Login success uncertain - proceeding anyway")
        return True
        
    except requests.RequestException as e:
        logging.error(f"Login request failed: {e}")
        return False
    except Exception as e:
        logging.error(f"Login error: {e}")
        return False


def save_debug_html(html: str, address: str, debug_dir: str = "debug_html"):
    """Save HTML response to file for debugging"""
    try:
        os.makedirs(debug_dir, exist_ok=True)
        # Clean filename
        safe_address = re.sub(r'[^\w\s-]', '', address.replace(' ', '_'))[:50]
        timestamp = datetime.now().strftime("%H%M%S")
        filename = f"{safe_address}_{timestamp}.html"
        filepath = os.path.join(debug_dir, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(html)
        
        logging.debug(f"Saved debug HTML to: {filepath}")
        return filepath
    except Exception as e:
        logging.warning(f"Failed to save debug HTML: {e}")
        return None

"""

------------------------------------------------------------------------
imporntant
------------------------------------------------------------------------

"""
def parse_apn_from_html(html: str, address: str = "", verbose: bool = False, save_debug: bool = False) -> Optional[str]:
    """
    Parse APNs from the fully-rendered Retran results page.

    Strategy
    - Find rows with ids theRow1, theRow2, ... (inside or outside #window-float).
    - For each row, use the 3rd <td> (APN / Street / City).
    - Take the first text chunk before the first <br> as the APN candidate.
    - Prefer rows that contain the searched address (case/space-insensitive).
    - Deduplicate and return all APNs joined with '; '.
    """
    if not html:
        if verbose:
            logging.info("[parse] Empty HTML; address='%s' -> APNs=None", address)
        return None

    soup = BeautifulSoup(html, "html.parser")

    # Normalizers and validators
    def _norm(s: str) -> str:
        return re.sub(r"\s+", " ", (s or "")).strip().upper()

    target = _norm(address) if address else ""

    # Prefer dashed APNs; fallback allows A-Z/0-9/- with at least one digit
    dashed = re.compile(r"\b\d{2,4}-\d{3,5}-\d{2,3}\b")
    fallback = re.compile(r"\b(?=[A-Z0-9-]{6,20}\b)(?=.*\d)[A-Z0-9-]+\b", re.I)

    def _is_valid_apn(txt: str) -> bool:
        return bool(dashed.fullmatch(txt) or fallback.fullmatch(txt))

    def _first_chunk(cell_html: str) -> str:
        # text before the first <br>
        first = re.split(r"<br\s*/?>", cell_html, flags=re.I, maxsplit=1)[0]
        return BeautifulSoup(first, "html.parser").get_text(" ", strip=True)

    # Gather all result rows (the site uses ids theRow1, theRow2, ...)
    all_rows = soup.find_all("tr", id=re.compile(r"^theRow\d+$"))
    if not all_rows:
        # Rare fallback: look under #window-float if ids are missing
        wf = soup.find(id="window-float")
        if wf:
            all_rows = wf.find_all("tr")

    # Prefer rows that contain the searched address text
    preferred = []
    if target:
        for r in all_rows:
            if target in _norm(r.get_text(" ", strip=True)):
                preferred.append(r)
    rows = preferred if preferred else all_rows

    # Extract APNs from the 3rd <td> of each row
    apns = []
    for r in rows:
        tds = r.find_all("td")
        if len(tds) < 3:
            continue
        apn_cell = tds[2]
        leading = _first_chunk(str(apn_cell))
        # dashed first, then fallback
        found = None
        m = dashed.search(leading)
        if m:
            cand = m.group(0).strip()
            if _is_valid_apn(cand):
                found = cand
        if not found:
            m2 = fallback.search(leading)
            if m2:
                cand = m2.group(0).strip()
                if _is_valid_apn(cand):
                    found = cand
        if found:
            apns.append(found)

    # Deduplicate in order
    out = []
    seen = set()
    for a in apns:
        if a not in seen:
            seen.add(a)
            out.append(a)

    if verbose:
        if out:
            logging.info("[parse] Address='%s' -> APNs found: %s", address, ", ".join(out))
        else:
            logging.info("[parse] Address='%s' -> APNs found: NONE", address)

    return "; ".join(out) if out else None








# Add these arguments to your argument parser (find the parser.add_argument section):
# parser.add_argument("--save-debug-html", action="store_true", help="Save HTML responses for debugging")
# parser.add_argument("--debug-address", help="Debug specific address")

# In your fetch_apn_for_address function, change this line:
# apn = parse_apn_from_html(resp.text, verbose=verbose)
# to:
# apn = parse_apn_from_html(resp.text, verbose=verbose, address=address, save_debug=args.save_debug_html)
def fetch_apn_for_address(searcher: APNSearcher, address: str, timeout: int,
                          verbose: bool = False, save_debug: bool = False) -> Tuple[Optional[str], str]:
    """
    Fully-rendered scrape using the SAME authenticated session cookies:
      - Transfers cookies from searcher.session (requests) into Playwright (no re-login).
      - Enters via /search.asp, fills and submits the real form.
      - Neutralizes page logout traps (on*unload -> CheckBrowser -> logout.asp).
      - Blocks any request to logout.asp.
      - Waits for JS-rendered results (including iframe) and captures the final DOM.
      - Parses APNs via parse_apn_from_html(final_html, address=...).

    Falls back to requests-based flow if Playwright isn't available.
    """

    # NEW: skip empty/blank addresses
    if not address or not str(address).strip():
        if verbose:
            logging.debug("[fetch] Skipping empty address.")
        searcher.cache_result(address or "", None)
        return None, "skipped_empty"

    # Cache
    cached = searcher.get_cached_result(address)
    if cached is not None:
        searcher.stats['cache_hits'] = searcher.stats.get('cache_hits', 0) + 1
        return cached, ('found' if cached else 'not_found')

    # Try Playwright; fallback to requests if missing
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    except Exception:
        if verbose:
            logging.error("Playwright not available. Install with: pip install playwright && python -m playwright install chromium")
        # Minimal requests fallback (still follows iframe)
        try:
            headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                "Origin": RETRAN_BASE_URL,
                "Referer": "https://retran.net/search.asp",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "User-Agent": searcher.session.headers.get("User-Agent", "Mozilla/5.0"),
            }
            try:
                searcher.session.get("https://retran.net/search.asp", timeout=timeout, allow_redirects=True)
            except Exception:
                pass
            data = {
                "txtSearch": re.sub(r"\s+", "%", (address or "").strip().upper()),
                "select": "Property Address",
                "sortIndex": "tor_mailing_city",
                "sortType": "asc",
                "startNum": "1",
                "endNum": "100",
                "Submit": "Search",
            }
            resp = searcher.session.post(RETRAN_SEARCH_URL, data=data, headers=headers,
                                         allow_redirects=True, timeout=timeout)
            resp.raise_for_status()
            html = resp.text
            soup = BeautifulSoup(html, "html.parser")
            frame = soup.find("iframe", src=True)
            if frame:
                fr = searcher.session.get(urljoin(RETRAN_SEARCH_URL, frame["src"]),
                                          timeout=timeout, allow_redirects=True)
                fr.raise_for_status()
                html = fr.text
            apns = parse_apn_from_html(html, address=address, verbose=verbose, save_debug=False)
            searcher.cache_result(address, apns)
            return (apns, 'found' if apns else 'not_found')
        except Exception:
            searcher.cache_result(address, None)
            return None, 'error'

    wait_ms = max(7000, int(timeout * 1000))

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            ua = searcher.session.headers.get("User-Agent", (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ))
            context = browser.new_context(user_agent=ua)

            # Transfer the EXACT same cookies (avoid 30-min lockout)
            try:
                jar = searcher.session.cookies
                pw_cookies = []
                for c in jar:
                    dom = (c.domain or "retran.net")
                    if "retran.net" not in dom:
                        continue
                    domain = dom if dom.startswith(".") else f".{dom}"
                    cookie = {
                        "name": c.name,
                        "value": c.value,
                        "domain": domain,
                        "path": c.path or "/",
                        "secure": True,
                        "httpOnly": False,
                        "sameSite": "Lax",
                    }
                    try:
                        if getattr(c, "expires", None):
                            cookie["expires"] = int(c.expires)
                    except Exception:
                        pass
                    pw_cookies.append(cookie)
                if pw_cookies:
                    context.add_cookies(pw_cookies)
                    if verbose:
                        logging.debug(f"Transferred {len(pw_cookies)} cookies to browser context.")
            except Exception as e:
                if verbose:
                    logging.debug(f"Cookie transfer skipped/failed: {e}")

            # Block any attempt to hit logout.asp
            def _route_block_logout(route, request):
                url = request.url.lower()
                if "logout.asp" in url:
                    return route.abort()
                return route.continue_()
            context.route("**/*", _route_block_logout)

            page = context.new_page()

            # Neutralize the logout-on-unload booby-traps BEFORE any page scripts run
            page.add_init_script("""
                try {
                  window.clicked = true;
                  window.CheckBrowser = function(){};
                  Object.defineProperty(window, 'onbeforeunload', { get(){return null;}, set(){}});
                  Object.defineProperty(window, 'onunload', { get(){return null;}, set(){}});
                  window.addEventListener('beforeunload', e => { e.stopImmediatePropagation(); }, true);
                  window.addEventListener('unload', e => { e.stopImmediatePropagation(); }, true);
                } catch(e){}
            """)

            # 1) Enter via /search.asp using existing cookies (no login page)
            entry_url = "https://retran.net/search.asp"
            page.goto(entry_url, timeout=wait_ms)

            # 2) Fill and submit the real search
            encoded = re.sub(r"\s+", "%", (address or "").strip().upper())

            if page.locator("input[name='txtSearch']").count() == 0:
                # fallback to results page (still authenticated)
                page.goto(RETRAN_SEARCH_URL, timeout=wait_ms)

            if page.locator("input[name='txtSearch']").count() > 0:
                page.fill("input[name='txtSearch']", encoded)
            else:
                page.evaluate("(v)=>{let i=document.createElement('input');i.type='hidden';i.name='txtSearch';i.value=v;(document.forms[0]||document.body).appendChild(i);}", encoded)

            try:
                page.select_option("select[name='select']", label="Property Address")
            except Exception:
                try:
                    page.select_option("select[name='select']", value="Property Address")
                except Exception:
                    page.evaluate("""() => {
                        let i=document.createElement('input');
                        i.type='hidden'; i.name='select'; i.value='Property Address';
                        (document.forms[0]||document.body).appendChild(i);
                    }""")

            # Server-expected hidden fields
            page.evaluate("""
                () => {
                  const ensure = (n,v)=>{
                    let el=document.querySelector(`input[name="${n}"]`);
                    if(!el){el=document.createElement('input');el.type='hidden';el.name=n;(document.forms[0]||document.body).appendChild(el);}
                    el.value=v;
                  };
                  ensure('sortIndex','tor_mailing_city');
                  ensure('sortType','asc');
                  ensure('startNum','1');
                  ensure('endNum','100');
                  ensure('sortBy','');
                  ensure('sortBee','');
                }
            """)

            # Submit
            if page.locator("form input[type=submit]").count() > 0:
                page.click("form input[type=submit]")
            elif page.locator("form button[type=submit]").count() > 0:
                page.click("form button[type=submit]")
            else:
                page.evaluate("()=>{ const f=document.forms[0]; if(f) f.submit(); }")

            # 3) Wait for render
            try:
                page.wait_for_load_state("networkidle", timeout=wait_ms)
            except PWTimeout:
                pass

            # 4) Capture fully rendered DOM (prefer the frame that has result rows)
            def collect_all_frames_html() -> List[Tuple[str, str, str]]:
                out = []
                try:
                    out.append(("main", page.url + "", page.evaluate("document.documentElement.outerHTML")))
                except Exception:
                    pass
                for fr in page.frames:
                    if fr == page.main_frame:
                        continue
                    try:
                        out.append((fr.name or "frame", fr.url + "", fr.evaluate("document.documentElement.outerHTML")))
                    except Exception:
                        continue
                return out

            final_html = None
            deadline = time.time() + (wait_ms / 1000.0)
            while time.time() < deadline and final_html is None:
                for (_fname, _url, html) in collect_all_frames_html():
                    if re.search(r'id=["\']theRow\\d+["\']', html):
                        final_html = html
                        break
                if final_html is None:
                    time.sleep(0.2)

            if final_html is None:
                # pick largest DOM for debugging
                bundles = collect_all_frames_html()
                if bundles:
                    final_html = max(bundles, key=lambda b: len(b[2]))[2]
                else:
                    final_html = page.evaluate("document.documentElement.outerHTML")

            if save_debug and address:
                os.makedirs("debug_html", exist_ok=True)
                ts = datetime.now().strftime("%H%M%S")
                try:
                    with open(os.path.join("debug_html", f"{re.sub(r'[^A-Za-z0-9_-]+','_',address)}_RENDERED_{ts}.html"),
                              "w", encoding="utf-8") as f:
                        f.write(final_html)
                except Exception:
                    pass

            apns_joined = parse_apn_from_html(final_html or "", address=address, verbose=verbose, save_debug=False)

            browser.close()
            searcher.cache_result(address, apns_joined)
            return (apns_joined, 'found' if apns_joined else 'not_found')

    except Exception as e:
        if verbose:
            logging.debug(f"Playwright flow failed for '{address}': {e}")
        searcher.cache_result(address, None)
        return None, 'error'





def process_addresses_parallel(searcher: APNSearcher, addresses_data: List[Tuple], 
                              max_workers: int, rate_limit: float, timeout: int, 
                              verbose: bool) -> List[Tuple]:
    """Process addresses in parallel with rate limiting"""
    results = []
    
    def worker(address_data):
        idx, address = address_data
        result = fetch_apn_for_address(searcher, address, timeout, verbose)
        time.sleep(rate_limit)  # Rate limiting per worker
        return (idx, address, result[0], result[1])
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_data = {executor.submit(worker, addr_data): addr_data 
                         for addr_data in addresses_data}
        
        for future in as_completed(future_to_data):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                addr_data = future_to_data[future]
                logging.error(f"Error processing {addr_data[1]}: {e}")
                results.append((addr_data[0], addr_data[1], None, 'error'))
    
    return sorted(results, key=lambda x: x[0])  # Sort by original index


def export_failed_lookups(failed_data: List[Dict], output_path: str):
    """Export failed lookups to a separate file for manual review"""
    if not failed_data:
        return
        
    try:
        failed_df = pd.DataFrame(failed_data)
        ext = os.path.splitext(output_path.lower())[1]
        
        if ext in [".xlsx", ".xlsm", ".xls"]:
            failed_df.to_excel(output_path, index=False)
        else:
            failed_df.to_csv(output_path, index=False)
            
        logging.info(f"Exported {len(failed_data)} failed lookups to {output_path}")
        
    except Exception as e:
        logging.error(f"Failed to export failed lookups: {e}")

def main():
    parser = argparse.ArgumentParser(description="Enhanced Retran APN Filler with caching and parallel processing.")
    # Required
    parser.add_argument("-i", "--input", required=True, help="Path to input Excel/CSV file")
    parser.add_argument("-o", "--output", required=True, help="Path to output Excel/CSV file")
    # Auth
    parser.add_argument("--username", help="Login username")
    parser.add_argument("--password", help="Login password (omit to be prompted)")
    parser.add_argument("--cookie", help="Cookie header for authentication")
    # File
    parser.add_argument("--sheet", help="Excel sheet name (default: first sheet)")
    parser.add_argument("--backup", action="store_true", help="Create backup of input file (input only)")
    # Processing
    parser.add_argument("--rate", type=float, default=1.0, help="Seconds between requests (default: 1.0)")
    parser.add_argument("--max-retries", type=int, default=3, help="Max retries per address (default: 3)")
    parser.add_argument("--timeout", type=int, default=30, help="Request/page timeout seconds (default: 30)")
    parser.add_argument("--parallel", type=int, default=1, help="Number of parallel workers (default: 1)")
    # Cache & resume
    parser.add_argument("--cache", action="store_true", help="Enable result caching")
    parser.add_argument("--cache-file", default=".retran_cache.json", help="Cache file path")
    parser.add_argument("--resume", action="store_true", help="Resume from previous run (requires --cache)")
    # Output
    parser.add_argument("--failed-output", help="Export failed lookups to separate file")
    parser.add_argument("--progress", action="store_true", default=True, help="Show progress bar")
    # Debug
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")
    parser.add_argument("--debug-first", type=int, help="Only process first N rows")
    parser.add_argument("--save-debug-html", action="store_true", help="Save HTML responses to ./debug_html")
    parser.add_argument("--debug-address", help="Debug a single address (search and dump rendered HTML)")

    args = parser.parse_args()

    # Logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(f"retran_apn_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
        ]
    )

    failed_lookups: List[Dict] = []
    searcher: Optional[APNSearcher] = None
    df_out: Optional[pd.DataFrame] = None

    try:
        # Optional: backup INPUT (we never modify it anyway)
        if args.backup:
            create_backup(args.input)

        # Read INPUT (never mutate)
        logging.info(f"Reading input file: {args.input}")
        df_in = read_table(args.input, sheet=args.sheet)
        logging.info(f"Available columns: {list(df_in.columns)}")

        # Make a deep COPY we will modify and write
        df_out = df_in.copy(deep=True)

        # Detect/normalize columns in COPY
        address_col, apn_col = detect_columns(df_out)
        if not address_col:
            logging.error("Could not find an address column. Please ensure your file has a column containing 'address'.")
            sys.exit(1)

        mapping = {}
        if address_col != "Address":
            mapping[address_col] = "Address"
        if not apn_col:
            df_out["APN"] = ""
            apn_col = "APN"
        elif apn_col != "APN":
            mapping[apn_col] = "APN"
        if mapping:
            df_out = df_out.rename(columns=mapping)
            logging.info(f"Mapped columns in output copy: {mapping}")

        if "Time APN Added" not in df_out.columns:
            df_out["Time APN Added"] = ""

        # Limit rows if debugging
        if args.debug_first:
            n = min(args.debug_first, len(df_out))
            df_out = df_out.head(n)
            logging.info(f"Debug mode: processing only first {n} rows")

        # Session/searcher
        searcher = APNSearcher(cache_enabled=args.cache, cache_file=args.cache_file)
        if args.cookie:
            searcher.session.headers["Cookie"] = args.cookie
            logging.info("Using provided Cookie header")

        if args.username:
            pw = args.password or getpass.getpass(f"Password for {args.username}: ")
            ok = login_to_retran(searcher.session, args.username, pw, RETRAN_LOGIN_URL, args.timeout)
            if not ok:
                logging.error("Login failed. Searches may not work.")

        # Single-address debug mode (no file writes)
        if args.debug_address:
            logging.info(f"=== DEBUG MODE: Testing address '{args.debug_address}' ===")
            try:
                try:
                    searcher.session.get(RETRAN_SEARCH_URL, timeout=args.timeout, allow_redirects=True)
                except Exception:
                    pass
                apn, status = fetch_apn_for_address(
                    searcher, args.debug_address, args.timeout, verbose=True, save_debug=args.save_debug_html
                )
                logging.info(f"=== DEBUG RESULT: APN='{apn}', Status='{status}' ===")
            finally:
                try:
                    searcher.save_cache()
                except Exception:
                    pass
            return

        # Build worklist from COPY (only blanks with address)
        work: List[Tuple[int, str]] = []
        for idx, row in df_out.iterrows():
            addr = str(row.get("Address", "")).strip()
            apn_cur = str(row.get("APN", "")).strip()
            if addr and (apn_cur == "" or apn_cur.lower() in ("nan", "none")):
                work.append((idx, addr))

        if not work:
            logging.info("No addresses require APN lookup!")
            write_table(df_out, args.output)
            return

        logging.info(f"Processing {len(work)} addresses requiring APN lookup")

        # Lookup runner with retries
        def do_lookup(idx: int, addr: str) -> Tuple[int, Optional[str], str]:
            last_status = "error"
            apn_val: Optional[str] = None
            for _ in range(args.max_retries):
                apn_val, last_status = fetch_apn_for_address(
                    searcher, addr, args.timeout, verbose=args.verbose, save_debug=args.save_debug_html
                )
                if apn_val or last_status in ("found", "not_found"):
                    break
                time.sleep(args.rate)
            return idx, apn_val, last_status

        results: List[Tuple[int, Optional[str], str]] = []

        # Sequential or parallel
        if args.parallel and args.parallel > 1:
            packed = [(i, a) for (i, a) in work]
            par = process_addresses_parallel(
                searcher, packed, max_workers=args.parallel, rate_limit=args.rate,
                timeout=args.timeout, verbose=args.verbose
            )
            for idx, _addr, apn_val, status in par:
                results.append((idx, apn_val, status))
        else:
            iterator = work
            if args.progress and TQDM_AVAILABLE:
                iterator = tqdm(work, desc="APN lookups", unit="row")
            for (idx, addr) in iterator:
                idx2, apn_val, status = do_lookup(idx, addr)
                time.sleep(args.rate)
                results.append((idx2, apn_val, status))

        # Apply results only to COPY; stamp time only when newly filled
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        filled = 0
        for idx, apn_val, status in results:
            if apn_val and apn_val.strip():
                current = str(df_out.at[idx, "APN"]).strip() if pd.notna(df_out.at[idx, "APN"]) else ""
                if not current:
                    df_out.at[idx, "APN"] = apn_val.strip()
                    df_out.at[idx, "Time APN Added"] = now_str
                    filled += 1
            else:
                if args.failed_output:
                    failed_lookups.append({
                        "row_index": idx,
                        "Address": df_out.at[idx, "Address"],
                        "Status": status
                    })

        logging.info(f"Filled {filled} new APN(s).")

        # Save cache and failures
        try:
            searcher.save_cache()
        except Exception as e:
            logging.warning(f"Could not save cache: {e}")

        if args.failed_output and failed_lookups:
            try:
                export_failed_lookups(failed_lookups, args.failed_output)
            except Exception as e:
                logging.error(f"Failed to export failed lookups: {e}")

        # Write ONLY the COPY
        write_table(df_out, args.output)
        logging.info("=== Processing complete ===")

    except Exception as e:
        logging.exception("Fatal error during processing: %s", e)
        try:
            if df_out is not None:
                write_table(df_out, args.output)
        except Exception:
            pass




if __name__ == "__main__":
    main()

            