#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
TitlePro247 Foreclosure Update (Standalone)
-------------------------------------------
- Reads Excel/CSV with columns: Address, City, and optionally foreclosure fields
- Logs into https://www.titlepro247.com
- For every row with non-empty Address and City, searches by Address + City (CA)
- Scrapes 5 foreclosure fields: Loan Amount, NOD, Sale Date, Back to Beneficiary On, Bene/Client Name
- Updates only changed fields and maintains Last Checked timestamp

Quick start:
  pip install playwright pandas tqdm python-dateutil
  python -m playwright install chromium

Run (headless):
  python titlepro_foreclosure_update.py -i INPUT.xlsx -o OUTPUT.xlsx --username "USER" --password "PASS"

Show the browser (debug):
  python titlepro_foreclosure_update.py -i INPUT.xlsx -o OUTPUT.xlsx --username "USER" --password "PASS" --headful --save-debug -v
"""

import argparse
import getpass
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple, List, Any

import pandas as pd

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False

try:
    from dateutil import parser as date_parser
    DATEUTIL_AVAILABLE = True
except ImportError:
    DATEUTIL_AVAILABLE = False


# =============================================================================
# Data Model
# =============================================================================

@dataclass
class ForeclosureInfo:
    """Container for the 5 foreclosure fields scraped from TitlePro247"""
    loan_amount: Optional[str] = None
    nod: Optional[str] = None
    sale_date: Optional[str] = None
    back_to_beneficiary_on: Optional[str] = None
    bene_client_name: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Optional[str]]:
        return {
            "Loan Amount": self.loan_amount,
            "NOD": self.nod,
            "Sale Date": self.sale_date,
            "Back to Beneficiary On": self.back_to_beneficiary_on,
            "Bene/Client Name": self.bene_client_name
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Optional[str]]) -> 'ForeclosureInfo':
        return cls(
            loan_amount=data.get("Loan Amount"),
            nod=data.get("NOD"),
            sale_date=data.get("Sale Date"),
            back_to_beneficiary_on=data.get("Back to Beneficiary On"),
            bene_client_name=data.get("Bene/Client Name")
        )


# =============================================================================
# Selectors / Config (tuned to TitlePro247 foreclosure data)
# =============================================================================

TITLEPRO_BASE_URL = "https://www.titlepro247.com"
LOGIN_URL = TITLEPRO_BASE_URL

# --- Login form (from existing script) ---
SEL_LOGIN_FORM   = "form[name='login']"
SEL_USER         = "input[name='username']"
SEL_PASS         = "input[name='password']"
SEL_SUBMIT       = "input[type='submit'][value='LOGIN']"

# --- Post-login search UI ---
SEL_ADDR_INPUT_CANDIDATES = [
    "input[placeholder*='Street'][type='text']",
    "input[aria-label*='Street']",
    "input#txtAddress",
]

SEL_CITY_OR_COUNTY_SELECTS = [
    "select[name*='county']",
    "select[name*='city']",
    "select#ddlCounty",
    "select#ddlCity",
    "div select",
]

SEL_STATE_SELECTS = [
    "select[name*='state']",
    "select#ddlState",
]

SEL_SEARCH_BUTTONS = [
    "button:has-text('SEARCH')",
    "input[type='submit'][value*='SEARCH']",
    "button[aria-label*='Search']",
]

# --- Results parsing for foreclosure data ---
SEL_RESULTS_TABLE_CANDIDATES = [
    "table#searchResultsList",
    "table.results",
    "table.data-grid",
    "table:has(th:has-text('Loan'))",
    "table:has(th:has-text('NOD'))",
    "table:has(th:has-text('Sale'))",
]

# Header matchers for foreclosure fields (case-insensitive)
FORECLOSURE_HEADER_MATCHERS = {
    "Loan Amount": ["loan amount", "loan", "amount", "loanamt"],
    "NOD": ["nod", "notice of default", "default"],
    "Sale Date": ["sale date", "sale", "auction date"],
    "Back to Beneficiary On": ["back to beneficiary on", "beneficiary", "back to bene", "back to bene on"],
    "Bene/Client Name": ["bene/client name", "beneficiary", "client", "bene name", "beneficiary name"]
}


# =============================================================================
# Normalization Helpers
# =============================================================================

def normalize_currency(text: str) -> Optional[str]:
    """Normalize currency to $X,XXX.XX format if possible, else return cleaned text"""
    if not text:
        return None
    
    # Remove common currency symbols and extra whitespace
    cleaned = re.sub(r'[$,\s]+', '', str(text).strip())
    
    # Check if it's a valid number
    try:
        # Handle decimal points
        if '.' in cleaned:
            parts = cleaned.split('.')
            if len(parts) == 2 and parts[0].isdigit() and parts[1].isdigit():
                amount = float(cleaned)
                return f"${amount:,.2f}"
        elif cleaned.isdigit():
            amount = float(cleaned)
            return f"${amount:,.0f}"
    except (ValueError, IndexError):
        pass
    
    # If not a clean number, return trimmed original
    return str(text).strip() or None


def normalize_date(text: str) -> Optional[str]:
    """Normalize date to YYYY-MM-DD format if possible"""
    if not text:
        return None
    
    text = str(text).strip()
    if not text:
        return None
    
    # Try dateutil parser first if available
    if DATEUTIL_AVAILABLE:
        try:
            parsed = date_parser.parse(text)
            return parsed.strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            pass
    
    # Fallback: common date patterns
    date_patterns = [
        (r'(\d{1,2})/(\d{1,2})/(\d{4})', r'\3-\1-\2'),  # MM/DD/YYYY
        (r'(\d{4})-(\d{1,2})-(\d{1,2})', r'\1-\2-\3'),  # YYYY-MM-DD
        (r'(\d{1,2})-(\d{1,2})-(\d{4})', r'\3-\1-\2'),  # MM-DD-YYYY
    ]
    
    for pattern, replacement in date_patterns:
        match = re.search(pattern, text)
        if match:
            try:
                # Validate the date
                formatted = re.sub(pattern, replacement, text)
                year, month, day = formatted.split('-')
                if 1 <= int(month) <= 12 and 1 <= int(day) <= 31:
                    return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
            except (ValueError, IndexError):
                continue
    
    # Return original if no pattern matches
    return text


def normalize_field_value(field_type: str, text: str) -> Optional[str]:
    """Normalize field value based on field type"""
    if not text:
        return None
    
    text = str(text).strip()
    if not text:
        return None
    
    if field_type == "Loan Amount":
        return normalize_currency(text)
    elif field_type in ["NOD", "Sale Date", "Back to Beneficiary On"]:
        return normalize_date(text)
    else:  # Bene/Client Name and others
        return text


def values_changed(old_dict: Dict[str, Optional[str]], new_dict: Dict[str, Optional[str]]) -> Tuple[bool, List[str]]:
    """Compare two field dictionaries and return (changed, list_of_changed_fields)"""
    changed_fields = []
    
    for key in ["Loan Amount", "NOD", "Sale Date", "Back to Beneficiary On", "Bene/Client Name"]:
        old_val = old_dict.get(key)
        new_val = new_dict.get(key)
        
        # Normalize for comparison (case-insensitive, strip spaces)
        old_norm = (old_val or "").strip().lower()
        new_norm = (new_val or "").strip().lower()
        
        if old_norm != new_norm:
            changed_fields.append(key)
    
    return len(changed_fields) > 0, changed_fields


# =============================================================================
# IO helpers (from existing script)
# =============================================================================

def read_table(path: str, sheet: Optional[str] = None) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(path)
    ext = Path(path).suffix.lower()
    if ext in (".xlsx", ".xlsm", ".xls"):
        if sheet is None:
            x = pd.ExcelFile(path)
            sheet = x.sheet_names[0]
            logging.info(f"No sheet specified; using first sheet: '{sheet}'")
        return pd.read_excel(path, sheet_name=sheet)
    elif ext in (".csv", ".tsv"):
        sep = "," if ext == ".csv" else "\t"
        for enc in ("utf-8", "latin1", "cp1252"):
            try:
                return pd.read_csv(path, sep=sep, encoding=enc)
            except UnicodeDecodeError:
                continue
        raise ValueError("Could not decode CSV with utf-8/latin1/cp1252")
    else:
        raise ValueError("Unsupported input extension")

def write_table(df: pd.DataFrame, path: str) -> None:
    ext = Path(path).suffix.lower()
    if ext in (".xlsx", ".xlsm", ".xls"):
        df.to_excel(path, index=False)
    elif ext in (".csv", ".tsv"):
        sep = "," if ext == ".csv" else "\t"
        df.to_csv(path, index=False, sep=sep, encoding="utf-8")
    else:
        raise ValueError("Unsupported output extension")

def detect_columns(df: pd.DataFrame) -> Tuple[Optional[str], Optional[str]]:
    cols = [str(c).strip() for c in df.columns]
    def find_any(patterns: List[str]) -> Optional[str]:
        for pat in patterns:
            for c in cols:
                if re.fullmatch(pat, c, flags=re.I):
                    return c
        return None
    addr = find_any([r"address", r".*street.*address.*", r".*property.*address.*"])
    city = find_any([r"city", r".*mailing.*city.*", r".*property.*city.*"])
    return addr, city


# =============================================================================
# Cache (adapted for foreclosure data)
# =============================================================================

class ForeclosureCache:
    def __init__(self, path: str, enabled: bool):
        self.path = path
        self.enabled = enabled
        self.data: Dict[str, Dict[str, Any]] = {}
        if self.enabled and os.path.exists(self.path):
            try:
                self.data = json.load(open(self.path, "r"))
                logging.info(f"Loaded cache: {len(self.data)} entries")
            except Exception as e:
                logging.warning(f"Cache load failed: {e}")

    @staticmethod
    def _key(addr: str, city: str) -> str:
        na = re.sub(r"\s+", " ", (addr or "").strip().upper())
        nc = re.sub(r"\s+", " ", (city or "").strip().upper())
        return f"{na}||{nc}"

    def get(self, addr: str, city: str) -> Optional[ForeclosureInfo]:
        if not self.enabled: 
            return None
        rec = self.data.get(self._key(addr, city))
        if not rec: 
            return None
        return ForeclosureInfo.from_dict(rec.get("foreclosure_data", {}))

    def put(self, addr: str, city: str, foreclosure_info: ForeclosureInfo) -> None:
        if not self.enabled: 
            return
        self.data[self._key(addr, city)] = {"foreclosure_data": foreclosure_info.to_dict()}

    def save(self):
        if not self.enabled: 
            return
        try:
            json.dump(self.data, open(self.path, "w"), indent=2)
        except Exception as e:
            logging.warning(f"Cache save failed: {e}")


# =============================================================================
# Playwright automation (from existing script)
# =============================================================================

def ensure_playwright():
    try:
        from playwright.sync_api import sync_playwright
        return sync_playwright
    except Exception:
        raise RuntimeError(
            "Playwright not installed. Run:\n"
            "  pip install playwright\n"
            "  python -m playwright install chromium"
        )

def _first_existing_locator(page, selectors: List[str]):
    for sel in selectors:
        try:
            loc = page.locator(sel)
            if loc.count() > 0:
                return sel
        except Exception:
            continue
    return None

def login_titlepro(page, username: str, password: str, timeout_ms: int = 30000, verbose: bool = True) -> bool:
    """
    Robust TitlePro247 login (from existing script)
    """
    from playwright.sync_api import TimeoutError as PWTimeout

    BASE = "https://www.titlepro247.com"

    # Broad selectors for varied markup
    SEL_FORM         = "form"
    SEL_USERNAME     = "input#username, input#UserName, input[name*='user' i], input[name*='email' i]"
    SEL_PASSWORD     = "input#Password, input[type='password'], input[name*='pass' i]"
    SEL_SUBMIT       = "button[type='submit'], input[type='submit'], button:has-text('Sign In'), button:has-text('Login')"
    SEL_LOGIN_ENTRY  = "a:has-text('Login'), a:has-text('Sign In'), button:has-text('Login'), button:has-text('Sign In')"

    # Concrete search UI (from your screenshots/spec)
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
                    if verbose: logging.info("[login] Cookie banner dismissed")
                    break
            except Exception:
                continue

    if verbose:
        logging.info(f"[login] Navigating to {BASE}")

    # Go to the base page
    page.goto(BASE, timeout=timeout_ms, wait_until="domcontentloaded")
    _dismiss_cookies()

    # If no form yet, click a launcher if present
    try:
        if page.locator(SEL_FORM).count() == 0:
            if page.locator(SEL_LOGIN_ENTRY).count() > 0:
                page.locator(SEL_LOGIN_ENTRY).first.click()
                if verbose: logging.info("[login] Clicked Login/Sign In entry")
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
        # Sometimes the site navigates to a dedicated login URL after clicking entry
        try:
            page.wait_for_selector(SEL_FORM, timeout=timeout_ms)
            form = _find_form_on_page()
        except PWTimeout:
            form = _find_form_on_page()

    if not form:
        logging.error("[login] Could not find a login form.")
        return False

    # Fill username/password
    try:
        form.locator(SEL_USERNAME).first.fill(username, timeout=5000)
        form.locator(SEL_PASSWORD).first.fill(password, timeout=5000)
        if verbose: logging.info(f"[login] Filled credentials for '{username}'")
    except Exception as e:
        logging.error(f"[login] Failed to fill credentials: {e}")
        return False

    # Submit (click first matching submit, else Enter)
    try:
        if form.locator(SEL_SUBMIT).count() > 0:
            form.locator(SEL_SUBMIT).first.click(timeout=5000)
        else:
            form.press(SEL_PASSWORD, "Enter")
        if verbose: logging.info("[login] Submitted login form")
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

    # After login, some sites need a nudge to the home/search surface
    # Try clicking a nav item or just revisiting BASE
    for _ in range(2):
        try:
            # If the concrete search elements are already there, success
            if (page.locator(SEL_SEARCH_TYPE).count() > 0 and
                page.locator(SEL_ADDR_INPUT).count() > 0 and
                page.locator(SEL_CITY_INPUT).count() > 0):
                if verbose: logging.info("[login] Search UI present — login OK")
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
            if verbose: logging.info("[login] Search UI present — login OK")
            return True
    except Exception:
        pass

    logging.error("[login] Login flow completed, but search UI not detected (likely session drop).")
    return False


def fill_city_autocomplete(page, city: str, timeout_ms: int) -> None:
    """
    Types city into #CityStateZip and selects the first autocomplete entry that
    contains 'CA' and the city name (case-insensitive). If none match, picks the
    first entry that contains 'CA'. If no list renders, leaves typed value.
    """
    from playwright.sync_api import TimeoutError as PWTimeout

    city = (city or "").strip()
    if not city:
        return

    CITY_INPUT = "#CityStateZip"
    page.fill(CITY_INPUT, city)

    # Candidate dropdown containers
    candidates = [
        "ul.ui-autocomplete li",
        "div.ui-autocomplete li",
        "ul[role='listbox'] li",
        "div[role='listbox'] [role='option']",
        "div.autocomplete li",
    ]

    dropdown_sel = None
    for sel in candidates:
        try:
            page.wait_for_selector(sel, timeout=min(1500, timeout_ms))
            if page.locator(sel).count() > 0:
                dropdown_sel = sel
                break
        except PWTimeout:
            continue
        except Exception:
            continue

    if not dropdown_sel:
        return  # no dropdown appeared

    try:
        options = page.eval_on_selector_all(
            dropdown_sel,
            "els => els.map(e => ({text:(e.innerText||e.textContent||'').trim()}))"
        )
    except Exception:
        options = []

    target = city.upper()
    chosen_idx = None

    for i, o in enumerate(options):
        t = (o.get('text') or '').upper()
        if 'CA' in t and target in t:
            chosen_idx = i
            break

    if chosen_idx is None:
        for i, o in enumerate(options):
            t = (o.get('text') or '').upper()
            if ' CA' in t or t.endswith(',CA') or t.endswith(' CA'):
                chosen_idx = i
                break

    if chosen_idx is None and options:
        chosen_idx = 0

    if chosen_idx is None:
        return

    try:
        page.locator(dropdown_sel).nth(chosen_idx).click()
    except Exception:
        try:
            page.keyboard.press("Enter")
        except Exception:
            pass


# =============================================================================
# Foreclosure-specific scraping functions
# =============================================================================

def parse_results_from_grid_foreclosure(page, timeout_ms: int = 30000) -> List[ForeclosureInfo]:
    """
    Extracts foreclosure data from all rows in the results grid:
      <table id="searchResultsList">
        <td aria-describedby="searchResultsList_LoanAmount">...</td>
        <td aria-describedby="searchResultsList_NOD">...</td>
        etc.
    Returns list of ForeclosureInfo objects, one per result row.
    """
    from playwright.sync_api import TimeoutError as PWTimeout

    TABLE_SEL = "#searchResultsList"
    ROWS_SEL  = "#searchResultsList tbody tr[role='row']"

    try:
        page.wait_for_selector(TABLE_SEL, timeout=timeout_ms)
    except PWTimeout:
        logging.warning("[parse] Results grid not found (timeout).")
        return []

    try:
        rows = page.locator(ROWS_SEL)
        count = rows.count()
        if count == 0:
            return []

        # Build header map by reading table headers
        header_map = {}
        try:
            # Get headers from thead or first row
            header_cells = page.locator(f"{TABLE_SEL} thead th, {TABLE_SEL} tr:first-child th, {TABLE_SEL} tr:first-child td")
            for i in range(header_cells.count()):
                header_text = header_cells.nth(i).inner_text().strip().lower()
                header_text = re.sub(r'\s+', ' ', header_text)  # normalize spaces
                
                # Match against our field patterns
                for field_name, patterns in FORECLOSURE_HEADER_MATCHERS.items():
                    for pattern in patterns:
                        if pattern in header_text or header_text in pattern:
                            header_map[field_name] = i
                            break
                    if field_name in header_map:
                        break
        except Exception as e:
            logging.debug(f"[parse] Header mapping failed: {e}")

        # Parse each data row
        results = []
        for i in range(count):
            try:
                row = rows.nth(i)
                
                # Skip header rows (those without proper row IDs)
                rid = (row.get_attribute("id") or "").strip()
                if not rid:
                    continue
                
                foreclosure_data = {}
                
                # Extract each field using header map or aria-describedby
                for field_name in FORECLOSURE_HEADER_MATCHERS.keys():
                    cell_text = None
                    
                    # Try aria-describedby first (most reliable)
                    aria_sel = f"td[aria-describedby*='{field_name.replace(' ', '').replace('/', '')}']"
                    if row.locator(aria_sel).count() > 0:
                        cell_text = row.locator(aria_sel).first.inner_text().strip()
                    
                    # Fallback to header map
                    elif field_name in header_map:
                        cell_index = header_map[field_name]
                        if cell_index < row.locator("td").count():
                            cell_text = row.locator("td").nth(cell_index).inner_text().strip()
                    
                    # Normalize the field value
                    foreclosure_data[field_name] = normalize_field_value(field_name, cell_text)
                
                results.append(ForeclosureInfo.from_dict(foreclosure_data))
                
            except Exception as e:
                logging.debug(f"[parse] Error parsing row {i}: {e}")
                continue

        return results

    except Exception as e:
        logging.debug(f"[parse] Error reading results grid: {e}")
        return []


def pick_most_recent(rows: List[ForeclosureInfo]) -> Optional[ForeclosureInfo]:
    """
    Choose the most recent record from multiple TitlePro results:
    - Prefer the row with the latest Sale Date if any row has a Sale Date
    - Otherwise prefer the row with the latest NOD date
    - If neither date exists, pick the first data row
    """
    if not rows:
        return None
    
    if len(rows) == 1:
        return rows[0]
    
    # Parse dates for comparison
    def parse_date_for_comparison(date_str: Optional[str]) -> Optional[datetime]:
        if not date_str:
            return None
        try:
            # Try YYYY-MM-DD format first
            if re.match(r'\d{4}-\d{2}-\d{2}', date_str):
                return datetime.strptime(date_str, "%Y-%m-%d")
            # Try dateutil if available
            if DATEUTIL_AVAILABLE:
                return date_parser.parse(date_str)
            # Fallback patterns
            for fmt in ["%m/%d/%Y", "%m-%d-%Y", "%Y/%m/%d"]:
                try:
                    return datetime.strptime(date_str, fmt)
                except ValueError:
                    continue
        except Exception:
            pass
        return None
    
    # Find rows with Sale Date
    sale_date_rows = []
    for row in rows:
        if row.sale_date:
            parsed_date = parse_date_for_comparison(row.sale_date)
            if parsed_date:
                sale_date_rows.append((parsed_date, row))
    
    if sale_date_rows:
        # Sort by Sale Date descending (most recent first)
        sale_date_rows.sort(key=lambda x: x[0], reverse=True)
        return sale_date_rows[0][1]
    
    # Find rows with NOD date
    nod_date_rows = []
    for row in rows:
        if row.nod:
            parsed_date = parse_date_for_comparison(row.nod)
            if parsed_date:
                nod_date_rows.append((parsed_date, row))
    
    if nod_date_rows:
        # Sort by NOD date descending (most recent first)
        nod_date_rows.sort(key=lambda x: x[0], reverse=True)
        return nod_date_rows[0][1]
    
    # Fallback: return first row
    return rows[0]


def search_foreclosure_once(page, address: str, city: str, timeout_ms: int, save_debug: bool, tag: str) -> Optional[ForeclosureInfo]:
    """
    Runs a single TitlePro247 search for foreclosure data:
      - Selects "Property Address" (value=1) in #PDVSearchType
      - Fills #Address and #CityStateZip (selects first '..., CA' match from dropdown)
      - Clicks #btnsearch
      - Parses all foreclosure fields from the results grid (#searchResultsList)
      - Returns the most recent ForeclosureInfo record
    """
    from playwright.sync_api import TimeoutError as PWTimeout

    SEL_TYPE_DROPDOWN = "#PDVSearchType"
    SEL_ADDRESS       = "#Address"
    SEL_CITY          = "#CityStateZip"
    SEL_SEARCH_BTN    = "#btnsearch"

    # Ensure search UI is ready
    try:
        page.wait_for_selector(SEL_ADDRESS, timeout=timeout_ms)
        page.wait_for_selector(SEL_CITY, timeout=timeout_ms)
    except PWTimeout:
        logging.warning(f"[search] Search UI not ready for '{address}, {city}'")
        return None

    # 1) Search type = Property Address (value="1")
    try:
        if page.locator(SEL_TYPE_DROPDOWN).count() > 0:
            page.select_option(SEL_TYPE_DROPDOWN, value="1")
    except Exception:
        pass  # default may already be correct

    # 2) Address
    try:
        page.locator(SEL_ADDRESS).fill(address or "")
        time.sleep(0.15)
    except Exception as e:
        logging.warning(f"[search] Could not fill address: {e}")
        return None

    # 3) City with autocomplete, prefer California option
    try:
        fill_city_autocomplete(page, city or "", timeout_ms)
    except Exception as e:
        logging.debug(f"[search] City autocomplete issue: {e}")

    # 4) Submit
    try:
        if page.locator(SEL_SEARCH_BTN).count() > 0:
            page.locator(SEL_SEARCH_BTN).first.click()
        else:
            # fallback (Enter key in city field)
            page.focus(SEL_CITY)
            page.keyboard.press("Enter")
        try:
            page.wait_for_load_state("networkidle", timeout=timeout_ms)
        except PWTimeout:
            pass
    except Exception as e:
        logging.error(f"[search] Failed to submit search: {e}")
        return None

    # Optional debug artifacts after submit
    if save_debug:
        try:
            os.makedirs("tp_debug", exist_ok=True)
            ts = datetime.now().strftime("%H%M%S")
            page.screenshot(path=f"tp_debug/{tag}_{ts}_after_submit.png", full_page=True)
            with open(f"tp_debug/{tag}_{ts}_after_submit.html", "w", encoding="utf-8") as f:
                f.write(page.content())
        except Exception:
            pass

    # 5) Parse the results grid for foreclosure data
    foreclosure_rows = parse_results_from_grid_foreclosure(page, timeout_ms)
    
    if not foreclosure_rows:
        return None
    
    # Pick the most recent record
    return pick_most_recent(foreclosure_rows)


# =============================================================================
# Main driver
# =============================================================================

def main():
    p = argparse.ArgumentParser(description="TitlePro247 foreclosure update tool.")
    p.add_argument("-i", "--input", required=True, help="Path to input Excel/CSV")
    p.add_argument("-o", "--output", required=True, help="Path to output Excel/CSV")
    p.add_argument("--sheet", help="Excel sheet name (default: first)")
    p.add_argument("--username", required=True)
    p.add_argument("--password", help="Password (optional; if omitted you will be prompted)")
    p.add_argument("--rate", type=float, default=1.0, help="Seconds between lookups")
    p.add_argument("--timeout", type=int, default=30, help="Per-page timeout (seconds)")
    p.add_argument("--max-retries", type=int, default=2,
                   help="Attempts per address (fixed 8s wait between attempts). Default: 2")
    p.add_argument("--cache", action="store_true")
    p.add_argument("--cache-file", default=".titlepro_cache.json")
    p.add_argument("--progress", dest="progress", action="store_true", default=True)
    p.add_argument("--no-progress", dest="progress", action="store_false")
    p.add_argument("--debug-address", help="Test a single address (uses --debug-city)")
    p.add_argument("--debug-city", help="City for --debug-address")
    p.add_argument("--save-debug", action="store_true", help="Save HTML/PNG to ./tp_debug")
    p.add_argument("--headful", action="store_true", help="Show the browser window")
    p.add_argument("--hold-after-login", action="store_true",
                   help="With --debug-login and --headful, keep browser open until Enter is pressed")
    p.add_argument("-v", "--verbose", action="store_true")
    p.add_argument("--debug-login", action="store_true", help="Only test TitlePro login (no search or spreadsheet)")

    args = p.parse_args()

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )

    password = args.password or getpass.getpass(f"Password for {args.username}: ")

    # Helper to create a hardened browser/page each time we need one
    def _make_page(pwt):
        ua = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        browser = pwt.chromium.launch(
            headless=not args.headful,
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

    # --- Login-only debug mode ---
    if args.debug_login:
        sp = ensure_playwright()
        with sp() as pwt:
            browser, ctx, page = _make_page(pwt)
            ok = login_titlepro(page, args.username, password, timeout_ms=args.timeout * 1000, verbose=args.verbose)
            print(f"Login check: {'OK' if ok else 'FAILED'}")

            if args.headful and args.hold_after_login:
                try:
                    logging.info("Login check complete — browser will stay open until you press Enter in the terminal.")
                    input("Press Enter to close the browser and exit...")
                except Exception:
                    pass

            browser.close()
        return

    # ---- Normal program flow ----
    df = read_table(args.input, sheet=args.sheet)
    addr_col, city_col = detect_columns(df)
    if not addr_col or not city_col:
        logging.error("Required columns not found: Address and City.")
        sys.exit(2)

    # Ensure required foreclosure columns exist
    foreclosure_columns = ["Loan Amount", "NOD", "Sale Date", "Back to Beneficiary On", "Bene/Client Name"]
    for col in foreclosure_columns:
        if col not in df.columns:
            df[col] = ""
    
    # Ensure metadata columns exist
    if "Last Checked" not in df.columns:
        df["Last Checked"] = ""
    if "Changed Fields" not in df.columns:
        df["Changed Fields"] = ""

    # Optional: single-address debug mode
    if args.debug_address:
        sp = ensure_playwright()
        with sp() as pwt:
            browser, ctx, page = _make_page(pwt)
            ok = login_titlepro(page, args.username, password, timeout_ms=args.timeout * 1000, verbose=args.verbose)
            logging.info(f"Login check: {'OK' if ok else 'FAILED'}")
            if ok:
                foreclosure_info = search_foreclosure_once(
                    page, args.debug_address, args.debug_city or "",
                    timeout_ms=args.timeout * 1000, save_debug=args.save_debug,
                    tag=re.sub(r"[^A-Za-z0-9_-]+", "_", args.debug_address)[:50]
                )
                if foreclosure_info:
                    result_dict = foreclosure_info.to_dict()
                    print(json.dumps(result_dict, indent=2))
                else:
                    print(json.dumps({col: None for col in foreclosure_columns}, indent=2))
            browser.close()
        return

    # Build worklist: all rows with non-empty Address and City
    work: List[Tuple[int, str, str]] = []
    for idx, row in df.iterrows():
        raw_addr = row.get(addr_col, "")
        raw_city = row.get(city_col, "")
        addr = "" if pd.isna(raw_addr) else str(raw_addr).strip()
        city = "" if pd.isna(raw_city) else str(raw_city).strip()
        if addr and city:
            work.append((idx, addr, city))

    total_needed = len(work)
    if total_needed == 0:
        logging.info("No rows with Address and City to process.")
        write_table(df, args.output)
        return

    cache = ForeclosureCache(args.cache_file, args.cache)

    # Session health check
    def _session_alive(pg) -> bool:
        try:
            return (pg.locator("#Address").count() > 0 and pg.locator("#CityStateZip").count() > 0)
        except Exception:
            return False

    sp = ensure_playwright()
    updated = 0
    failed = 0

    with sp() as pwt:
        browser, ctx, page = _make_page(pwt)

        if not login_titlepro(page, args.username, password, timeout_ms=args.timeout * 1000, verbose=args.verbose):
            logging.error("Login failed; cannot proceed.")
            browser.close()
            sys.exit(3)

        iterator = work
        bar = None
        if args.progress and TQDM_AVAILABLE and sys.stdout.isatty() and total_needed > 1:
            bar = tqdm(work, desc="TitlePro247 foreclosure lookups", unit="row")
            bar.set_postfix_str(f"{updated}/{total_needed} updated")

        for item in iterator if bar is None else bar:
            idx, addr, city = item if bar is None else item

            # Re-auth if needed
            if not _session_alive(page):
                logging.warning("[session] Search UI missing — attempting re-login.")
                if not login_titlepro(page, args.username, password, timeout_ms=args.timeout * 1000, verbose=args.verbose):
                    logging.error("[session] Re-login failed; stopping.")
                    break

            # Cache
            cached = cache.get(addr, city)
            foreclosure_info = cached

            if not foreclosure_info:
                # Exactly max_retries attempts, fixed 8s wait between
                attempts = max(1, int(args.max_retries))
                for attempt in range(attempts):
                    foreclosure_info = search_foreclosure_once(
                        page, addr, city, timeout_ms=args.timeout * 1000,
                        save_debug=args.save_debug, tag=re.sub(r"[^A-Za-z0-9_-]+", "_", addr)[:50]
                    )
                    if foreclosure_info:
                        break
                    if attempt < attempts - 1:
                        time.sleep(8)
                cache.put(addr, city, foreclosure_info)

            # Update logic: only if changed
            if foreclosure_info:
                # Get existing values
                existing_dict = {}
                for col in foreclosure_columns:
                    val = df.at[idx, col]
                    existing_dict[col] = "" if pd.isna(val) else str(val).strip()
                
                # Get new values
                new_dict = foreclosure_info.to_dict()
                
                # Compare and update only changed fields
                changed, changed_fields = values_changed(existing_dict, new_dict)
                
                if changed:
                    for col in changed_fields:
                        new_val = new_dict[col]
                        # Only overwrite if new value is not None/empty, or if existing is empty
                        if new_val or not existing_dict[col]:
                            df.at[idx, col] = new_val
                    
                    df.at[idx, "Changed Fields"] = ", ".join(changed_fields)
                    updated += 1
                else:
                    df.at[idx, "Changed Fields"] = ""
                
                # Always update Last Checked timestamp
                df.at[idx, "Last Checked"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            else:
                failed += 1

            if bar is not None:
                bar.set_postfix_str(f"{updated}/{total_needed} updated")
                bar.update(1)

            time.sleep(args.rate)

        if bar is not None:
            bar.close()

        browser.close()

    cache.save()
    write_table(df, args.output)
    success_pct = (updated / total_needed) * 100.0 if total_needed else 0.0
    logging.info(f"Done. Updated: {updated}/{total_needed} ({success_pct:.1f}%), Failed: {failed}")


if __name__ == "__main__":
    main()
