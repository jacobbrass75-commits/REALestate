#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
TitlePro247 APN & Use Filler (Standalone)
-----------------------------------------
- Reads Excel/CSV with columns: Address, City, APN (Use is added if missing)
- Logs into https://www.titlepro247.com
- Verifies login by ensuring the post-login search UI is present
- For each row with blank APN, searches by Address + City (CA)
- Scrapes APN and Use from first result, writes to output

Quick start:
  pip install playwright pandas tqdm
  python -m playwright install chromium

Run (headless):
  python titlepro_apn_use_fill.py -i INPUT.xlsx -o OUTPUT.xlsx --username "USER"

Show the browser (debug):
  python titlepro_apn_use_fill.py -i INPUT.xlsx -o OUTPUT.xlsx --username "USER" --headful --save-debug -v
"""

import argparse
import getpass
import json
import logging
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple, List

import pandas as pd

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False


# =============================================================================
# Selectors / Config (tuned to your screenshots; adjust here if the site changes)
# =============================================================================

TITLEPRO_BASE_URL = "https://www.titlepro247.com"
LOGIN_URL = TITLEPRO_BASE_URL  # reliably lands on the login form

# --- Login form (from your login screenshot) ---
SEL_LOGIN_FORM   = "form[name='login']"
SEL_USER         = "input[name='username']"
SEL_PASS         = "input[name='password']"
SEL_SUBMIT       = "input[type='submit'][value='LOGIN']"

# --- Post-login search UI (from your Index.aspx screenshot banner) ---
# Address input has placeholder "Enter House #, Street Name, Unit #"
# There are adjacent selects (search type; county/city), and a SEARCH button.
SEL_ADDR_INPUT_CANDIDATES = [
    "input[placeholder*='Street'][type='text']",
    "input[aria-label*='Street']",
    "input#txtAddress",
]
# County/City dropdown candidates (TitlePro shows county dropdown; we force CA)
SEL_CITY_OR_COUNTY_SELECTS = [
    "select[name*='county']",
    "select[name*='city']",
    "select#ddlCounty",
    "select#ddlCity",
    "div select",  # generic fallback inside banner
]
# Optional state select (force CA if present)
SEL_STATE_SELECTS = [
    "select[name*='state']",
    "select#ddlState",
]
# The Search button (text or input submit)
SEL_SEARCH_BUTTONS = [
    "button:has-text('SEARCH')",
    "input[type='submit'][value*='SEARCH']",
    "button[aria-label*='Search']",
]

# --- Results parsing ---
# Prefer a tabular grid; fallback to scanning visible text. We look for header cells containing APN/Assessor etc.
SEL_RESULTS_TABLE_CANDIDATES = [
    "table#results",
    "table.results",
    "table.data-grid",
    "table:has(th:has-text('APN'))",
    "table:has(th:has-text('Assessor'))",
    "table:has(th:has-text('Parcel'))",
]
SEL_FIRST_RESULT_ROW = "tbody tr"
SEL_CELL_APN_CANDIDATES = [
    "td.apn",
    "td:has-text('APN')",  # sometimes detail layouts include labels; we handle both cell and detail modes
]
SEL_CELL_USE_CANDIDATES = [
    "td.use",
    "td:has-text('Use')",
    "td:has-text('TYPE CODE')",  # your other site uses Type Code; include in fallback
    "td:has-text('Property Use')",
]

# APN patterns
APN_DASHED_RE = re.compile(r'\b\d{2,4}-\d{3,5}-\d{2,3}\b')
APN_ALNUM_RE  = re.compile(r'\b(?=[A-Z0-9-]{6,20}\b)(?=.*\d)[A-Z0-9-]+\b', re.I)


# =============================================================================
# IO helpers
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

def detect_columns(df: pd.DataFrame) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    cols = [str(c).strip() for c in df.columns]
    def find_any(patterns: List[str]) -> Optional[str]:
        for pat in patterns:
            for c in cols:
                if re.fullmatch(pat, c, flags=re.I):
                    return c
        return None
    addr = find_any([r"address", r".*street.*address.*", r".*property.*address.*"])
    city = find_any([r"city", r".*mailing.*city.*", r".*property.*city.*"])
    apn  = find_any([r"apn", r".*parcel.*number.*", r".*parcel.*id.*"])
    return addr, city, apn


# =============================================================================
# Cache
# =============================================================================

class SimpleCache:
    def __init__(self, path: str, enabled: bool):
        self.path = path
        self.enabled = enabled
        self.data: Dict[str, Dict[str, Optional[str]]] = {}
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

    def get(self, addr: str, city: str) -> Optional[Tuple[Optional[str], Optional[str]]]:
        if not self.enabled: return None
        rec = self.data.get(self._key(addr, city))
        if not rec: return None
        return rec.get("apn"), rec.get("use")

    def put(self, addr: str, city: str, apn: Optional[str], use: Optional[str]) -> None:
        if not self.enabled: return
        self.data[self._key(addr, city)] = {"apn": apn, "use": use}

    def save(self):
        if not self.enabled: return
        try:
            json.dump(self.data, open(self.path, "w"), indent=2)
        except Exception as e:
            logging.warning(f"Cache save failed: {e}")


# =============================================================================
# Playwright automation
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
    Robust TitlePro247 login:
    - Handles splash/Login/Sign In launchers
    - Dismisses cookie banners
    - Fills username/password using broad selectors
    - Submits, then ensures the SEARCH UI (PDVSearchType, Address, CityStateZip) is present
    Returns True iff the search inputs are detected on the resulting page.
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




def select_city_and_state(page, city: str, timeout_ms: int):
    # Force CA if any state select exists
    for sel in SEL_STATE_SELECTS:
        try:
            if page.locator(sel).count() > 0:
                page.select_option(sel, value="CA")
                break
        except Exception:
            continue

    # Pick city/county: try exact, then prefix/contains
    opts = []
    chosen_sel = None
    for sel in SEL_CITY_OR_COUNTY_SELECTS:
        try:
            if page.locator(sel).count() > 0:
                chosen_sel = sel
                opts = page.eval_on_selector_all(
                    sel + " option",
                    "els => els.map(e => ({value:e.value, text:(e.textContent||'').trim()}))"
                )
                break
        except Exception:
            continue

    if not chosen_sel:
        # Many UIs use combobox widgets instead of native <select>; try to type city
        for sel in SEL_CITY_OR_COUNTY_SELECTS:
            try:
                page.fill(sel, city)
                return
            except Exception:
                continue
        return

    target = (city or "").strip().upper()
    chosen = None
    for o in opts:
        if o["text"].strip().upper() == target:
            chosen = o["value"]; break
    if not chosen:
        for o in opts:
            if o["text"].strip().upper().startswith(target):
                chosen = o["value"]; break
    if not chosen:
        for o in opts:
            if target in o["text"].strip().upper():
                chosen = o["value"]; break
    if chosen:
        try:
            page.select_option(chosen_sel, value=chosen)
        except Exception:
            try:
                page.select_option(chosen_sel, label=city)
            except Exception:
                pass
    else:
        # fallback: type the text if supported
        try:
            page.fill(chosen_sel, city)
        except Exception:
            pass

def extract_apn(text: str) -> Optional[str]:
    t = (text or "").strip()
    m = APN_DASHED_RE.search(t)
    if m:
        return m.group(0)
    m2 = APN_ALNUM_RE.search(t)
    if m2:
        return m2.group(0)
    return None

def clean_use(text: str) -> Optional[str]:
    t = re.sub(r"\s+", " ", (text or "")).strip()
    return t or None

def parse_from_table(page, timeout_ms: int) -> Tuple[Optional[str], Optional[str]]:
    """Look for a results table with APN/Use columns and take the first row."""
    for tbl_sel in SEL_RESULTS_TABLE_CANDIDATES:
        try:
            page.wait_for_selector(tbl_sel, timeout=timeout_ms)
        except Exception:
            continue
        try:
            row = page.query_selector(f"{tbl_sel} {SEL_FIRST_RESULT_ROW}")
            if not row:
                continue
            # APN cell
            apn_txt = ""
            for cell_sel in SEL_CELL_APN_CANDIDATES:
                try:
                    cell = row.query_selector(cell_sel)
                    if cell:
                        apn_txt = cell.inner_text()
                        if apn_txt:
                            break
                except Exception:
                    continue
            # Use cell
            use_txt = ""
            for cell_sel in SEL_CELL_USE_CANDIDATES:
                try:
                    cell = row.query_selector(cell_sel)
                    if cell:
                        use_txt = cell.inner_text()
                        if use_txt:
                            break
                except Exception:
                    continue
            return extract_apn(apn_txt), clean_use(use_txt)
        except Exception:
            continue
    return None, None

def parse_fallback_scan(page) -> Tuple[Optional[str], Optional[str]]:
    """If no table found, scan visible text for a likely APN and lightweight 'Use' clues."""
    content = page.content()
    apn = extract_apn(content)
    use = None
    # very light heuristic for Use label
    m = re.search(r"(Use|Type|Property Use)\s*[:\-]\s*([A-Za-z /]+)", content, re.I)
    if m:
        use = clean_use(m.group(2))
    return apn, use


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


def parse_apn_from_grid(page, timeout_ms: int = 30000) -> Optional[str]:
    """
    Extracts APN from the first data row in the results grid:
      <table id="searchResultsList">
        <td aria-describedby="searchResultsList_APN"><span>2355-005-037</span></td>
    Returns apn or None.
    """
    from playwright.sync_api import TimeoutError as PWTimeout

    TABLE_SEL = "#searchResultsList"
    ROWS_SEL  = "#searchResultsList tbody tr[role='row']"
    APN_CELL  = "td[aria-describedby='searchResultsList_APN']"

    try:
        page.wait_for_selector(TABLE_SEL, timeout=timeout_ms)
    except PWTimeout:
        logging.warning("[parse] Results grid not found (timeout).")
        return None

    try:
        rows = page.locator(ROWS_SEL)
        count = rows.count()
        if count == 0:
            return None

        # Prefer a row with an id (jqGrid assigns '1','2',...) else first visible data row
        row_idx = None
        for i in range(count):
            rid = (rows.nth(i).get_attribute("id") or "").strip()
            if rid:
                row_idx = i
                break
        if row_idx is None:
            row_idx = 0

        row = rows.nth(row_idx)

        if row.locator(APN_CELL).count() == 0:
            return None

        apn_text = row.locator(APN_CELL).first.inner_text().strip()

        # Normalize: prefer dashed form
        m = re.search(r"\b\d{2,4}-\d{3,5}-\d{2,3}\b", apn_text)
        if m:
            return m.group(0)
        m2 = re.search(r"(?=[A-Z0-9-]{6,20}\b)(?=.*\d)[A-Z0-9-]+", apn_text, flags=re.I)
        return m2.group(0) if m2 else (apn_text or None)

    except Exception as e:
        logging.debug(f"[parse] Error reading results grid: {e}")
        return None



def parse_results_from_grid(page, timeout_ms: int = 30000) -> Tuple[Optional[str], Optional[str]]:
    """
    Extracts APN and Use from the first data row in the results grid:
      <table id="searchResultsList"> ... <td aria-describedby="searchResultsList_APN"> ... </td>
                                      ... <td aria-describedby="searchResultsList_Use"> ... </td>
    Returns (apn, use) or (None, None).
    """
    from playwright.sync_api import TimeoutError as PWTimeout

    TABLE_SEL = "#searchResultsList"
    ROWS_SEL  = "#searchResultsList tbody tr[role='row']"
    APN_CELL  = "td[aria-describedby='searchResultsList_APN']"
    USE_CELL  = "td[aria-describedby='searchResultsList_Use']"

    try:
        page.wait_for_selector(TABLE_SEL, timeout=timeout_ms)
    except PWTimeout:
        logging.warning("[parse] Results grid not found (timeout).")
        return None, None

    # Grab the first *data* row (skip header/sizer rows)
    try:
        rows = page.locator(ROWS_SEL)
        count = rows.count()
        if count == 0:
            return None, None

        # Prefer a row with an id (e.g., id="1"), else first visible row after header
        row_idx = None
        for i in range(count):
            r = rows.nth(i)
            try:
                rid = r.get_attribute("id") or ""
            except Exception:
                rid = ""
            # jqGrid usually gives data rows an id (1,2,...)
            if rid.strip():
                row_idx = i
                break
        if row_idx is None:
            row_idx = 0

        row = rows.nth(row_idx)

        # Cells
        apn_text = None
        use_text = None

        if row.locator(APN_CELL).count() > 0:
            apn_text = row.locator(APN_CELL).first.inner_text().strip()
        if row.locator(USE_CELL).count() > 0:
            use_text = row.locator(USE_CELL).first.inner_text().strip()

        # Normalize APN (keep dashed form if present)
        if apn_text:
            m = re.search(r"\b\d{2,4}-\d{3,5}-\d{2,3}\b", apn_text)
            if m:
                apn_text = m.group(0)
            else:
                # fallback: scrub and keep alnum/dash
                m2 = re.search(r"(?=[A-Z0-9-]{6,20}\b)(?=.*\d)[A-Z0-9-]+", apn_text, flags=re.I)
                apn_text = m2.group(0) if m2 else apn_text.strip()

        return (apn_text or None, use_text or None)

    except Exception as e:
        logging.debug(f"[parse] Error reading results grid: {e}")
        return None, None


def search_once(page, address: str, city: str, timeout_ms: int, save_debug: bool, tag: str) -> Optional[str]:
    """
    Runs a single TitlePro247 search for APN only:
      - Selects "Property Address" (value=1) in #PDVSearchType
      - Fills #Address and #CityStateZip (selects first '..., CA' match from dropdown)
      - Clicks #btnsearch
      - Parses APN from the results grid (#searchResultsList)
    Returns apn or None.
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

    # 5) Parse the results grid (APN only)
    apn = parse_apn_from_grid(page, timeout_ms)

    return apn







# =============================================================================
# Driver
# =============================================================================

def main():
    p = argparse.ArgumentParser(description="TitlePro247 APN filler (APN only).")
    p.add_argument("-i", "--input", required=True, help="Path to input Excel/CSV")
    p.add_argument("-o", "--output", required=True, help="Path to output Excel/CSV")
    p.add_argument("--sheet", help="Excel sheet name (default: first)")
    p.add_argument("--username", required=True)
    p.add_argument("--password", help="Password (optional; if omitted you will be prompted)")
    p.add_argument("--rate", type=float, default=1.0, help="Seconds between lookups")
    p.add_argument("--timeout", type=int, default=30, help="Per-page timeout (seconds)")
    p.add_argument("--max-retries", type=int, default=2,  # exactly 2 attempts by default
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
    addr_col, city_col, apn_col = detect_columns(df)
    if not addr_col or not city_col:
        logging.error("Required columns not found: Address and City.")
        sys.exit(2)
    if apn_col is None:
        apn_col = "APN"
        df[apn_col] = ""
    if "Time APN Added" not in df.columns:
        df["Time APN Added"] = ""

    # Optional: single-address debug mode
    if args.debug_address:
        sp = ensure_playwright()
        with sp() as pwt:
            browser, ctx, page = _make_page(pwt)
            ok = login_titlepro(page, args.username, password, timeout_ms=args.timeout * 1000, verbose=args.verbose)
            logging.info(f"Login check: {'OK' if ok else 'FAILED'}")
            if ok:
                apn = search_once(
                    page, args.debug_address, args.debug_city or "",
                    timeout_ms=args.timeout * 1000, save_debug=args.save_debug,
                    tag=re.sub(r"[^A-Za-z0-9_-]+", "_", args.debug_address)[:50]
                )
                logging.info(f"Result -> APN: {apn}")
            browser.close()
        return

    # Build worklist: only rows with blank APN
    work: List[Tuple[int, str, str]] = []
    for idx, row in df.iterrows():
        raw_addr = row.get(addr_col, "")
        raw_city = row.get(city_col, "")
        raw_apn  = row.get(apn_col, "")
        addr = "" if pd.isna(raw_addr) else str(raw_addr).strip()
        city = "" if pd.isna(raw_city) else str(raw_city).strip()
        apn  = "" if pd.isna(raw_apn)  else str(raw_apn).strip()
        if addr and city and apn == "":
            work.append((idx, addr, city))

    total_needed = len(work)
    if total_needed == 0:
        logging.info("No blank APNs to fill.")
        write_table(df, args.output)
        return

    cache = SimpleCache(args.cache_file, args.cache)

    # Session health check
    def _session_alive(pg) -> bool:
        try:
            return (pg.locator("#Address").count() > 0 and pg.locator("#CityStateZip").count() > 0)
        except Exception:
            return False

    sp = ensure_playwright()
    filled = 0
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
            bar = tqdm(work, desc="TitlePro247 lookups", unit="row")
            bar.set_postfix_str(f"{filled}/{total_needed} filled")

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
            if isinstance(cached, (list, tuple)):
                apn_val = cached[0]
            else:
                apn_val = cached

            if not apn_val:
                # Exactly 2 attempts, fixed 8s wait between
                attempts = max(1, int(args.max_retries))
                for attempt in range(attempts):
                    apn_val = search_once(
                        page, addr, city, timeout_ms=args.timeout * 1000,
                        save_debug=args.save_debug, tag=re.sub(r"[^A-Za-z0-9_-]+", "_", addr)[:50]
                    )
                    if apn_val:
                        break
                    if attempt < attempts - 1:
                        time.sleep(8)
                cache.put(addr, city, apn_val, None)  # keep signature compatible

            # Write back: APN + timestamp (only when APN was previously blank)
            if apn_val:
                current_apn = "" if pd.isna(df.at[idx, apn_col]) else str(df.at[idx, apn_col]).strip()
                if not current_apn:
                    df.at[idx, apn_col] = apn_val
                    df.at[idx, "Time APN Added"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    filled += 1
            else:
                failed += 1

            if bar is not None:
                bar.set_postfix_str(f"{filled}/{total_needed} filled")
                bar.update(1)

            time.sleep(args.rate)

        if bar is not None:
            bar.close()

        browser.close()

    cache.save()
    write_table(df, args.output)
    success_pct = (filled / total_needed) * 100.0 if total_needed else 0.0
    logging.info(f"Done. Success: {filled}/{total_needed} ({success_pct:.1f}%), Failed: {failed}")



if __name__ == "__main__":
    main()
