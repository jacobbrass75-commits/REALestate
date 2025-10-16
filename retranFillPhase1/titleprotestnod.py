#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
TitlePro247 Foreclosure Fields Updater (Standalone)
---------------------------------------------------
- Reads Excel/CSV with columns: Address, City (APN not required)
- Logs into https://www.titlepro247.com
- For *every* row with Address+City, searches by Property Address (CA)
- Scrapes 5 fields from the results grid (most recent record):
    Loan Amount, NOD, Sale Date, Back to Beneficiary On, Bene/Client Name
- Updates those columns only when values changed (creates columns if missing)
- Stamps "Last Checked" and optional "Changed Fields" audit

Quick start:
  pip install playwright pandas tqdm python-dateutil
  python -m playwright install chromium

Run (headless):
  python titlepro_foreclosure_update.py -i INPUT.xlsx -o OUTPUT.xlsx --username "USER"

Show the browser (debug):
  python titlepro_foreclosure_update.py -i INPUT.xlsx -o OUTPUT.xlsx --username "USER" --headful --save-debug -v
"""

import argparse
import getpass
import json
import logging
import os
import re
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple, List

import pandas as pd

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False

try:
    from dateutil import parser as dateparser
    DATEUTIL = True
except Exception:
    DATEUTIL = False


# =============================================================================
# Selectors / Config (aligned with your baseline)
# =============================================================================

TITLEPRO_BASE_URL = "https://www.titlepro247.com"

# Concrete search UI seen post-login
SEL_SEARCH_TYPE  = "#PDVSearchType"
SEL_ADDR_INPUT   = "#Address"
SEL_CITY_INPUT   = "#CityStateZip"
SEL_SEARCH_BTN   = "#btnsearch"

# jqGrid results
SEL_RESULTS_TABLE  = "#searchResultsList"
SEL_HEAD_CELLS     = "#searchResultsList thead th"
SEL_DATA_ROWS      = "#searchResultsList tbody tr[role='row']"


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

def detect_address_city(df: pd.DataFrame) -> Tuple[Optional[str], Optional[str]]:
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
# Cache (optional, keyed by Address||City -> five-field dict)
# =============================================================================

class ForeclosureCache:
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

    def get(self, addr: str, city: str) -> Optional[Dict[str, Optional[str]]]:
        if not self.enabled: return None
        return self.data.get(self._key(addr, city))

    def put(self, addr: str, city: str, info: Dict[str, Optional[str]]) -> None:
        if not self.enabled: return
        self.data[self._key(addr, city)] = info

    def save(self):
        if not self.enabled: return
        try:
            json.dump(self.data, open(self.path, "w"), indent=2)
        except Exception as e:
            logging.warning(f"Cache save failed: {e}")


# =============================================================================
# Playwright automation — reuse your robust login + helpers (copied inline)
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

def login_titlepro(page, username: str, password: str, timeout_ms: int = 30000, verbose: bool = True) -> bool:
    """Copied from your baseline with a gentle nudge to /Index.aspx after submit."""
    from playwright.sync_api import TimeoutError as PWTimeout
    BASE = TITLEPRO_BASE_URL

    SEL_FORM         = "form"
    SEL_USERNAME     = "input#username, input#UserName, input[name*='user' i], input[name*='email' i]"
    SEL_PASSWORD     = "input#Password, input[type='password'], input[name*='pass' i]"
    SEL_SUBMIT       = "button[type='submit'], input[type='submit'], button:has-text('Sign In'), button:has-text('Login')"
    SEL_LOGIN_ENTRY  = "a:has-text('Login'), a:has-text('Sign In'), button:has-text('Login'), button:has-text('Sign In')"

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
    page.goto(BASE, timeout=timeout_ms, wait_until="domcontentloaded")
    _dismiss_cookies()

    try:
        if page.locator(SEL_FORM).count() == 0:
            if page.locator(SEL_LOGIN_ENTRY).count() > 0:
                page.locator(SEL_LOGIN_ENTRY).first.click()
                try:
                    page.wait_for_load_state("domcontentloaded", timeout=8000)
                except PWTimeout:
                    pass
    except Exception:
        pass

    # find form on page or iframes
    def _find_form_on_page():
        try:
            if page.locator(SEL_FORM).count() > 0:
                f = page.locator(SEL_FORM).first
                if f.is_visible():
                    return f
        except Exception:
            pass
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
        logging.error("[login] Could not find a login form.")
        return False

    try:
        form.locator(SEL_USERNAME).first.fill(username, timeout=5000)
        form.locator(SEL_PASSWORD).first.fill(password, timeout=5000)
        logging.info(f"[login] Filled credentials for '{username}'")
    except Exception as e:
        logging.error(f"[login] Failed to fill credentials: {e}")
        return False

    try:
        if form.locator(SEL_SUBMIT).count() > 0:
            form.locator(SEL_SUBMIT).first.click(timeout=5000)
        else:
            form.press(SEL_PASSWORD, "Enter")
        logging.info("[login] Submitted login form")
    except Exception:
        try:
            form.press(SEL_PASSWORD, "Enter")
        except Exception:
            pass

    try:
        page.wait_for_load_state("networkidle", timeout=timeout_ms)
    except PWTimeout:
        pass

    # gentle nudge to search surface
    try:
        page.goto(f"{BASE}/Index.aspx", timeout=timeout_ms, wait_until="domcontentloaded")
    except Exception:
        pass

    # final presence check
    try:
        page.wait_for_selector(SEL_ADDR_INPUT, timeout=6000)
        page.wait_for_selector(SEL_CITY_INPUT, timeout=6000)
        return True
    except Exception:
        logging.error("[login] Login flow completed, but search UI not detected.")
        return False


def fill_city_autocomplete(page, city: str, timeout_ms: int) -> None:
    from playwright.sync_api import TimeoutError as PWTimeout
    city = (city or "").strip()
    if not city:
        return
    page.fill(SEL_CITY_INPUT, city)
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
        return
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
            chosen_idx = i; break
    if chosen_idx is None:
        for i, o in enumerate(options):
            t = (o.get('text') or '').upper()
            if ' CA' in t or t.endswith(',CA') or t.endswith(' CA'):
                chosen_idx = i; break
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
# Foreclosure parsing helpers
# =============================================================================

@dataclass
class ForeclosureInfo:
    loan_amount: Optional[str] = None
    nod: Optional[str] = None
    sale_date: Optional[str] = None
    back_to_bene_on: Optional[str] = None
    bene_name: Optional[str] = None

HEADER_MAP = {
    "Loan Amount": ["loan amount", "loan", "amount", "loanamt"],
    "NOD": ["nod", "notice of default", "default"],
    "Sale Date": ["sale date", "sale", "auction date"],
    "Back to Beneficiary On": ["back to beneficiary on", "back to bene", "back to bene on", "beneficiary on"],
    "Bene/Client Name": ["bene/client name", "beneficiary", "client", "bene name", "beneficiary name"],
}

def _slug(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())

def normalize_currency(s: Optional[str]) -> Optional[str]:
    if not s: return None
    # keep digits and dot
    t = re.sub(r"[^0-9.]", "", s)
    if not t: return None
    try:
        v = float(t)
        if v.is_integer():
            return f"${int(v):,}"
        return f"${v:,.2f}"
    except Exception:
        return s.strip() or None

def normalize_date(s: Optional[str]) -> Optional[str]:
    if not s: return None
    s = s.strip()
    if not s: return None
    if DATEUTIL:
        try:
            d = dateparser.parse(s, fuzzy=True)
            return d.strftime("%Y-%m-%d")
        except Exception:
            pass
    m = re.search(r"(\d{1,2})/(\d{1,2})/(\d{2,4})", s)
    if m:
        mm, dd, yy = map(int, m.groups())
        if yy < 100: yy += 2000
        try:
            return datetime(yy, mm, dd).strftime("%Y-%m-%d")
        except Exception:
            return None
    # fallback: ISO-like already
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return s
    return None

def _canon_field(header_text: str) -> Optional[str]:
    h = _slug(header_text)
    if not h: return None
    for canon, alts in HEADER_MAP.items():
        for a in alts:
            if _slug(a) in h:
                return canon
    return None

def parse_results_from_grid_foreclosure(page, timeout_ms: int = 30000) -> List[ForeclosureInfo]:
    """Read all jqGrid rows; map headers to our five canonical fields."""
    try:
        page.wait_for_selector(SEL_RESULTS_TABLE, timeout=timeout_ms)
    except Exception:
        return []

    # Build header index -> canonical field map
    header_map: Dict[int, str] = {}
    try:
        heads = page.locator(SEL_HEAD_CELLS)
        for i in range(heads.count()):
            txt = (heads.nth(i).inner_text() or "").strip()
            canon = _canon_field(txt)
            if canon:
                header_map[i] = canon
    except Exception:
        pass

    rows = page.locator(SEL_DATA_ROWS)
    out: List[ForeclosureInfo] = []
    for r_i in range(rows.count()):
        r = rows.nth(r_i)
        info = ForeclosureInfo()
        try:
            tds = r.locator("td")
            td_count = tds.count()
            for i in range(td_count):
                canon = header_map.get(i)
                if not canon:
                    continue
                raw = (tds.nth(i).inner_text() or "").strip()
                if not raw:
                    continue
                if canon == "Loan Amount":
                    info.loan_amount = normalize_currency(raw) or raw
                elif canon == "NOD":
                    info.nod = normalize_date(raw) or raw
                elif canon == "Sale Date":
                    info.sale_date = normalize_date(raw) or raw
                elif canon == "Back to Beneficiary On":
                    info.back_to_bene_on = normalize_date(raw) or raw
                elif canon == "Bene/Client Name":
                    info.bene_name = raw
        except Exception:
            continue
        if any([info.loan_amount, info.nod, info.sale_date, info.back_to_bene_on, info.bene_name]):
            out.append(info)
    return out

def _to_dt_or_min(s: Optional[str]) -> datetime:
    n = normalize_date(s or "")
    if not n: return datetime.min
    try:
        return datetime.strptime(n, "%Y-%m-%d")
    except Exception:
        return datetime.min

def pick_most_recent(rows: List[ForeclosureInfo]) -> Optional[ForeclosureInfo]:
    if not rows: return None
    # sort by Sale Date, then NOD (desc)
    rows_sorted = sorted(rows, key=lambda r: (_to_dt_or_min(r.sale_date), _to_dt_or_min(r.nod)), reverse=True)
    return rows_sorted[0]

def search_foreclosure_once(page, address: str, city: str, timeout_ms: int, save_debug: bool, tag: str) -> Optional[ForeclosureInfo]:
    """Submit Property Address search and return best ForeclosureInfo for Address+City."""
    from playwright.sync_api import TimeoutError as PWTimeout

    # Ensure search UI is ready
    try:
        page.wait_for_selector(SEL_ADDR_INPUT, timeout=timeout_ms)
        page.wait_for_selector(SEL_CITY_INPUT, timeout=timeout_ms)
    except PWTimeout:
        logging.warning(f"[search] Search UI not ready for '{address}, {city}'")
        return None

    # Select "Property Address" if the dropdown exists
    try:
        if page.locator(SEL_SEARCH_TYPE).count() > 0:
            page.select_option(SEL_SEARCH_TYPE, value="1")
    except Exception:
        pass

    # Fill address & city
    try:
        page.locator(SEL_ADDR_INPUT).fill(address or "")
        time.sleep(0.15)
    except Exception as e:
        logging.warning(f"[search] Could not fill address: {e}")
        return None

    try:
        fill_city_autocomplete(page, city or "", timeout_ms)
    except Exception:
        pass

    # Submit
    try:
        if page.locator(SEL_SEARCH_BTN).count() > 0:
            page.locator(SEL_SEARCH_BTN).first.click()
        else:
            page.focus(SEL_CITY_INPUT)
            page.keyboard.press("Enter")
        try:
            page.wait_for_load_state("networkidle", timeout=timeout_ms)
        except PWTimeout:
            pass
    except Exception as e:
        logging.error(f"[search] Failed to submit search: {e}")
        return None

    # Save debug artifacts
    if save_debug:
        try:
            os.makedirs("tp_debug", exist_ok=True)
            ts = datetime.now().strftime("%H%M%S")
            page.screenshot(path=f"tp_debug/{tag}_{ts}_after_submit.png", full_page=True)
            with open(f"tp_debug/{tag}_{ts}_after_submit.html", "w", encoding="utf-8") as f:
                f.write(page.content())
        except Exception:
            pass

    rows = parse_results_from_grid_foreclosure(page, timeout_ms)
    best = pick_most_recent(rows)
    return best


# =============================================================================
# Driver
# =============================================================================

def main():
    p = argparse.ArgumentParser(description="TitlePro247 foreclosure fields updater.")
    # Make -i/-o optional for debug-only modes
    p.add_argument("-i", "--input", help="Path to input Excel/CSV")
    p.add_argument("-o", "--output", help="Path to output Excel/CSV")
    p.add_argument("--sheet", help="Excel sheet name (default: first)")
    p.add_argument("--username", required=True)
    p.add_argument("--password", help="Password (optional; if omitted you will be prompted)")
    p.add_argument("--rate", type=float, default=1.0, help="Seconds between lookups")
    p.add_argument("--timeout", type=int, default=30, help="Per-page timeout (seconds)")
    p.add_argument("--max-retries", type=int, default=2, help="Attempts per address (fixed 8s wait between). Default: 2")
    p.add_argument("--cache", action="store_true")
    p.add_argument("--cache-file", default=".titlepro_cache.json")
    p.add_argument("--progress", dest="progress", action="store_true", default=True)
    p.add_argument("--no-progress", dest="progress", action="store_false")
    p.add_argument("--debug-address", help="Test a single address (uses --debug-city)")
    p.add_argument("--debug-city", help="City for --debug-address")
    p.add_argument("--save-debug", action="store_true", help="Save HTML/PNG to ./tp_debug")
    p.add_argument("--headful", action="store_true", help="Show the browser window")
    p.add_argument("--hold-after-login", action="store_true", help="Keep browser open after debug-login until Enter is pressed")
    p.add_argument("-v", "--verbose", action="store_true")
    p.add_argument("--debug-login", action="store_true", help="Only test TitlePro login (no search or spreadsheet)")

    args = p.parse_args()

    # Enforce input/output only when not in debug-only modes
    if not (args.debug_login or args.debug_address):
        if not args.input or not args.output:
            p.error("the following arguments are required: -i/--input and -o/--output")

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)]
    )

    password = args.password or getpass.getpass(f"Password for {args.username}: ")

    # Hardened browser/page, same evasion pattern as your baseline
    def _make_page(pwt):
        ua = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        browser = pwt.chromium.launch(
            headless=not args.headful,
            args=["--disable-blink-features=AutomationControlled"]
        )
        ctx = browser.new_context(user_agent=ua)

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
        try:
            page.add_init_script("""
                Object.defineProperty(navigator, 'webdriver', {get: () => false});
                window.chrome = window.chrome || { runtime: {} };
            """)
        except Exception:
            pass
        return browser, ctx, page

    # --- Login-only debug ---
    if args.debug_login:
        sp = ensure_playwright()
        with sp() as pwt:
            browser, ctx, page = _make_page(pwt)
            ok = login_titlepro(page, args.username, password, timeout_ms=args.timeout * 1000, verbose=args.verbose)
            print(f"Login check: {'OK' if ok else 'FAILED'}")
            if args.headful and args.hold_after_login:
                try:
                    logging.info("Login check complete — press Enter in the terminal to close the browser.")
                    input("Press Enter to close the browser and exit...")
                except Exception:
                    pass
            browser.close()
        return

    # --- Single-address smoke test ---
    if args.debug_address:
        if not args.debug_city:
            p.error("--debug-address requires --debug-city")
        sp = ensure_playwright()
        with sp() as pwt:
            browser, ctx, page = _make_page(pwt)
            ok = login_titlepro(page, args.username, password, timeout_ms=args.timeout * 1000, verbose=args.verbose)
            logging.info(f"Login check: {'OK' if ok else 'FAILED'}")
            if ok:
                info = search_foreclosure_once(
                    page, args.debug_address, args.debug_city,
                    timeout_ms=args.timeout * 1000, save_debug=args.save_debug,
                    tag=re.sub(r"[^A-Za-z0-9_-]+", "_", args.debug_address)[:50]
                )
                print(json.dumps(asdict(info) if info else None, indent=2))
            browser.close()
        return

    # ---- Normal program flow ----
    df = read_table(args.input, sheet=args.sheet)
    addr_col, city_col = detect_address_city(df)
    if not addr_col or not city_col:
        logging.error("Required columns not found: Address and City.")
        sys.exit(2)

    # Ensure target columns exist
    needed_cols = [
        "Loan Amount", "NOD", "Sale Date", "Back to Beneficiary On", "Bene/Client Name",
        "Last Checked", "Changed Fields"
    ]
    for col in needed_cols:
        if col not in df.columns:
            df[col] = ""

    # Build worklist: ALL rows with Address+City
    work: List[Tuple[int, str, str]] = []
    for idx, row in df.iterrows():
        addr = "" if pd.isna(row.get(addr_col, "")) else str(row.get(addr_col, "")).strip()
        city = "" if pd.isna(row.get(city_col, "")) else str(row.get(city_col, "")).strip()
        if addr and city:
            work.append((idx, addr, city))

    total = len(work)
    if total == 0:
        logging.info("No rows with Address+City found.")
        write_table(df, args.output)
        return

    cache = ForeclosureCache(args.cache_file, args.cache)

    # Session health check
    def _session_alive(pg) -> bool:
        try:
            return (pg.locator(SEL_ADDR_INPUT).count() > 0 and pg.locator(SEL_CITY_INPUT).count() > 0)
        except Exception:
            return False

    sp = ensure_playwright()
    updated = 0
    looked_up = 0
    failed = 0

    with sp() as pwt:
        browser, ctx, page = _make_page(pwt)

        if not login_titlepro(page, args.username, password, timeout_ms=args.timeout * 1000, verbose=args.verbose):
            logging.error("Login failed; cannot proceed.")
            browser.close()
            sys.exit(3)

        iterator = work
        bar = None
        if args.progress and TQDM_AVAILABLE and sys.stdout.isatty() and total > 1:
            bar = tqdm(work, desc="TitlePro247 lookups", unit="row")
            bar.set_postfix_str(f"updated={updated} failed={failed}")

        for item in iterator if bar is None else bar:
            idx, addr, city = item if bar is None else item

            # Re-auth if needed
            if not _session_alive(page):
                logging.warning("[session] Search UI missing — attempting re-login.")
                if not login_titlepro(page, args.username, password, timeout_ms=args.timeout * 1000, verbose=args.verbose):
                    logging.error("[session] Re-login failed; stopping.")
                    break

            looked_up += 1

            # Cache
            cached = cache.get(addr, city)
            info_dict: Optional[Dict[str, Optional[str]]] = None
            if cached:
                info_dict = cached
            else:
                info = None
                attempts = max(1, int(args.max_retries))
                for attempt in range(attempts):
                    info = search_foreclosure_once(
                        page, addr, city, timeout_ms=args.timeout * 1000,
                        save_debug=args.save_debug, tag=re.sub(r"[^A-Za-z0-9_-]+", "_", addr)[:50]
                    )
                    if info:
                        break
                    if attempt < attempts - 1:
                        time.sleep(8)
                info_dict = asdict(info) if info else None
                if info_dict is not None:
                    cache.put(addr, city, info_dict)

            changed_cols: List[str] = []

            def _maybe_update(col: str, new_val: Optional[str]):
                nonlocal changed_cols
                if new_val is None or new_val == "":
                    return
                old_val = "" if pd.isna(df.at[idx, col]) else str(df.at[idx, col]).strip()
                nv = new_val.strip()
                ov = old_val.strip()
                if col in ("Loan Amount",):
                    # compare by digits only
                    cmp_nv = re.sub(r"[^0-9.]", "", nv).lower()
                    cmp_ov = re.sub(r"[^0-9.]", "", ov).lower()
                elif col in ("NOD", "Sale Date", "Back to Beneficiary On"):
                    cmp_nv = (normalize_date(nv) or nv).lower()
                    cmp_ov = (normalize_date(ov) or ov).lower()
                else:
                    cmp_nv = nv.lower()
                    cmp_ov = ov.lower()
                if cmp_nv != cmp_ov:
                    df.at[idx, col] = nv
                    changed_cols.append(col)

            if info_dict:
                _maybe_update("Loan Amount", info_dict.get("loan_amount"))
                _maybe_update("NOD", info_dict.get("nod"))
                _maybe_update("Sale Date", info_dict.get("sale_date"))
                _maybe_update("Back to Beneficiary On", info_dict.get("back_to_bene_on"))
                _maybe_update("Bene/Client Name", info_dict.get("bene_name"))
            else:
                failed += 1

            # Audit columns
            df.at[idx, "Last Checked"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            df.at[idx, "Changed Fields"] = ", ".join(changed_cols) if changed_cols else ""

            if changed_cols:
                updated += 1

            if bar is not None:
                bar.set_postfix_str(f"updated={updated} failed={failed}")
                bar.update(1)

            time.sleep(args.rate)

        if bar is not None:
            bar.close()
        browser.close()

    cache.save()
    write_table(df, args.output)
    logging.info(f"Done. Looked up: {looked_up}, Rows with updates: {updated}, Failed: {failed}")
    logging.info(f"Wrote: {args.output}")


if __name__ == "__main__":
    main()
