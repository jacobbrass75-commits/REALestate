#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Retran APN & Foreclosure Filler + PDF Downloader (Sheets-ready)
---------------------------------------------------------------
- Reads your Excel/CSV (defaults to the 'Properties' sheet if present)
- Looks up APN via Retran (requests + Playwright fallback)
- Parses foreclosure fields (NOD, NOS, Sale Date, etc.)
- Downloads evidence PDFs found on the result page to a folder you choose
- Writes back XLSX with clickable hyperlinks to each downloaded PDF
- Optionally mirrors the updated sheet to CSV

USAGE (Mac example):
  python retran_fill_with_pdfs.py \
    --input "/Users/you/Desktop/Retran_Master_Data.xlsx" \
    --output "/Users/you/Desktop/Retran_Master_Data_with_apn.xlsx" \
    --csv-output "/Users/you/Desktop/Retran_Master_Data_with_apn.csv" \
    --download-pdfs \
    --doc-dir "~/Downloads/foreclosures" \
    --max-pdfs-per-row 3 \
    --username "your_user" --password "your_pass" \
    --cache --progress --save-debug-html
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
from urllib.parse import urljoin, urlparse, quote as urlquote

import pandas as pd
import requests
from bs4 import BeautifulSoup

try:
    from tqdm import tqdm
    TQDM_AVAILABLE = True
except ImportError:
    TQDM_AVAILABLE = False

RETRAN_BASE_URL = "https://retran.net"
RETRAN_SEARCH_URL = "https://retran.net/reports/list.asp"
RETRAN_LOGIN_URL = "https://retran.net/login4scvb2.asp"

# Cache lock (thread-safety)
_cache_lock = threading.Lock()

# APN patterns
APN_DASHED_RE = re.compile(r'\b\d{2,4}-\d{3,5}-\d{2,3}\b')
APN_ALNUM_RE  = re.compile(r'\b(?=[A-Z0-9-]{6,20}\b)(?=.*\d)[A-Z0-9-]+\b', re.I)

# Foreclosure columns
FORECLOSURE_COLUMNS = [
    "Foreclosure Stage",
    "NOD",
    "NOD Date",
    "NOS",
    "NOS Date",
    "Sale Date",
    "Back to Beneficiary Date",
    "Foreclosure Document Type",
    "Foreclosure Recording Date",
    "LIS",
]

# Evidence link columns we'll create (clickable Excel HYPERLINK formulas)
EVIDENCE_LINK_COLUMNS = [f"Evidence Link {i}" for i in range(1, 6)]  # up to 5 links per row by default


class APNSearcher:
    def __init__(self, cache_enabled=False, cache_file=".retran_cache.json"):
        self.session = requests.Session()
        self.cache_enabled = cache_enabled
        self.cache_file = cache_file
        self.cache: Dict[str, Dict[str, object]] = {}  # addr -> {"apn": str|None, "foreclosure": dict, "files": [paths]}
        self.stats = {'found': 0, 'not_found': 0, 'errors': 0, 'cache_hits': 0}
        self.load_cache()

    @staticmethod
    def _norm_addr(address: str) -> str:
        return re.sub(r"\s+", " ", (address or "").strip().upper())

    def load_cache(self):
        if self.cache_enabled and os.path.exists(self.cache_file):
            try:
                with open(self.cache_file, "r") as f:
                    self.cache = json.load(f)
                logging.info(f"Loaded {len(self.cache)} cached entries from {self.cache_file}")
            except Exception as e:
                logging.warning(f"Failed to load cache: {e}")

    def save_cache(self):
        if self.cache_enabled:
            try:
                with open(self.cache_file, "w") as f:
                    json.dump(self.cache, f, indent=2)
            except Exception as e:
                logging.warning(f"Failed to save cache: {e}")

    def get_cached(self, address: str):
        if not self.cache_enabled:
            return None
        key = self._norm_addr(address)
        val = self.cache.get(key)
        if val is None:
            return None
        return val.get("apn"), val.get("foreclosure") or {}, val.get("files") or []

    def put_cache(self, address: str, apn: Optional[str], foreclosure: Dict[str, str], files: List[str]):
        if self.cache_enabled:
            key = self._norm_addr(address)
            self.cache[key] = {"apn": apn, "foreclosure": foreclosure, "files": files}


def read_table(path: str, sheet: Optional[str] = None) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Input file not found: {path}")
    ext = os.path.splitext(path.lower())[1]
    if ext in [".xlsx", ".xlsm", ".xls"]:
        if sheet is None:
            x = pd.ExcelFile(path)
            sheet = "Properties" if "Properties" in x.sheet_names else x.sheet_names[0]
            logging.info(f"No sheet specified; using '{sheet}'")
        return pd.read_excel(path, sheet_name=sheet)
    elif ext in [".csv", ".tsv"]:
        sep = "," if ext == ".csv" else "\t"
        return pd.read_csv(path, sep=sep, encoding="utf-8")
    else:
        raise ValueError("Unsupported input extension. Use .xlsx/.xlsm/.xls/.csv/.tsv")


def write_table(df: pd.DataFrame, path: str) -> None:
    ext = os.path.splitext(path.lower())[1]
    if ext in [".xlsx", ".xlsm", ".xls"]:
        # Keep formulas as formulas in Excel
        df.to_excel(path, index=False)
    elif ext in [".csv", ".tsv"]:
        sep = "," if ext == ".csv" else "\t"
        df.to_csv(path, index=False, sep=sep, encoding="utf-8")
    else:
        raise ValueError("Unsupported output extension.")


def create_backup(file_path: str) -> str:
    if not os.path.exists(file_path):
        return ""
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    p = Path(file_path)
    backup = p.parent / f"{p.stem}_backup_{ts}{p.suffix}"
    try:
        import shutil
        shutil.copy2(file_path, backup)
        logging.info(f"Backup created: {backup}")
        return str(backup)
    except Exception as e:
        logging.warning(f"Backup failed: {e}")
        return ""


def detect_columns(df: pd.DataFrame) -> Tuple[Optional[str], Optional[str]]:
    cols = [str(c) for c in df.columns]
    addr = next((c for c in cols if re.match(r'^(address|.*street.*address|.*property.*address)$', c, re.I)), None)
    apn  = next((c for c in cols if re.match(r'^(apn|assessor.*parcel.*number|parcel.*(id|number))$', c, re.I)), None)
    return addr, apn


def login_to_retran(session: requests.Session, username: str, password: str,
                    login_url: str = RETRAN_LOGIN_URL, timeout: int = 30) -> bool:
    try:
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        })
        resp = session.get(login_url, timeout=timeout)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        form = soup.find("form") or soup
        data = {}
        for inp in form.find_all("input"):
            n = inp.get("name"); t = (inp.get("type") or "").lower(); v = inp.get("value") or ""
            if n and t == "hidden":
                data[n] = v
        for k in ["username", "user", "login", "email"]:
            data[k] = username
        for k in ["password", "pass", "pwd"]:
            data[k] = password
        action = form.get("action") or login_url
        post_url = action if action.startswith("http") else urljoin(login_url, action)
        r = session.post(post_url, data=data, timeout=timeout, allow_redirects=True)
        r.raise_for_status()
        ok = "logout" in r.text.lower() or "search" in r.text.lower()
        if not ok:
            t = session.get(RETRAN_SEARCH_URL, timeout=timeout)
            ok = t.status_code == 200 and "search" in t.text.lower()
        return bool(ok)
    except Exception as e:
        logging.error(f"Login failed: {e}")
        return False


def save_debug_html(html: str, address: str, debug_dir: str = "debug_html"):
    try:
        os.makedirs(debug_dir, exist_ok=True)
        safe = re.sub(r"[^\w\s-]", "", (address or "").replace(" ", "_"))[:60]
        fp = os.path.join(debug_dir, f"{safe}_{datetime.now().strftime('%H%M%S')}.html")
        with open(fp, "w", encoding="utf-8") as f:
            f.write(html)
        return fp
    except Exception:
        return None


def parse_apn_from_html(html: str, address: str = "", verbose: bool = False) -> Optional[str]:
    if not html:
        return None
    soup = BeautifulSoup(html, "html.parser")
    def _norm(s): return re.sub(r"\s+", " ", (s or "")).strip().upper()
    target = _norm(address)
    def _first_chunk(cell_html: str) -> str:
        first = re.split(r"<br\s*/?>", cell_html, flags=re.I, maxsplit=1)[0]
        return BeautifulSoup(first, "html.parser").get_text(" ", strip=True)
    rows = soup.find_all("tr", id=re.compile(r"^theRow\d+$"))
    if not rows:
        wf = soup.find(id="window-float")
        if wf:
            rows = wf.find_all("tr")
    pref = []
    if target:
        for r in rows:
            if target in _norm(r.get_text(" ", strip=True)):
                pref.append(r)
    use = pref or rows
    apns = []
    for r in use:
        tds = r.find_all("td")
        if len(tds) < 3:
            continue
        leading = _first_chunk(str(tds[2]))
        m = APN_DASHED_RE.search(leading) or APN_ALNUM_RE.search(leading)
        if m:
            apn = m.group(0).strip()
            apns.append(apn)
    out, seen = [], set()
    for a in apns:
        if a not in seen:
            seen.add(a); out.append(a)
    return "; ".join(out) if out else None


def parse_foreclosure_fields_from_html(html: str, verbose: bool = False) -> Dict[str, str]:
    wanted = {k: "" for k in FORECLOSURE_COLUMNS}
    if not html:
        return wanted
    soup = BeautifulSoup(html, "html.parser")
    def norm_txt(s): return re.sub(r"\s+", " ", (s or "")).strip()
    def lc(s): return norm_txt(s).lower()
    # Heuristic 1
    for row in soup.find_all(["tr", "div"]):
        cells = row.find_all(["td", "div", "span", "label"], recursive=False)
        if len(cells) >= 2:
            label = lc(cells[0].get_text(" ", strip=True))
            val = norm_txt(cells[1].get_text(" ", strip=True))
            for k in FORECLOSURE_COLUMNS:
                if lc(k) in label and not wanted[k]:
                    wanted[k] = val
    # Heuristic 2
    for lab in soup.find_all(["label", "span", "div"]):
        label_text = lc(lab.get_text(" ", strip=True))
        nxt = lab.find_next_sibling()
        if not nxt:
            continue
        field = nxt if nxt.name in ("input", "select") else nxt.find(["input", "select"])
        val_text = ""
        if field:
            if field.name == "select":
                sel = field.find("option", selected=True) or field.find("option")
                val_text = norm_txt(sel.get_text(" ", strip=True)) if sel else ""
            else:
                val_text = norm_txt(field.get("value") or field.get("placeholder") or field.get_text(" ", strip=True))
        else:
            val_text = norm_txt(nxt.get_text(" ", strip=True))
        for k in FORECLOSURE_COLUMNS:
            if lc(k) in label_text and not wanted[k]:
                wanted[k] = val_text
    # Heuristic 3
    for inp in soup.find_all(["input", "select"]):
        meta = " ".join([
            lc(inp.get("aria-label") or ""),
            lc(inp.get("placeholder") or ""),
            lc(inp.get("name") or "")
        ])
        if not meta.strip():
            continue
        if inp.name == "select":
            sel = inp.find("option", selected=True) or inp.find("option")
            text_val = norm_txt(sel.get_text(" ", strip=True)) if sel else ""
        else:
            text_val = norm_txt(inp.get("value") or inp.get_text(" ", strip=True))
        for k in FORECLOSURE_COLUMNS:
            if lc(k) in meta and not wanted[k]:
                wanted[k] = text_val
    return wanted


def _url_is_pdf(session: requests.Session, url: str, timeout: int) -> bool:
    try:
        # Try HEAD first
        h = session.head(url, allow_redirects=True, timeout=timeout)
        ct = h.headers.get("Content-Type", "").lower()
        if "application/pdf" in ct:
            return True
    except Exception:
        pass
    try:
        # Light GET (no stream) to sniff headers
        g = session.get(url, allow_redirects=True, timeout=timeout, stream=True)
        ct = g.headers.get("Content-Type", "").lower()
        g.close()
        return "application/pdf" in ct or urlparse(g.url).path.lower().endswith(".pdf")
    except Exception:
        return urlparse(url).path.lower().endswith(".pdf")


def _find_pdf_links_from_html(html: str, base_url: str = RETRAN_SEARCH_URL, max_links: int = 10) -> List[str]:
    """Return absolute URLs that look like PDFs, deduped, up to max_links."""
    if not html:
        return []
    soup = BeautifulSoup(html, "html.parser")
    hrefs = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href or href.startswith("#"):
            continue
        abs_url = urljoin(base_url, href)
        if abs_url.lower().endswith(".pdf"):
            hrefs.append(abs_url)
    # Dedup in order
    out, seen = [], set()
    for u in hrefs:
        if u not in seen:
            seen.add(u); out.append(u)
        if len(out) >= max_links:
            break
    return out


def _sanitize_filename(s: str) -> str:
    s = re.sub(r"[^\w\s.-]+", "", (s or "")).strip().replace(" ", "_")
    return s[:80] if len(s) > 80 else s


def _download_pdfs(session: requests.Session, urls: List[str], dest_dir: str, prefix: str, timeout: int,
                   referer: str = RETRAN_SEARCH_URL, max_files: int = 5) -> List[str]:
    """Download up to max_files PDFs to dest_dir, return saved filepaths."""
    saved = []
    if not urls:
        return saved
    os.makedirs(dest_dir, exist_ok=True)
    headers = {"Referer": referer, "User-Agent": session.headers.get("User-Agent", "Mozilla/5.0")}
    for i, url in enumerate(urls[:max_files], start=1):
        try:
            if not _url_is_pdf(session, url, timeout=timeout):
                continue
            r = session.get(url, headers=headers, timeout=timeout, allow_redirects=True, stream=True)
            r.raise_for_status()
            # Final name
            ext = ".pdf"
            fname = f"{_sanitize_filename(prefix)}_{i}{ext}"
            path = os.path.join(dest_dir, fname)
            with open(path, "wb") as f:
                for chunk in r.iter_content(chunk_size=65536):
                    if chunk:
                        f.write(chunk)
            saved.append(os.path.abspath(path))
        except Exception as e:
            logging.debug(f"PDF download failed for {url}: {e}")
            continue
    return saved


def _excel_hyperlink_formula(local_path: str, label: str) -> str:
    """
    Build an Excel HYPERLINK formula pointing to a local file. We use a file:// URL
    with URL-encoded path to avoid issues with spaces.
    """
    abs_path = os.path.abspath(os.path.expanduser(local_path))
    # Excel tends to accept plain paths too, but file:/// is safer across OSes.
    file_url = "file:///" + urlquote(abs_path).replace("%3A", ":").replace("%5C", "/")
    safe_label = label.replace('"', "'")
    return f'=HYPERLINK("{file_url}","{safe_label}")'


def fetch_apn_foreclosure_and_pdfs(searcher: APNSearcher, address: str, timeout: int,
                                   verbose: bool = False, save_debug: bool = False,
                                   download_pdfs: bool = False, doc_dir: Optional[str] = None,
                                   max_pdfs_per_row: int = 3) -> Tuple[Optional[str], str, Dict[str, str], List[str]]:
    """
    Full flow:
      - load rendered HTML via Playwright (fallback to requests)
      - parse APN + foreclosure fields
      - (optional) find and download PDF links; return saved file paths
    """
    if not address or not str(address).strip():
        searcher.put_cache(address or "", None, {}, [])
        return None, "skipped_empty", {}, []

    cached = searcher.get_cached(address)
    if cached is not None:
        searcher.stats['cache_hits'] += 1
        apn, fore, files = cached
        return apn, ("found" if apn else "not_found"), fore, files

    # -- Try Playwright
    html_blobs: List[str] = []
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    except Exception:
        html_blobs = []  # will trigger requests fallback

    if html_blobs == []:  # Only try Playwright if we haven't already
        # Playwright flow
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                ua = searcher.session.headers.get("User-Agent", (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
                ))
                context = browser.new_context(user_agent=ua)

                # Copy cookies from requests session
                try:
                    jar = searcher.session.cookies
                    cookies = []
                    for c in jar:
                        dom = c.domain or "retran.net"
                        if "retran.net" not in dom:
                            continue
                        domain = dom if dom.startswith(".") else f".{dom}"
                        obj = {"name": c.name, "value": c.value, "domain": domain, "path": c.path or "/",
                               "secure": True, "httpOnly": False, "sameSite": "Lax"}
                        if getattr(c, "expires", None):
                            try: obj["expires"] = int(c.expires)
                            except Exception: pass
                        cookies.append(obj)
                    if cookies:
                        context.add_cookies(cookies)
                except Exception:
                    pass

                # Block logout booby-traps
                def _route(route, request):
                    if "logout.asp" in request.url.lower():
                        return route.abort()
                    return route.continue_()
                context.route("**/*", _route)

                page = context.new_page()
                page.add_init_script("""
                    try{
                      window.clicked=true;
                      window.CheckBrowser=function(){};
                      Object.defineProperty(window,'onbeforeunload',{get(){return null;},set(){}});
                      Object.defineProperty(window,'onunload',{get(){return null;},set(){}});
                      window.addEventListener('beforeunload',e=>{e.stopImmediatePropagation();},true);
                      window.addEventListener('unload',e=>{e.stopImmediatePropagation();},true);
                    }catch(e){}
                """)
                wait_ms = max(7000, int(timeout * 1000))
                page.goto("https://retran.net/search.asp", timeout=wait_ms)
                
                # Wait for page to load completely
                page.wait_for_load_state("networkidle", timeout=wait_ms)
                
                # Use the actual address, not URL-encoded
                search_address = (address or "").strip()
                if verbose:
                    logging.info(f"Searching for address: {search_address}")
                
                # Try to find and fill the search input
                try:
                    if page.locator("input[name='txtSearch']").count() > 0:
                        page.fill("input[name='txtSearch']", search_address)
                    else:
                        # Create hidden input if not found
                        page.evaluate(f"(v)=>{{let i=document.createElement('input');i.type='hidden';i.name='txtSearch';i.value=v;(document.forms[0]||document.body).appendChild(i);}}", search_address)
                except Exception as e:
                    if verbose:
                        logging.debug(f"Error filling search input: {e}")

                page.evaluate("""
                    () => {
                      const ensure=(n,v)=>{let el=document.querySelector(`input[name="${n}"]`);
                        if(!el){el=document.createElement('input');el.type='hidden';el.name=n;(document.forms[0]||document.body).appendChild(el);}
                        el.value=v;};
                      ensure('select','Property Address');
                      ensure('sortIndex','tor_mailing_city');
                      ensure('sortType','asc');
                      ensure('startNum','1');
                      ensure('endNum','100');
                    }
                """)

                # Submit the form
                try:
                    if page.locator("input[type='submit']").count() > 0:
                        page.click("input[type='submit']")
                    elif page.locator("button[type='submit']").count() > 0:
                        page.click("button[type='submit']")
                    elif page.locator("form input[type=submit]").count() > 0:
                        page.click("form input[type=submit]")
                    elif page.locator("form button[type=submit]").count() > 0:
                        page.click("form button[type=submit]")
                    else:
                        page.evaluate("()=>{ const f=document.forms[0]; if(f) f.submit(); }")
                    
                    if verbose:
                        logging.info("Form submitted, waiting for results...")
                except Exception as e:
                    if verbose:
                        logging.debug(f"Error submitting form: {e}")

                try:
                    page.wait_for_load_state("networkidle", timeout=wait_ms)
                except PWTimeout:
                    pass

                # Collect DOMs (main + frames)
                try: html_blobs.append(page.evaluate("document.documentElement.outerHTML"))
                except Exception: pass
                for fr in page.frames:
                    if fr == page.main_frame: continue
                    try: html_blobs.append(fr.evaluate("document.documentElement.outerHTML"))
                    except Exception: continue

                browser.close()
        except Exception as e:
            logging.debug(f"Playwright flow failed: {e}")
            html_blobs = []

    # requests fallback if needed
    if not html_blobs:
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
            html_blobs = [html]
        except Exception as e:
            logging.debug(f"Requests fallback failed: {e}")
            searcher.put_cache(address, None, {}, [])
            return None, "error", {}, []

    # Choose the blob that actually contains rows, else the largest
    chosen = None
    for h in html_blobs:
        if re.search(r'id=["\']theRow\\d+["\']', h):
            chosen = h; break
    if chosen is None:
        chosen = max(html_blobs, key=len) if html_blobs else ""

    if save_debug:
        save_debug_html(chosen, address)

    apn = parse_apn_from_html(chosen, address=address, verbose=verbose)
    foreclosure = parse_foreclosure_fields_from_html(chosen, verbose=verbose)

    files: List[str] = []
    if download_pdfs:
        pdf_urls = _find_pdf_links_from_html(chosen, base_url=RETRAN_SEARCH_URL, max_links=10)
        if pdf_urls:
            dest_dir = os.path.abspath(os.path.expanduser(doc_dir or "~/Downloads/foreclosures"))
            prefix = f"{_sanitize_filename(address)}_{_sanitize_filename(apn or 'noapn')}"
            files = _download_pdfs(searcher.session, pdf_urls, dest_dir, prefix, timeout, RETRAN_SEARCH_URL, max_files=max_pdfs_per_row)

    searcher.put_cache(address, apn, foreclosure, files)
    return apn, ("found" if apn else "not_found"), foreclosure, files


def export_failed_lookups(failed_data: List[Dict], output_path: str):
    if not failed_data:
        return
    try:
        df = pd.DataFrame(failed_data)
        write_table(df, output_path)
        logging.info(f"Exported {len(failed_data)} failed lookups -> {output_path}")
    except Exception as e:
        logging.error(f"Failed to export failed lookups: {e}")


def main():
    parser = argparse.ArgumentParser(description="Retran APN & Foreclosure Filler + PDF downloader")
    # Required
    parser.add_argument("-i", "--input", required=True, help="Path to input Excel/CSV (defaults to 'Properties' if present)")
    parser.add_argument("-o", "--output", required=True, help="Path to output Excel/CSV")
    # Optional CSV mirror
    parser.add_argument("--csv-output", help="Also write a CSV copy of the updated Properties sheet")
    # Auth
    parser.add_argument("--username", help="Login username")
    parser.add_argument("--password", help="Login password (omit to be prompted)")
    parser.add_argument("--cookie", help="Cookie header for authentication")
    # File
    parser.add_argument("--sheet", help="Excel sheet name (default: 'Properties' if present)")
    parser.add_argument("--backup", action="store_true", help="Create timestamped backup of INPUT file")
    # Processing
    parser.add_argument("--rate", type=float, default=1.0, help="Seconds between requests (default: 1.0)")
    parser.add_argument("--max-retries", type=int, default=3, help="Max retries per address")
    parser.add_argument("--timeout", type=int, default=30, help="Request/page timeout seconds")
    parser.add_argument("--parallel", type=int, default=1, help="Parallel workers (experimental)")
    # Cache & resume
    parser.add_argument("--cache", action="store_true", help="Enable result caching")
    parser.add_argument("--cache-file", default=".retran_cache.json", help="Cache file path")
    # Output
    parser.add_argument("--failed-output", help="Write failed lookups to separate file (xlsx/csv)")
    parser.add_argument("--progress", action="store_true", default=True, help="Show progress bar")
    # Debug
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose logging")
    parser.add_argument("--save-debug-html", action="store_true", help="Save rendered HTML to ./debug_html")
    # PDFs
    parser.add_argument("--download-pdfs", action="store_true", help="Download PDF evidence files from result page")
    parser.add_argument("--doc-dir", default="~/Downloads/foreclosures", help="Folder for downloaded PDFs (default: ~/Downloads/foreclosures)")
    parser.add_argument("--max-pdfs-per-row", type=int, default=3, help="Max PDFs to save per row (default: 3)")

    args = parser.parse_args()

    # Logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(sys.stdout),
                  logging.FileHandler(f"retran_apn_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")]
    )

    failed_lookups: List[Dict] = []
    df_out: Optional[pd.DataFrame] = None
    searcher: Optional[APNSearcher] = None

    try:
        if args.backup:
            create_backup(args.input)

        # Read INPUT (we never mutate it directly)
        df_in = read_table(args.input, sheet=args.sheet)
        df_out = df_in.copy(deep=True)

        # Ensure required columns
        address_col, apn_col = detect_columns(df_out)
        if not address_col:
            logging.error("No address column found. Include a column named 'Address' (or similar).")
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
            logging.info(f"Renamed columns: {mapping}")

        if "Time APN Added" not in df_out.columns:
            df_out["Time APN Added"] = ""

        # Ensure foreclosure & evidence columns
        for col in FORECLOSURE_COLUMNS:
            if col not in df_out.columns:
                df_out[col] = ""
        # one column with all paths (plain text), plus several clickable link columns
        if "Evidence Files (local)" not in df_out.columns:
            df_out["Evidence Files (local)"] = ""
        for col in EVIDENCE_LINK_COLUMNS:
            if col not in df_out.columns:
                df_out[col] = ""

        # Build worklist
        work: List[Tuple[int, str]] = []
        for idx, row in df_out.iterrows():
            addr = str(row.get("Address", "")).strip()
            apn_cur = str(row.get("APN", "")).strip()
            if addr and (not apn_cur or apn_cur.lower() in ("nan", "none")):
                work.append((idx, addr))

        if not work:
            logging.info("No addresses require APN lookup; writing output copy...")
            write_table(df_out, args.output)
            if args.csv_output:
                write_table(df_out, args.csv_output)
            return

        # Prepare session and auth
        searcher = APNSearcher(cache_enabled=args.cache, cache_file=args.cache_file)
        if args.cookie:
            searcher.session.headers["Cookie"] = args.cookie
        if args.username:
            pw = args.password or getpass.getpass(f"Password for {args.username}: ")
            ok = login_to_retran(searcher.session, args.username, pw, RETRAN_LOGIN_URL, args.timeout)
            if not ok:
                logging.warning("Login may have failed; proceeding anyway.")

        # Lookup function with retries
        def do_lookup(idx: int, addr: str):
            last_status = "error"
            apn_val: Optional[str] = None
            fore: Dict[str, str] = {}
            files: List[str] = []
            for _ in range(args.max_retries):
                apn_val, last_status, fore, files = fetch_apn_foreclosure_and_pdfs(
                    searcher, addr, args.timeout, verbose=args.verbose, save_debug=args.save_debug_html,
                    download_pdfs=args.download_pdfs, doc_dir=args.doc_dir, max_pdfs_per_row=args.max_pdfs_per_row
                )
                if apn_val or last_status in ("found", "not_found"):
                    break
                time.sleep(args.rate)
            return idx, apn_val, last_status, fore, files

        results = []
        # Run (sequential or parallel)
        if args.parallel and args.parallel > 1:
            packed = [(i, a) for (i, a) in work]
            def worker(item):
                i, a = item
                res = do_lookup(i, a)
                time.sleep(args.rate)
                return res
            with ThreadPoolExecutor(max_workers=args.parallel) as ex:
                futs = [ex.submit(worker, it) for it in packed]
                for f in as_completed(futs):
                    results.append(f.result())
            results.sort(key=lambda x: x[0])
        else:
            iterator = work
            if args.progress and TQDM_AVAILABLE:
                iterator = tqdm(work, desc="APN+Foreclosure+PDFs", unit="row")
            for (idx, addr) in iterator:
                results.append(do_lookup(idx, addr))
                time.sleep(args.rate)

        # Apply to df_out
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        filled = 0
        for idx, apn_val, status, foreclosure, files in results:
            # APN + timestamp
            if apn_val and str(apn_val).strip():
                cur = str(df_out.at[idx, "APN"]).strip() if pd.notna(df_out.at[idx, "APN"]) else ""
                if not cur:
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
            # Foreclosure fields
            for col in FORECLOSURE_COLUMNS:
                val = foreclosure.get(col, "")
                if val and pd.notna(val):
                    df_out.at[idx, col] = val
            # Evidence file paths (plain text list)
            if files:
                df_out.at[idx, "Evidence Files (local)"] = "; ".join(files)
                # And set clickable Excel hyperlinks in columns Evidence Link 1..N
                for j, fpath in enumerate(files[:len(EVIDENCE_LINK_COLUMNS)], start=1):
                    df_out.at[idx, f"Evidence Link {j}"] = _excel_hyperlink_formula(fpath, f"PDF {j}")

        logging.info(f"Filled {filled} new APN(s).")

        # Save cache + failures
        try:
            if searcher:
                searcher.save_cache()
        except Exception as e:
            logging.warning(f"Could not save cache: {e}")

        if args.failed_output and failed_lookups:
            export_failed_lookups(failed_lookups, args.failed_output)

        # Write outputs
        write_table(df_out, args.output)
        if args.csv_output:
            write_table(df_out, args.csv_output)
        logging.info("=== Processing complete ===")

    except Exception as e:
        logging.exception(f"Fatal error: {e}")
        try:
            if df_out is not None:
                write_table(df_out, args.output)
                if args.csv_output:
                    write_table(df_out, args.csv_output)
        except Exception:
            pass


if __name__ == "__main__":
    main()
