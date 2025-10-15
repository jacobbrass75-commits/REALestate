# Retran APN & Foreclosure Filler + PDF Downloader

## Overview

This tool automates the retrieval of **Assessor's Parcel Numbers (APNs)** and **foreclosure details** from [Retran.net](https://retran.net), then stores the results in a structured Excel file ready for Google Sheets import.

It also downloads **supporting PDF evidence files** (foreclosure notices, trustee deeds, etc.) to a local folder and automatically creates **clickable hyperlinks** in your Excel sheet pointing to those saved files.

The goal is to create a **verifiable property record system** where every lookup has a traceable data source (the saved PDF).

---

## Key Features

- Logs into Retran using your account credentials or cookie header.
- Reads addresses from an Excel or CSV file (`Properties` sheet preferred).
- Looks up APN, foreclosure stage, and related metadata.
- Downloads and saves up to N PDF documents per property.
- Creates Excel hyperlinks to each downloaded file for direct verification.
- Maintains a local cache (`.retran_cache.json`) so repeated runs skip already-seen properties.
- Writes detailed logs and optional HTML snapshots for debugging.

---

## Folder Structure

```
project/
├── retran_fill_with_pdfs.py          # Main enhanced script
├── retranFillPhase1/
│   └── retran_apn_fill-1.py         # Original script
├── convert_excel.py                  # Excel to CSV converter
├── .retran_cache.json               # Cache file (created on first run)
├── debug_html/                      # Debug HTML files (if --save-debug-html)
├── ~/Downloads/foreclosures/        # Downloaded PDFs (if --download-pdfs)
└── README_Enhanced.md               # This file
```

---

## Installation

### Prerequisites
- Python 3.8+ 
- Retran.net account credentials

### Setup
```bash
# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install pandas requests beautifulsoup4 tqdm playwright openpyxl
playwright install chromium
```

---

## Usage

### Basic APN Lookup
```bash
python retran_fill_with_pdfs.py \
  --input "input.xlsx" \
  --output "output.xlsx" \
  --username "your_email@domain.com" \
  --password "your_password"
```

### Full Workflow with PDFs
```bash
python retran_fill_with_pdfs.py \
  --input "/Users/you/Desktop/Retran_Master_Data.xlsx" \
  --output "/Users/you/Desktop/Retran_Master_Data_with_apn.xlsx" \
  --csv-output "/Users/you/Desktop/Retran_Master_Data_with_apn.csv" \
  --download-pdfs \
  --doc-dir "~/Downloads/foreclosures" \
  --max-pdfs-per-row 3 \
  --username "your_user" --password "your_pass" \
  --cache --progress --save-debug-html
```

### Command Line Options

#### Required
- `-i, --input`: Path to input Excel/CSV file
- `-o, --output`: Path to output Excel/CSV file

#### Authentication
- `--username`: Retran login username
- `--password`: Retran login password (omit to be prompted)
- `--cookie`: Cookie header for authentication

#### File Processing
- `--sheet`: Excel sheet name (default: 'Properties' if present)
- `--csv-output`: Also write a CSV copy of the updated sheet
- `--backup`: Create timestamped backup of input file

#### PDF Download
- `--download-pdfs`: Download PDF evidence files from result page
- `--doc-dir`: Folder for downloaded PDFs (default: ~/Downloads/foreclosures)
- `--max-pdfs-per-row`: Max PDFs to save per row (default: 3)

#### Performance
- `--rate`: Seconds between requests (default: 1.0)
- `--max-retries`: Max retries per address (default: 3)
- `--timeout`: Request/page timeout seconds (default: 30)
- `--parallel`: Parallel workers (experimental, default: 1)

#### Caching & Resume
- `--cache`: Enable result caching
- `--cache-file`: Cache file path (default: .retran_cache.json)

#### Output & Debug
- `--failed-output`: Write failed lookups to separate file
- `--progress`: Show progress bar (default: True)
- `--verbose, -v`: Verbose logging
- `--save-debug-html`: Save rendered HTML to ./debug_html

---

## Output Columns

The tool automatically adds these columns to your output file:

### Core Data
- `APN`: Assessor's Parcel Number
- `Time APN Added`: Timestamp when APN was found

### Foreclosure Fields
- `Foreclosure Stage`
- `NOD` (Notice of Default)
- `NOD Date`
- `NOS` (Notice of Sale)
- `NOS Date`
- `Sale Date`
- `Back to Beneficiary Date`
- `Foreclosure Document Type`
- `Foreclosure Recording Date`
- `LIS` (Lis Pendens)

### Evidence Files
- `Evidence Files (local)`: Semicolon-separated list of downloaded PDF paths
- `Evidence Link 1` through `Evidence Link 5`: Clickable Excel hyperlinks to PDFs

---

## Examples

### Convert Excel to CSV
```bash
python convert_excel.py
# Reads Retran_Master_Data.xlsx from Desktop
# Creates individual CSV files for each sheet
```

### Debug Single Address
```bash
python retran_fill_with_pdfs.py \
  --debug-address "123 Main St, City, ST" \
  --timeout 8 -v --save-debug-html
```

### Process with Caching
```bash
python retran_fill_with_pdfs.py \
  -i "properties.xlsx" -o "properties_with_apn.xlsx" \
  --username "user@domain.com" --password "pass" \
  --cache --download-pdfs --doc-dir "./evidence" \
  --max-pdfs-per-row 5 --verbose
```

---

## Troubleshooting

### Common Issues

1. **"No module named 'pandas'"**
   ```bash
   source .venv/bin/activate
   pip install pandas requests beautifulsoup4 tqdm playwright openpyxl
   ```

2. **"Playwright not available"**
   ```bash
   playwright install chromium
   ```

3. **Login failures**
   - Verify credentials
   - Try `--cookie` option with session cookie
   - Check if Retran requires 2FA

4. **PDF download issues**
   - Ensure `--doc-dir` path exists and is writable
   - Check network connectivity
   - Verify Retran session is still valid

### Debug Mode
Use `--save-debug-html` to save rendered pages for inspection:
```bash
python retran_fill_with_pdfs.py --debug-address "123 Main St" --save-debug-html
# Check ./debug_html/ folder for saved HTML files
```

---

## File Formats

### Input
- Excel: `.xlsx`, `.xlsm`, `.xls`
- CSV: `.csv`, `.tsv`
- Must have `Address` column (or similar)
- Optional `APN` column (will be filled if empty)

### Output
- Excel: `.xlsx` (with clickable hyperlinks)
- CSV: `.csv` (plain text, no hyperlinks)
- Both formats include all foreclosure and evidence columns

---

## Cache System

The tool maintains a local cache (`.retran_cache.json`) to avoid re-processing addresses:

- **Cache hits**: Skip already-processed addresses
- **Resume capability**: Continue from where you left off
- **Manual cache**: Edit `.retran_cache.json` to add/remove entries

---

## Integration with Google Sheets

1. Run the tool to generate Excel output
2. Open Excel file in Google Sheets
3. Hyperlinks will work if PDFs are accessible via Google Drive
4. For local PDFs, upload to Google Drive and update hyperlinks

---

## Support

For questions or issues:
- Check the debug HTML files in `./debug_html/`
- Review the log file: `retran_apn_YYYYMMDD_HHMMSS.log`
- Verify your Retran account has proper access
- Ensure all dependencies are installed correctly

---

## License

This tool is provided as-is for educational and business purposes. Please respect Retran.net's terms of service and rate limiting policies.
