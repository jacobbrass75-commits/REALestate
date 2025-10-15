Retran APN Filler — Quick Start (Mac & Windows)



What this tool does:

    Reads your Excel file with two columns: “Address” and “APN”.

    For each row where APN is blank, it searches Retran and fills in the APN.

    It also fills “Time APN Added” for each APN it adds.

    Your original input file is never changed. The program writes results to your output copy.
-------------------------
Before you start:

    Get your Excel file ready, Make sure it has a column named "Address" and a column named "APN".

    Make a copy of the file.

    Example:

        Input file: jacobsProperties.xlsx

        Output file: jacobsProperties2.xlsx (a simple copy of the input to start)

 
-------------------------
Install the requirements


A) macOS (Mac)

Open Terminal

Check that Python 3 is installed

python3 --version


If you do not see a version (like Python 3.11.x), install Python from https://www.python.org/downloads/
 (just run the installer), then reopen Terminal.

Install the Python libraries
Copy/paste each line:

python3 -m pip install --upgrade pip
python3 -m pip install pandas requests beautifulsoup4 tqdm playwright openpyxl
python3 -m playwright install chromium


openpyxl lets the program read/write .xlsx.
playwright + install chromium gives the tool a browser engine to load the site fully (so the table shows up).


------------------
B) Windows

Open PowerShell

Press Start, type PowerShell, open Windows PowerShell.

Check that Python 3 is installed

python --version


If you do not see a version (like Python 3.11.x), install Python from https://www.python.org/downloads/
 (check “Add Python to PATH” during install), then reopen PowerShell.

Install the Python libraries
Copy/paste each line:

python -m pip install --upgrade pip
python -m pip install pandas requests beautifulsoup4 tqdm playwright openpyxl
python -m playwright install chromium


Windows tip: Keep your output Excel file closed while the program runs. Windows locks files that are open in Excel.


-----------------------------------------

How to run it

Replace the example paths/emails/passwords with yours. Keep the quotes if the path has spaces.

macOS — Terminal

Recommended run (with caching, progress bar, and debug HTML saved):

python3 "path_to_retran_apn_fill-1.py" \
  -i "path_to_input_file" \
  -o "path_to_output_file" \
  --username "your_email@your_company.com" \
  --password "your_password" \
  --rate 1.0 --timeout 8 -v 

Windows — PowerShell

Recommended run (with caching, progress bar, and debug HTML saved):

python "C:path_to_retran_apn_fill-1.py" `
  -i "C:path_to_input_file" `
  -o "C:path_to_output_file" `
  --username "your_email@your_company.com" `
  --password "your_password" `
  --rate 1.0 --timeout 8 -v 


-----------------------------------


The output Excel is updated at the end with:

APNs filled into the APN column (for rows that were blank and successfully found),

the Time APN Added column filled for those rows.


-------------------------------------

Email me: matt.waeldner@du.edu

For questions. 