import pandas as pd
import os

# Define paths - looking on Desktop
input_path = "/Users/brass/Desktop/Retran_Master_Data.xlsx"
output_path = "Retran_Master_Data.csv"

# Check if file exists
if not os.path.exists(input_path):
    print(f"File not found: {input_path}")
    print("Available files on Desktop:")
    desktop_files = [f for f in os.listdir("/Users/brass/Desktop") if f.endswith(('.xlsx', '.xls', '.csv'))]
    for f in desktop_files:
        print(f"  - {f}")
    exit(1)

# Load the Excel file
df = pd.read_excel(input_path, sheet_name=None)  # Reads all sheets
print(f"Sheets found: {list(df.keys())}")

# Option 1: Save each sheet to its own CSV
for sheet_name, data in df.items():
    sheet_csv_path = f"{os.path.splitext(output_path)[0]}_{sheet_name}.csv"
    data.to_csv(sheet_csv_path, index=False)
    print(f"Saved {sheet_name} → {sheet_csv_path}")

# Option 2 (optional): Combine all sheets into one CSV
combined = pd.concat(df.values(), ignore_index=True)
combined.to_csv("Retran_Master_Data_combined.csv", index=False)
print("Combined CSV saved → Retran_Master_Data_combined.csv")
