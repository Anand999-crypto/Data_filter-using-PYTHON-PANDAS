import pandas as pd
import os

# --- USER CONFIGURATION START ---


CSV_ROOT_DIRECTORY = r'D:\NSE DATA'


FUTURES_PREFIX = 'RELIANCE-I'

# 3. Define the final output CSV file name.
OUTPUT_CSV_FILE = 'RELIANCE_Futures_Complete_Data.csv'

# --- USER CONFIGURATION END ---


# ----------------- DATA PROCESSING -----------------

all_filtered_data = []

print(f"Recursively searching for all tickers starting with '{FUTURES_PREFIX}' in all CSV files under:")
print(f"-> {CSV_ROOT_DIRECTORY}")
print("-" * 40)

if not os.path.isdir(CSV_ROOT_DIRECTORY):
    print(f"\n--- ERROR ---")
    print(f"The directory specified does not exist: {CSV_ROOT_DIRECTORY}")
else:
    for dirpath, dirnames, filenames in os.walk(CSV_ROOT_DIRECTORY):
        for filename in filenames:
            if filename.lower().endswith('.csv'):
                file_path = os.path.join(dirpath, filename)
                print(f"Processing file: {file_path}...")
                
                try:
                    df = pd.read_csv(file_path, low_memory=False)

                    if 'Date' not in df.columns or 'Time' not in df.columns or 'Ticker' not in df.columns:
                        print(f"  - SKIPPED: File is missing required columns (Date, Time, Ticker).")
                        continue

                 
            
                    # We also ensure the Ticker is a string to avoid errors.
                    mask = df['Ticker'].astype(str).str.startswith(FUTURES_PREFIX, na=False)
                    filtered_df = df[mask].copy()

                    if not filtered_df.empty:
                        # Create a unified 'DateTime' column for sorting.
                        filtered_df['DateTime'] = pd.to_datetime(
                            filtered_df['Date'] + ' ' + filtered_df['Time'],
                            format='%d/%m/%Y %H:%M:%S',
                            errors='coerce'
                        )
                        all_filtered_data.append(filtered_df)
                        print(f"  + Found {len(filtered_df)} rows for tickers starting with {FUTURES_PREFIX}.")

                except Exception as e:
                    print(f"  - ERROR processing {filename}: {e}")

    # ----------------- FINAL AGGREGATION AND SAVING -----------------

    if not all_filtered_data:
        print("\n" + "-"*40)
        print(f"\n--- PROCESSING COMPLETE ---")
        print(f"No data was found for tickers starting with '{FUTURES_PREFIX}'.")
    else:
        print("\nCombining all found data...")
        final_df = pd.concat(all_filtered_data, ignore_index=True)
        
        final_df.dropna(subset=['DateTime'], inplace=True)

        print("Sorting all data by date and time...")
        final_df.sort_values(by='DateTime', inplace=True)

        final_df.drop(columns=['Date', 'Time'], inplace=True)

        print(f"Saving complete futures data to CSV file: {OUTPUT_CSV_FILE}")
        final_df.to_csv(OUTPUT_CSV_FILE, index=False)

        # ----------------- RESULTS -----------------
        print("\n" + "-"*40)
        print(f"\n--- SUCCESS ---")
        print(f"Complete futures data for tickers starting with **{FUTURES_PREFIX}** has been saved.")
        print(f"Total rows exported: {len(final_df)}")
        print(f"Output saved to: **{OUTPUT_CSV_FILE}**")
