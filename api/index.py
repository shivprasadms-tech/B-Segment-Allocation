import os
import pandas as pd
from datetime import datetime
import warnings
import shutil
import tempfile
import re
from flask import Flask, request, render_template, redirect, url_for, send_file, flash, session
from werkzeug.utils import secure_filename

# Suppress warnings that might clutter the console during execution
warnings.filterwarnings('ignore')

# Determine the base directory of the application
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Construct the path to the templates folder (assuming it's one level up from the script)
template_dir = os.path.join(BASE_DIR, '..', 'templates') 

# Initialize the Flask application
app = Flask(__name__, template_folder=template_dir)
# Set a secret key for session management. Use an environment variable for production.
app.secret_key = os.environ.get('FLASK_SECRET_KEY', 'default_secret_key_for_local_dev_only')

# --- Global Variables ---
# Define the standard columns expected in the final consolidated output
CONSOLIDATED_OUTPUT_COLUMNS = [
    'Barcode', 'Processor', 'Channel', 'Category', 'Company code', 'Region',
    'Vendor number', 'Vendor Name', 'Status', 'Received Date', 'Re-Open Date',
    'Allocation Date', 'Clarification Date', 'Completion Date', 'Requester',
    'Remarks', 'Aging', 'Today'
]

# --- Helper Functions ---

def format_date_to_mdyyyy(date_series):
    """
    Formats a pandas Series of dates to MM/DD/YYYY string format.
    Handles potential mixed types and NaT values.
    """
    # Convert series to datetime, coercing errors will turn invalid dates into NaT
    datetime_series = pd.to_datetime(date_series, errors='coerce')
    # Apply formatting only to valid dates, otherwise return an empty string
    formatted_series = datetime_series.apply(
        lambda x: f"{x.month}/{x.day}/{x.year}" if pd.notna(x) else ''
    )
    return formatted_series

def clean_column_names(df):
    """
    Cleans DataFrame column names by:
    1. Lowercasing all characters.
    2. Replacing spaces with underscores.
    3. Removing special characters (keeping only alphanumeric and underscores).
    4. Removing leading/trailing underscores.
    """
    new_columns = []
    for col in df.columns:
        col = str(col).strip().lower() # Strip whitespace and lowercase
        col = re.sub(r'\s+', '_', col) # Replace any sequence of whitespace with a single underscore
        col = re.sub(r'[^a-z0-9_]', '', col) # Remove non-alphanumeric and non-underscore characters
        col = col.strip('_') # Remove leading/trailing underscores
        new_columns.append(col)
    df.columns = new_columns
    return df

def consolidate_data_process(df_pisa, df_esm, df_pm7, df_workon, df_rgpa, consolidated_output_file_path):
    """
    Reads PISA, ESM, PM7, Workon, and RGPA Excel files (now passed as DFs), filters PISA & RGPA,
    consolidates data, and saves it to a new Excel file.
    """
    print("Starting data consolidation process...")
    print("All input DataFrames loaded successfully!")

    # Clean column names for all required input DataFrames
    df_pisa = clean_column_names(df_pisa.copy())
    df_esm = clean_column_names(df_esm.copy())
    df_pm7 = clean_column_names(df_pm7.copy())
    
    # Clean Workon and RGPA columns. Handle optional Workon by ensuring it's an empty DataFrame if not provided.
    df_workon_cleaned = clean_column_names(df_workon.copy()) if df_workon is not None and not df_workon.empty else pd.DataFrame()
    df_rgpa_cleaned = clean_column_names(df_rgpa.copy()) # RGPA is required, df_rgpa should not be None

    # Filter PISA data based on allowed users
    allowed_pisa_users = ["Goswami Sonali", "Patil Jayapal Gowd", "Ranganath Chilamakuri","Sridhar Divya","Sunitha S","Varunkumar N"]
    if 'assigned_user' in df_pisa.columns:
        original_pisa_count = len(df_pisa)
        df_pisa_filtered = df_pisa[df_pisa['assigned_user'].isin(allowed_pisa_users)].copy()
        print(f"\nPISA file filtered. Original records: {original_pisa_count}, Records after filter: {len(df_pisa_filtered)}")
    else:
        print("\nWarning: 'assigned_user' column not found in PISA file (after cleaning). No filter applied.")
        df_pisa_filtered = df_pisa.copy()

    all_consolidated_rows = []
    today_date = datetime.now()
    today_date_formatted = today_date.strftime("%m/%d/%Y") # Format today's date for fixed entries

    # --- PISA Processing ---
    if 'barcode' not in df_pisa_filtered.columns:
        print("Error: 'barcode' column not found in PISA file (after cleaning). Skipping PISA processing.")
    else:
        df_pisa_filtered['barcode'] = df_pisa_filtered['barcode'].astype(str)
        for index, row in df_pisa_filtered.iterrows():
            new_row = {
                'Barcode': row['barcode'],
                'Company code': row.get('company_code'),
                'Vendor number': row.get('vendor_number'),
                'Received Date': row.get('received_date'),
                'Completion Date': None, 'Status': None , 'Today': today_date, 'Channel': 'PISA',
                'Vendor Name': row.get('vendor_name'),
                'Re-Open Date': None, 'Allocation Date': None,
                'Requester': None, 'Clarification Date': None, 'Aging': None, 'Remarks': None,
                'Region': None,
                'Processor': None, 'Category': None
            }
            all_consolidated_rows.append(new_row)
        print(f"Collected {len(df_pisa_filtered)} rows from PISA.")

    # --- ESM Processing ---
    if 'barcode' not in df_esm.columns:
        print("Error: 'barcode' column not found in ESM file (after cleaning). Skipping ESM processing.")
    else:
        df_esm['barcode'] = df_esm['barcode'].astype(str)
        for index, row in df_esm.iterrows():
            new_row = {
                'Barcode': row['barcode'],
                'Received Date': row.get('received_date'),
                'Status': row.get('state'),
                'Requester': row.get('opened_by'),
                'Completion Date': row.get('closed') if pd.notna(row.get('closed')) else None,
                'Re-Open Date': row.get('updated') if (row.get('state') or '').lower() == 'reopened' else None,
                'Today': today_date, 'Remarks': row.get('short_description'),
                'Channel': 'ESM',
                'Company code': None,'Vendor Name': None,
                'Vendor number': None, 'Allocation Date': None,
                'Clarification Date': None, 'Aging': None,
                'Region': None,
                'Processor': None,
                'Category': None
            }
            all_consolidated_rows.append(new_row)
        print(f"Collected {len(df_esm)} rows from ESM.")

    # --- PM7 Processing ---
    if 'barcode' not in df_pm7.columns:
        print("Error: 'barcode' column not found in PM7 file (after cleaning). Skipping PM7 processing.")
    else:
        df_pm7['barcode'] = df_pm7['barcode'].astype(str)

        for index, row in df_pm7.iterrows():
            new_row = {
                'Barcode': row['barcode'],
                'Vendor Name': row.get('vendor_name'),
                'Vendor number': row.get('vendor_number'),
                'Received Date': row.get('received_date'),
                'Status': row.get('task'),
                'Today': today_date,
                'Channel': 'PM7',
                'Company code': row.get('company_code'),
                'Re-Open Date': None,
                'Allocation Date': None, 'Completion Date': None, 'Requester': None,
                'Clarification Date': None, 'Aging': None, 'Remarks': None,
                'Region': None,
                'Processor': None, 'Category': None
            }
            all_consolidated_rows.append(new_row)
        print(f"Collected {len(df_pm7)} rows from PM7.")

    # --- Workon P71 Processing (Optional) ---
    if not df_workon_cleaned.empty: # Process only if the Workon DataFrame is not empty
        if 'key' not in df_workon_cleaned.columns:
            print("Error: 'key' column not found in Workon file (after cleaning). Skipping Workon processing.")
        else:
            df_workon_cleaned['key'] = df_workon_cleaned['key'].astype(str)
            for index, row in df_workon_cleaned.iterrows():
                new_row = {
                    'Barcode': row['key'],
                    'Processor': 'Jayapal', # Fixed string
                    'Channel': 'Workon',    # Fixed string
                    'Category': row.get('action'),
                    'Company code': row.get('company_code'),
                    'Region': row.get('country'),
                    'Vendor number': row.get('vendor_number'),
                    'Vendor Name': row.get('name'),
                    'Status': row.get('status'),
                    'Received Date': row.get('updated'),
                    'Re-Open Date': None, # Blank
                    'Allocation Date': today_date_formatted, # Today's date
                    'Clarification Date': None, # Blank
                    'Completion Date': None, # Blank
                    'Requester': row.get('applicant'),
                    'Remarks': row.get('summary'),
                    'Aging': None, # Blank
                    'Today': today_date # Today's date (will be formatted later)
                }
                all_consolidated_rows.append(new_row)
            print(f"Collected {len(df_workon_cleaned)} rows from Workon.")
    else:
        print("Workon file not provided or is empty. Skipping Workon processing.")

    # --- RGPA Processing ---
    # Check if df_rgpa_cleaned is empty. RGPA is a required file, but it could still be empty if the file itself was blank.
    if df_rgpa_cleaned.empty:
        print("Warning: RGPA file is empty. Skipping RGPA processing.")
    elif 'key' not in df_rgpa_cleaned.columns:
        print("Error: 'key' column not found in RGPA file (after cleaning). Skipping RGPA processing.")
    else:
        # Apply filter for 'Current Assignee'
        original_rgpa_count = len(df_rgpa_cleaned)
        df_rgpa_filtered = df_rgpa_cleaned.copy() # Start with a copy of the cleaned DF
        if 'current_assignee' in df_rgpa_cleaned.columns:
            # Create a boolean mask for filtering
            filter_mask = df_rgpa_cleaned['current_assignee'].astype(str).str.contains("VMD GS OSP-NA (GS/OMD-APAC)", na=False)
            df_rgpa_filtered = df_rgpa_cleaned[filter_mask].copy() # Apply the mask to filter
            print(f"\nRGPA file filtered. Original records: {original_rgpa_count}, Records after filter: {len(df_rgpa_filtered)}")
        else:
            print("Warning: 'current_assignee' column not found in RGPA file (after cleaning). No filter applied for RGPA.")
            # If 'current_assignee' column is missing, df_rgpa_filtered remains unfiltered

        df_rgpa_filtered['key'] = df_rgpa_filtered['key'].astype(str)
        for index, row in df_rgpa_filtered.iterrows():
            new_row = {
                'Barcode': row['key'],
                'Processor': 'Divya', # Fixed string
                'Channel': 'RGPA',    # Fixed string
                'Category': None,     # Blank as per requirement
                'Company code': row.get('company_code'),
                'Region': None,       # Will be mapped later from external file
                'Vendor number': None,# Blank as per requirement
                'Vendor Name': None,  # Blank as per requirement
                'Status': None,       # Blank as per requirement
                'Received Date': row.get('updated'),
                'Re-Open Date': None, # Blank
                'Allocation Date': today_date_formatted, # Today's date
                'Clarification Date': None, # Blank
                'Completion Date': None, # Blank
                'Requester': None,    # Blank as per requirement
                'Remarks': row.get('summary'),
                'Aging': None,        # Blank
                'Today': today_date   # Today's date (will be formatted later)
            }
            all_consolidated_rows.append(new_row)
        print(f"Collected {len(df_rgpa_filtered)} rows from RGPA.")

    # If no data was collected from any source, return an error
    if not all_consolidated_rows:
        return False, "No data collected for consolidation from any source files."

    df_consolidated = pd.DataFrame(all_consolidated_rows)

    # Ensure all CONSOLIDATED_OUTPUT_COLUMNS are present in the DataFrame
    for col in CONSOLIDATED_OUTPUT_COLUMNS:
        if col not in df_consolidated.columns:
            df_consolidated[col] = None

    # Reorder columns to match the standard output format
    df_consolidated = df_consolidated[CONSOLIDATED_OUTPUT_COLUMNS]

    # Process date columns to MM/DD/YYYY format
    date_cols_to_process = ['Received Date', 'Re-Open Date', 'Allocation Date', 'Completion Date', 'Clarification Date', 'Today']
    for col in df_consolidated.columns:
        if col in date_cols_to_process:
            df_consolidated[col] = format_date_to_mdyyyy(df_consolidated[col])
        else:
            # Fill NaN/NaT in object columns with empty string, convert specific ID columns to string
            if df_consolidated[col].dtype == 'object':
                df_consolidated[col] = df_consolidated[col].fillna('')
            elif col in ['Barcode', 'Company code', 'Vendor number']:
                df_consolidated[col] = df_consolidated[col].astype(str).replace('nan', '')

    # Save the consolidated DataFrame to an Excel file
    try:
        df_consolidated.to_excel(consolidated_output_file_path, index=False)
        print(f"Consolidated file saved to: {consolidated_output_file_path}")
    except Exception as e:
        return False, f"Error saving consolidated file: {e}"
    print("--- Consolidated Data Process Complete ---")
    return True, df_consolidated

def process_central_file_step2_update_existing(consolidated_df, central_file_input_path):
    """
    Step 2: Updates status of *existing* central file records based on consolidated data.
    """
    print(f"\n--- Starting Central File Status Processing (Step 2: Update Existing Barcodes) ---")

    try:
        # Read the central file, specifying converters for ID-like columns to ensure they are read as strings
        converters = {'Barcode': str, 'Vendor number': str, 'Company code': str}
        df_central = pd.read_excel(central_file_input_path, converters=converters, keep_default_na=False)
        df_central_cleaned = clean_column_names(df_central.copy())

        print("Consolidated (DF) and Central (file) loaded successfully for Step 2!")
    except Exception as e:
        return False, f"Error loading Consolidated (DF) or Central (file) for processing (Step 2): {e}"

    # Basic checks for essential columns
    if 'Barcode' not in consolidated_df.columns:
        return False, "Error: 'Barcode' column not found in the consolidated file. Cannot proceed with central file processing (Step 2)."
    if 'barcode' not in df_central_cleaned.columns or 'status' not in df_central_cleaned.columns:
        return False, "Error: 'barcode' or 'status' column not found in the central file after cleaning. Cannot update status (Step 2)."

    # Ensure Barcode columns are string type for accurate comparison
    consolidated_df['Barcode'] = consolidated_df['Barcode'].astype(str)
    df_central_cleaned['barcode'] = df_central_cleaned['barcode'].astype(str)

    # Create a temporary column for comparison in the apply function
    df_central_cleaned['Barcode_compare'] = df_central_cleaned['barcode']

    # Define which channels trigger status changes for existing central file records.
    # Workon and RGPA records should NOT cause these specific status transformations.
    channels_for_status_change = ['PISA', 'ESM', 'PM7']
    consolidated_barcodes_for_status_change_set = set(
        consolidated_df[consolidated_df['Channel'].isin(channels_for_status_change)]['Barcode'].unique()
    )
    print(f"Found {len(consolidated_barcodes_for_status_change_set)} unique barcodes from {', '.join(channels_for_status_change)} in consolidated file for Step 2 status updates.")

    def transform_status_if_barcode_exists(row):
        central_barcode = str(row['Barcode_compare'])
        original_central_status = row['status']

        # Only apply status transformations if the central_barcode is found in
        # the consolidated PISA, ESM, or PM7 data.
        if central_barcode in consolidated_barcodes_for_status_change_set:
            # If the original status is blank/NaN, don't change it based on this logic
            if pd.isna(original_central_status) or \
               (isinstance(original_central_status, str) and original_central_status.strip().lower() in ['', 'n/a', 'na', 'none']):
                return original_central_status

            # Apply specific status transformations
            status_str = str(original_central_status).strip().lower()
            if status_str == 'new':
                return 'Untouched'
            elif status_str == 'completed':
                return 'Reopen'
            elif status_str == 'n/a': # This might be redundant if handled by the previous condition
                return 'New'
            else:
                return original_central_status
        else:
            return original_central_status

    df_central_cleaned['status'] = df_central_cleaned.apply(transform_status_if_barcode_exists, axis=1)
    df_central_cleaned = df_central_cleaned.drop(columns=['Barcode_compare']) # Remove the temporary column

    print(f"Updated 'status' column in central file for Step 2 for {len(df_central_cleaned)} records.")

    # Remap cleaned column names back to standard output names and format dates/IDs
    try:
        common_cols_map = {
            'barcode': 'Barcode', 'channel': 'Channel', 'company_code': 'Company code',
            'vendor_name': 'Vendor Name', 'vendor_number': 'Vendor number',
            'received_date': 'Received Date', 're_open_date': 'Re-Open Date',
            'allocation_date': 'Allocation Date', 'completion_date': 'Completion Date',
            'requester': 'Requester', 'clarification_date': 'Clarification Date',
            'aging': 'Aging', 'today': 'Today', 'status': 'Status', 'remarks': 'Remarks',
            'region': 'Region', 'processor': 'Processor', 'category': 'Category'
        }

        # Rename columns that exist in the cleaned central file
        cols_to_rename = {k: v for k, v in common_cols_map.items() if k in df_central_cleaned.columns}
        df_central_cleaned.rename(columns=cols_to_rename, inplace=True)

        # Format date columns and handle string/numeric types for other columns
        date_cols_in_central_file = [
            'Received Date', 'Re-Open Date', 'Allocation Date',
            'Completion Date', 'Clarification Date', 'Today'
        ]
        for col in df_central_cleaned.columns:
            if col in date_cols_in_central_file:
                df_central_cleaned[col] = format_date_to_mdyyyy(df_central_cleaned[col])
            elif df_central_cleaned[col].dtype == 'object':
                df_central_cleaned[col] = df_central_cleaned[col].fillna('')
            elif col in ['Barcode', 'Vendor number']:
                df_central_cleaned[col] = df_central_cleaned[col].astype(str).replace('nan', '')
            if col == 'Company code':
                 df_central_cleaned[col] = df_central_cleaned[col].astype(str).replace('nan', '')

        # Add any missing CONSOLIDATED_OUTPUT_COLUMNS
        for col in CONSOLIDATED_OUTPUT_COLUMNS:
            if col not in df_central_cleaned.columns:
                df_central_cleaned[col] = None

    except Exception as e:
        return False, f"Error processing central file (Step 2): {e}"
    print(f"--- Central File Status Processing (Step 2) Complete ---")
    return True, df_central_cleaned

# ### NEW CHANGE: Added df_rgpa_original parameter
def process_central_file_step3_final_merge_and_needs_review(consolidated_df, updated_existing_central_df, final_central_output_file_path, df_pisa_original, df_esm_original, df_pm7_original, df_workon_original, df_rgpa_original, region_mapping_df):
    """
    Step 3: Handles barcodes present only in consolidated (adds them as new)
            and barcodes present only in central (marks them as 'Needs Review' if not 'Completed').
            Also performs region mapping and final column reordering.
    """
    print(f"\n--- Starting Central File Status Processing (Step 3: Final Merge & Needs Review) ---")

    # Clean column names for original DataFrames for lookup purposes
    df_pisa_lookup = clean_column_names(df_pisa_original.copy())
    df_esm_lookup = clean_column_names(df_esm_original.copy())
    df_pm7_lookup = clean_column_names(df_pm7_original.copy())
    
    # Handle optional Workon and required RGPA DataFrames for lookup
    df_workon_lookup = clean_column_names(df_workon_original.copy()) if df_workon_original is not None and not df_workon_original.empty else pd.DataFrame()
    df_rgpa_lookup = clean_column_names(df_rgpa_original.copy())

    # Create indexed DataFrames for faster lookups based on 'barcode' or 'key'
    df_pisa_indexed = pd.DataFrame()
    if 'barcode' in df_pisa_lookup.columns:
        df_pisa_lookup['barcode'] = df_pisa_lookup['barcode'].astype(str)
        df_pisa_indexed = df_pisa_lookup.set_index('barcode')
        print(f"PISA lookup indexed by 'barcode'.")
    else:
        print("Warning: 'barcode' column not found in cleaned PISA lookup. Cannot perform PISA lookups.")

    df_esm_indexed = pd.DataFrame()
    if 'barcode' in df_esm_lookup.columns:
        df_esm_lookup['barcode'] = df_esm_lookup['barcode'].astype(str)
        df_esm_indexed = df_esm_lookup.set_index('barcode')
        print(f"ESM lookup indexed by 'barcode'.")
    else:
        print("Warning: 'barcode' column not found in cleaned ESM lookup. Cannot perform ESM lookups.")

    df_pm7_indexed = pd.DataFrame()
    if 'barcode' in df_pm7_lookup.columns:
        df_pm7_lookup['barcode'] = df_pm7_lookup['barcode'].astype(str)
        df_pm7_indexed = df_pm7_lookup.set_index('barcode')
        print(f"PM7 lookup indexed by 'barcode'.")
    else:
        print("Warning: 'barcode' column not found in cleaned PM7 lookup. Cannot perform PM7 lookups.")

    # Workon Lookup
    df_workon_indexed = pd.DataFrame()
    if not df_workon_lookup.empty: # Only try to index if Workon data exists
        if 'key' in df_workon_lookup.columns:
            df_workon_lookup['key'] = df_workon_lookup['key'].astype(str)
            df_workon_indexed = df_workon_lookup.set_index('key')
            print(f"Workon lookup indexed by 'key'.")
        else:
            print("Warning: 'key' column not found in cleaned Workon lookup. Cannot perform Workon lookups.")
    else:
        print("Workon lookup not created as file was not provided or empty.")

    # RGPA Lookup (includes pre-filtering by 'current_assignee')
    df_rgpa_indexed = pd.DataFrame()
    if 'key' in df_rgpa_lookup.columns:
        if 'current_assignee' in df_rgpa_lookup.columns:
            # Filter the lookup DataFrame by 'current_assignee' before indexing
            df_rgpa_lookup_filtered = df_rgpa_lookup[
                df_rgpa_lookup['current_assignee'].astype(str).str.contains("VMD GS OSP-NA (GS/OMD-APAC)", na=False)
            ].copy()
            df_rgpa_lookup_filtered['key'] = df_rgpa_lookup_filtered['key'].astype(str)
            df_rgpa_indexed = df_rgpa_lookup_filtered.set_index('key')
            print(f"RGPA lookup (filtered by assignee) indexed by 'key'.")
        else:
            print("Warning: 'current_assignee' column not found in RGPA lookup. RGPA lookup is not filtered by assignee.")
            df_rgpa_lookup['key'] = df_rgpa_lookup['key'].astype(str)
            df_rgpa_indexed = df_rgpa_lookup.set_index('key')
    else:
        print("Warning: 'key' column not found in cleaned RGPA lookup. Cannot perform RGPA lookups.")

    # Check for essential columns in input DataFrames
    if 'Barcode' not in consolidated_df.columns:
        return False, "Error: 'Barcode' column not found in the consolidated file. Cannot proceed with final central file processing (Step 3)."
    if 'Barcode' not in updated_existing_central_df.columns or 'Status' not in updated_existing_central_df.columns:
        return False, "Error: 'Barcode' or 'Status' column not found in the updated central file. Cannot update status (Step 3)."

    # Identify barcodes present only in consolidated (new records) and only in central ('Needs Review' candidates)
    consolidated_barcodes_set = set(consolidated_df['Barcode'].unique())
    central_barcodes_set = set(updated_existing_central_df['Barcode'].unique())

    barcodes_to_add = consolidated_barcodes_set - central_barcodes_set
    print(f"Found {len(barcodes_to_add)} new barcodes in consolidated file to add to central.")

    df_new_records_from_consolidated = consolidated_df[consolidated_df['Barcode'].isin(barcodes_to_add)].copy()

    all_new_central_rows_data = []
    today_date_formatted = datetime.now().strftime("%m/%d/%Y") 

    for index, row_consolidated in df_new_records_from_consolidated.iterrows():
        barcode = row_consolidated['Barcode']
        channel = row_consolidated['Channel']

        # Initialize fields from the already-processed `row_consolidated`.
        # This approach assumes `consolidate_data_process` has done the primary mapping.
        # This section serves to double-check or ensure specific fields are set correctly
        # for these *new* records, especially fixed values or those that rely on original DFs
        # for a more complete set of attributes.
        vendor_name = row_consolidated.get('Vendor Name')
        vendor_number = row_consolidated.get('Vendor number')
        company_code = row_consolidated.get('Company code')
        received_date = row_consolidated.get('Received Date')
        processor = row_consolidated.get('Processor')
        category = row_consolidated.get('Category')
        requester = row_consolidated.get('Requester')
        remarks = row_consolidated.get('Remarks')
        status = row_consolidated.get('Status')
        region = row_consolidated.get('Region')


        # The following 'elif' blocks allow for fine-tuning based on the original source
        # if the consolidated_df's initial population was incomplete or needed refinement
        # for 'new' records. For Workon and RGPA, fixed values and blank fields are already
        # set during `consolidate_data_process`.

        # --- PISA Lookup (for new records from PISA) ---
        if channel == 'PISA' and not df_pisa_indexed.empty and barcode in df_pisa_indexed.index:
            pisa_row = df_pisa_indexed.loc[barcode]
            # Override with original PISA values if consolidated didn't fully capture or needed to ensure fresh data
            if 'vendor_name' in pisa_row.index and pd.notna(pisa_row['vendor_name']):
                vendor_name = pisa_row['vendor_name']
            if 'vendor_number' in pisa_row.index and pd.notna(pisa_row['vendor_number']):
                vendor_number = pisa_row['vendor_number']
            if 'company_code' in pisa_row.index and pd.notna(pisa_row['company_code']):
                company_code = pisa_row['company_code']
            if 'received_date' in pisa_row.index and pd.notna(pisa_row['received_date']):
                received_date = pisa_row['received_date']

        # --- ESM Lookup (for new records from ESM) ---
        elif channel == 'ESM' and not df_esm_indexed.empty and barcode in df_esm_indexed.index:
            esm_row = df_esm_indexed.loc[barcode]
            if 'company_code' in esm_row.index and pd.notna(esm_row['company_code']):
                company_code = esm_row['company_code']
            if 'subcategory' in esm_row.index and pd.notna(esm_row['subcategory']):
                category = esm_row['subcategory']
            if 'vendor_name' in esm_row.index and pd.notna(esm_row['vendor_name']):
                vendor_name = esm_row['vendor_name']
            if 'vendor_number' in esm_row.index and pd.notna(esm_row['vendor_number']):
                vendor_number = esm_row['vendor_number']
            if 'received_date' in esm_row.index and pd.notna(esm_row['received_date']):
                received_date = esm_row['received_date']
            if 'opened_by' in esm_row.index and pd.notna(esm_row['opened_by']):
                requester = esm_row['opened_by']
            if 'state' in esm_row.index and pd.notna(esm_row['state']):
                status = esm_row['state']
            if 'short_description' in esm_row.index and pd.notna(esm_row['short_description']):
                remarks = esm_row['short_description']

        # --- PM7 Lookup (for new records from PM7) ---
        elif channel == 'PM7' and not df_pm7_indexed.empty and barcode in df_pm7_indexed.index:
            pm7_row = df_pm7_indexed.loc[barcode]
            if 'vendor_name' in pm7_row.index and pd.notna(pm7_row['vendor_name']):
                vendor_name = pm7_row['vendor_name']
            if 'vendor_number' in pm7_row.index and pd.notna(pm7_row['vendor_number']):
                vendor_number = pm7_row['vendor_number']
            if 'company_code' in pm7_row.index and pd.notna(pm7_row['company_code']):
                company_code = pm7_row['company_code']
            if 'received_date' in pm7_row.index and pd.notna(pm7_row['received_date']):
                received_date = pm7_row['received_date']
            if 'task' in pm7_row.index and pd.notna(pm7_row['task']):
                status = pm7_row['task']

        # --- Workon Lookup (for new records from Workon) ---
        elif channel == 'Workon' and not df_workon_indexed.empty and barcode in df_workon_indexed.index:
            workon_row = df_workon_indexed.loc[barcode]
            # Ensure these are pulled from the lookup for consistency
            vendor_name = workon_row.get('name')
            vendor_number = workon_row.get('vendor_number')
            company_code = workon_row.get('company_code')
            received_date = workon_row.get('updated')
            processor = 'Jayapal' 
            channel = 'Workon'    
            category = workon_row.get('action')
            region = workon_row.get('country')
            status = workon_row.get('status')
            requester = workon_row.get('applicant')
            remarks = workon_row.get('summary')
        
        # --- RGPA Lookup (for new records from RGPA) ---
        elif channel == 'RGPA' and not df_rgpa_indexed.empty and barcode in df_rgpa_indexed.index:
            rgpa_row = df_rgpa_indexed.loc[barcode]
            processor = 'Divya' 
            channel = 'RGPA'    
            company_code = rgpa_row.get('company_code')
            received_date = rgpa_row.get('updated')
            remarks = rgpa_row.get('summary')
            # Other RGPA fields are designed to be blank or mapped by region logic


        new_central_row_data = {
            'Barcode': barcode,
            'Processor': processor if processor is not None else '',
            'Channel': channel,
            'Category': category if category is not None else '',
            'Company code': company_code if company_code is not None else '',
            'Region': region if region is not None else '', # Preserve any region already set (e.g. from Workon)
            'Vendor number': vendor_number if vendor_number is not None else '',
            'Vendor Name': vendor_name if vendor_name is not None else '',
            'Status': status if status is not None else 'New', # Default to New if not set by source
            'Received Date': received_date, 
            'Re-Open Date': None,
            'Allocation Date': today_date_formatted, # Always today for new records
            'Clarification Date': None,
            'Completion Date': None,
            'Requester': requester if requester is not None else '',
            'Remarks': remarks if remarks is not None else '',
            'Aging': None,
            'Today': today_date_formatted
        }

        all_new_central_rows_data.append(new_central_row_data)

    # Create a DataFrame from the new central rows
    if all_new_central_rows_data:
        df_new_central_rows = pd.DataFrame(all_new_central_rows_data)
        # Ensure all standard output columns are present
        for col in CONSOLIDATED_OUTPUT_COLUMNS:
            if col not in df_new_central_rows.columns:
                df_new_central_rows[col] = None
        df_new_central_rows = df_new_central_rows[CONSOLIDATED_OUTPUT_COLUMNS]
    else:
        df_new_central_rows = pd.DataFrame(columns=CONSOLIDATED_OUTPUT_COLUMNS)

    # Fill NaN/NaT in new rows for object/ID columns
    for col in df_new_central_rows.columns:
        if df_new_central_rows[col].dtype == 'object':
            df_new_central_rows[col] = df_new_central_rows[col].fillna('')
        elif col in ['Barcode', 'Company code', 'Vendor number']:
            df_new_central_rows[col] = df_new_central_rows[col].astype(str).replace('nan', '')

    # Identify barcodes in central that are not in *any* consolidated source (Needs Review)
    barcodes_for_needs_review = central_barcodes_set - consolidated_barcodes_set
    print(f"Found {len(barcodes_for_needs_review)} barcodes in central not in any consolidated source.")

    df_final_central = updated_existing_central_df.copy()

    # Apply 'Needs Review' status if barcode is in `barcodes_for_needs_review` and status is not 'Completed'
    needs_review_barcode_mask = df_final_central['Barcode'].isin(barcodes_for_needs_review)
    is_not_completed_status_mask = ~df_final_central['Status'].astype(str).str.strip().str.lower().eq('completed')
    final_needs_review_condition = needs_review_barcode_mask & is_not_completed_status_mask

    df_final_central.loc[final_needs_review_condition, 'Status'] = 'Needs Review'
    print(f"Updated {final_needs_review_condition.sum()} records to 'Needs Review' where status was not 'Completed'.")

    # Ensure all standard output columns are present and ordered in the central DF
    for col in CONSOLIDATED_OUTPUT_COLUMNS:
        if col not in df_final_central.columns:
            df_final_central[col] = None
    df_final_central = df_final_central[CONSOLIDATED_OUTPUT_COLUMNS]

    # Concatenate the existing and new records
    df_final_central = pd.concat([df_final_central, df_new_central_rows], ignore_index=True)

    # --- Handle blank Company Code for PM7 channel ---
    print("\n--- Applying PM7 Company Code population logic ---")
    if 'Channel' in df_final_central.columns and 'Company code' in df_final_central.columns and 'Barcode' in df_final_central.columns:
        pm7_blank_cc_mask = (df_final_central['Channel'] == 'PM7') & \
                            (df_final_central['Company code'].astype(str).replace('nan', '').str.strip() == '')

        df_final_central.loc[pm7_blank_cc_mask, 'Company code'] = \
            df_final_central.loc[pm7_blank_cc_mask, 'Barcode'].astype(str).str[:4]

        print(f"Populated Company Code for {pm7_blank_cc_mask.sum()} PM7 records based on Barcode.")
    else:
        print("Warning: 'Channel', 'Company code', or 'Barcode' columns missing. Skipping PM7 Company Code population logic.")

    # --- Apply Region Mapping ---
    print("\n--- Applying Region Mapping ---")
    if region_mapping_df is None or region_mapping_df.empty:
        print("Warning: Region mapping file not provided or is empty. Region column will not be populated by external mapping.")
        if 'Region' not in df_final_central.columns:
            df_final_central['Region'] = ''
        df_final_central['Region'] = df_final_central['Region'].fillna('')
    else:
        region_mapping_df = clean_column_names(region_mapping_df.copy())
        if 'r3_coco' not in region_mapping_df.columns or 'region' not in region_mapping_df.columns:
            print("Error: Region mapping file must contain 'r3_coco' and 'region' columns after cleaning. Skipping region mapping.")
            if 'Region' not in df_final_central.columns:
                df_final_central['Region'] = ''
            df_final_central['Region'] = df_final_central['Region'].fillna('')
        else:
            region_map = {}
            for idx, row in region_mapping_df.iterrows():
                coco_key = str(row['r3_coco']).strip().upper()
                if coco_key:
                    region_map[coco_key[:4]] = str(row['region']).strip()

            print(f"Loaded {len(region_map)} unique R/3 CoCo -> Region mappings.")

            if 'Company code' in df_final_central.columns:
                df_final_central['Company code_lookup'] = df_final_central['Company code'].astype(str).str.strip().str.upper().str[:4]
                mapped_regions = df_final_central['Company code_lookup'].map(region_map)

                # NEW LOGIC FOR REGION MAPPING:
                # Fill 'Region' column only where it is currently empty or NaN, using the mapped regions.
                # Otherwise, keep the existing 'Region' value (e.g., from Workon 'country' field).
                mask_blank_or_na_region = df_final_central['Region'].replace('', pd.NA).isna()
                df_final_central.loc[mask_blank_or_na_region, 'Region'] = mapped_regions.loc[mask_blank_or_na_region].fillna(df_final_central.loc[mask_blank_or_na_region, 'Region'])
                
                df_final_central = df_final_central.drop(columns=['Company code_lookup'])
                df_final_central['Region'] = df_final_central['Region'].fillna('') # Ensure no NaNs remain after mapping
                print("Region mapping applied successfully. Existing regions prioritized.")
            else:
                print("Warning: 'Company code' column not found in final central DataFrame. Cannot apply region mapping.")
                if 'Region' not in df_final_central.columns:
                    df_final_central['Region'] = ''
                df_final_central['Region'] = df_final_central['Region'].fillna('')

    # Final formatting for date columns and string/numeric ID columns
    date_cols_in_central_file = [
        'Received Date', 'Re-Open Date', 'Allocation Date',
        'Completion Date', 'Clarification Date', 'Today'
    ]
    for col in df_final_central.columns:
        if col in date_cols_in_central_file:
            df_final_central[col] = format_date_to_mdyyyy(df_final_central[col])
        elif df_final_central[col].dtype == 'object':
            df_final_central[col] = df_final_central[col].fillna('')
        elif col in ['Barcode', 'Vendor number']:
            df_final_central[col] = df_final_central[col].astype(str).replace('nan', '')

    # Ensure all CONSOLIDATED_OUTPUT_COLUMNS are present and in final order
    for col in CONSOLIDATED_OUTPUT_COLUMNS:
        if col not in df_final_central.columns:
            df_final_central[col] = ''
    df_final_central = df_final_central[CONSOLIDATED_OUTPUT_COLUMNS]

    # Save the final central file
    try:
        df_final_central.to_excel(final_central_output_file_path, index=False)
        print(f"Final central file (after Step 3) saved to: {final_central_output_file_path}")
        print(f"Total rows in final central file (after Step 3): {len(df_final_central)}")
    except Exception as e:
        return False, f"Error saving final central file (after Step 3): {e}"
    print(f"--- Central File Status Processing (Step 3) Complete ---")
    return True, "Central file processing (Step 3) successful"


# --- Flask Routes ---

@app.route('/', methods=['GET'])
def index():
    """Renders the main upload form page."""
    return render_template('index.html')

@app.route('/process', methods=['POST'])
def process_files():
    """
    Handles file uploads, performs data consolidation and central file updates,
    and provides a download link for the final processed file.
    """
    # Create a temporary directory for uploaded files and processing outputs
    temp_dir = tempfile.mkdtemp(dir=tempfile.gettempdir())

    # Clear previous session data related to file paths
    session.pop('consolidated_output_path', None)
    session.pop('central_output_path', None)
    session.pop('temp_dir', None)

    # Store the temporary directory path in the session for cleanup
    session['temp_dir'] = temp_dir

    # Path to the external region mapping file
    REGION_MAPPING_FILE_PATH = os.path.join(BASE_DIR, '..', 'company_code_region_mapping.xlsx')

    try:
        uploaded_files = {}
        
        # --- Handle required files ---
        required_file_keys = ['pisa_file', 'esm_file', 'pm7_file', 'rgpa_file', 'central_file']
        for key in required_file_keys:
            if key not in request.files or request.files[key].filename == '':
                flash(f'Missing required file: "{key}". Please upload all required files.', 'error')
                return redirect(url_for('index'))
            file = request.files[key]
            if file and file.filename.lower().endswith('.xlsx'):
                filename = secure_filename(file.filename)
                file_path = os.path.join(temp_dir, filename)
                file.save(file_path)
                uploaded_files[key] = file_path
                flash(f'File "{filename}" uploaded successfully.', 'info')
            else:
                flash(f'Invalid file type for "{key}". Please upload an .xlsx file.', 'error')
                return redirect(url_for('index'))
        
        # --- Handle optional files ---
        optional_file_keys = ['workon_file']
        for key in optional_file_keys:
            if key in request.files and request.files[key].filename != '': # Check if file was provided
                file = request.files[key]
                if file.filename.lower().endswith('.xlsx'):
                    filename = secure_filename(file.filename)
                    file_path = os.path.join(temp_dir, filename)
                    file.save(file_path)
                    uploaded_files[key] = file_path
                    flash(f'Optional file "{filename}" uploaded successfully.', 'info')
                else:
                    flash(f'Invalid file type for optional file "{key}". It must be an .xlsx file.', 'warning')
                    uploaded_files[key] = None # Indicate it was not successfully uploaded
            else:
                flash(f'Optional file "{key}" not provided. Continuing without it.', 'info')
                uploaded_files[key] = None # Explicitly set to None if not provided

        # Extract file paths from the uploaded_files dictionary
        pisa_file_path = uploaded_files['pisa_file']
        esm_file_path = uploaded_files['esm_file']
        pm7_file_path = uploaded_files['pm7_file']
        workon_file_path = uploaded_files['workon_file'] # This can be None
        rgpa_file_path = uploaded_files['rgpa_file']
        initial_central_file_input_path = uploaded_files['central_file']

        # Initialize DataFrames to None or empty. df_region_mapping must be initialized.
        df_pisa_original = None
        df_esm_original = None
        df_pm7_original = None
        df_workon_original = None 
        df_rgpa_original = None 
        df_region_mapping = pd.DataFrame() # Initialize df_region_mapping to an empty DataFrame

        try:
            # Read all required Excel files into pandas DataFrames
            df_pisa_original = pd.read_excel(pisa_file_path)
            df_esm_original = pd.read_excel(esm_file_path)
            df_pm7_original = pd.read_excel(pm7_file_path)
            df_rgpa_original = pd.read_excel(rgpa_file_path) 
            
            # Conditionally read Workon file if it was uploaded
            if workon_file_path:
                df_workon_original = pd.read_excel(workon_file_path)
            else:
                print("Workon file not read as it was not provided.")
                df_workon_original = pd.DataFrame() # Provide an empty DataFrame if not uploaded

            # Read the region mapping file
            if os.path.exists(REGION_MAPPING_FILE_PATH):
                df_region_mapping = pd.read_excel(REGION_MAPPING_FILE_PATH)
                print(f"Successfully loaded region mapping file from: {REGION_MAPPING_FILE_PATH}")
            else:
                flash(f"Error: Region mapping file not found at {REGION_MAPPING_FILE_PATH}. Region column will be empty.", 'warning')
                # df_region_mapping is already initialized to an empty DataFrame, so no further action needed here.
        
        except Exception as e:
            # Catch errors during file reading and flash an appropriate message
            flash(f"Error loading one or more input Excel files or the region mapping file: {e}. Please ensure all files are valid .xlsx formats and the mapping file exists.", 'error')
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
            session.pop('temp_dir', None)
            return redirect(url_for('index'))

        today_str = datetime.now().strftime("%d_%m_%Y_%H%M%S")

        # --- Step 1: Consolidate Data ---
        consolidated_output_filename = f'ConsolidatedData_{today_str}.xlsx'
        consolidated_output_file_path = os.path.join(temp_dir, consolidated_output_filename)
        success, result = consolidate_data_process(
            df_pisa_original, df_esm_original, df_pm7_original, df_workon_original, df_rgpa_original, consolidated_output_file_path 
        )

        if not success:
            flash(f'Consolidation Error: {result}', 'error')
            if os.path.exists(temp_dir): shutil.rmtree(temp_dir)
            session.pop('temp_dir', None)
            return redirect(url_for('index'))
        df_consolidated = result
        flash('Data consolidation from the sources completed successfully!', 'success')
        session['consolidated_output_path'] = consolidated_output_file_path

        # --- Step 2: Update existing central file records based on consolidation ---
        success, result_df = process_central_file_step2_update_existing(
            df_consolidated, initial_central_file_input_path
        )
        if not success:
            flash(f'Central File Processing (Step 2) Error: {result_df}', 'error')
            if os.path.exists(temp_dir): shutil.rmtree(temp_dir)
            session.pop('temp_dir', None)
            return redirect(url_for('index'))
        df_central_updated_existing = result_df

        # --- Step 3: Final Merge (Add new barcodes, mark 'Needs Review', and apply Region Mapping) ---
        final_central_output_filename = f'CentralFile_FinalOutput_{today_str}.xlsx'
        final_central_output_file_path = os.path.join(temp_dir, final_central_output_filename)
        success, message = process_central_file_step3_final_merge_and_needs_review(
            df_consolidated, df_central_updated_existing, final_central_output_file_path,
            df_pisa_original, df_esm_original, df_pm7_original, df_workon_original, df_rgpa_original, df_region_mapping 
        )
        if not success:
            flash(f'Central File Processing (Step 3) Error: {message}', 'error')
            if os.path.exists(temp_dir): shutil.rmtree(temp_dir)
            session.pop('temp_dir', None)
            return redirect(url_for('index'))
        flash('Central file finalized successfully!', 'success')
        session['central_output_path'] = final_central_output_file_path

        # Render the index page again with the download link
        return render_template('index.html',
                                central_download_link=url_for('download_file', filename=os.path.basename(final_central_output_file_path))
                              )

    except Exception as e:
        # Catch any unhandled errors during the entire process
        flash(f'An unhandled error occurred during processing: {e}', 'error')
        import traceback
        traceback.print_exc() # Print traceback to console for debugging
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        session.pop('temp_dir', None)
        return redirect(url_for('index'))
    finally:
        # The finally block here isn't strictly necessary for cleanup
        # as cleanup_session is called via the redirect, but could be useful for other cases.
        pass


@app.route('/download/<filename>', methods=['GET'])
def download_file(filename):
    """Allows users to download the processed output files."""
    file_path_in_temp = None
    temp_dir = session.get('temp_dir')

    print(f"DEBUG: Download requested for filename: {filename}")
    print(f"DEBUG: Session temp_dir: {temp_dir}")
    print(f"DEBUG: Consolidated output path in session: {session.get('consolidated_output_path')}")
    print(f"DEBUG: Central output path in session: {session.get('central_output_path')}")

    if not temp_dir:
        print("DEBUG: temp_dir not found in session.")
        flash('File not found for download or session expired. Please re-run the process.', 'error')
        return redirect(url_for('index'))

    # Check if the requested filename matches any stored in the session
    consolidated_session_path = session.get('consolidated_output_path')
    central_session_path = session.get('central_output_path')

    if consolidated_session_path and os.path.basename(consolidated_session_path) == filename:
        file_path_in_temp = os.path.join(temp_dir, filename)
        print(f"DEBUG: Matched consolidated file. Reconstructed path: {file_path_in_temp}")
    elif central_session_path and os.path.basename(central_session_path) == filename:
        file_path_in_temp = os.path.join(temp_dir, filename)
        print(f"DEBUG: Matched final central file. Reconstructed path: {file_path_in_temp}")
    else:
        print(f"DEBUG: Filename '{filename}' did not match any known session output files.")

    if file_path_in_temp and os.path.exists(file_path_in_temp):
        print(f"DEBUG: File '{file_path_in_temp}' exists. Attempting to send.")
        try:
            # Send the file as an attachment
            response = send_file(
                file_path_in_temp,
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                as_attachment=True,
                download_name=filename
            )
            return response
        except Exception as e:
            print(f"ERROR: Exception while sending file '{file_path_in_temp}': {e}")
            flash(f'Error providing download: {e}. Please try again.', 'error')
            return redirect(url_for('index'))
    else:
        print(f"DEBUG: File '{filename}' not found for download or session data missing/expired. Full path attempted: {file_path_in_temp}")
        flash('File not found for download or session expired. Please re-run the process.', 'error')
        return redirect(url_for('index'))

@app.route('/cleanup_session', methods=['GET'])
def cleanup_session():
    """Cleans up the temporary directory created during processing."""
    temp_dir = session.get('temp_dir')
    if temp_dir and os.path.exists(temp_dir):
        try:
            shutil.rmtree(temp_dir) # Remove the temporary directory and its contents
            print(f"DEBUG: Cleaned up temporary directory: {temp_dir}")
            flash('Temporary files cleaned up.', 'info')
        except OSError as e:
            print(f"ERROR: Error removing temporary directory {temp_dir}: {e}")
            flash(f'Error cleaning up temporary files: {e}', 'error')
    # Clear session variables
    session.pop('temp_dir', None)
    session.pop('consolidated_output_path', None)
    session.pop('central_output_path', None)
    return redirect(url_for('index'))

if __name__ == '__main__':
    # Run the Flask development server
    app.run(debug=True)
