import os
import pandas as pd
from datetime import datetime
import warnings
import shutil
import tempfile
import re
from flask import Flask, request, render_template, redirect, url_for, send_file, flash, session
from werkzeug.utils import secure_filename

warnings.filterwarnings('ignore')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
template_dir = os.path.join(BASE_DIR, '..', 'templates') 

app = Flask(__name__, template_folder=template_dir)
app.secret_key = os.environ.get('FLASK_SECRET_KEY', 'default_secret_key_for_local_dev_only')

# --- Global Variables ---
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
    datetime_series = pd.to_datetime(date_series, errors='coerce')
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
        col = str(col).strip().lower()
        col = re.sub(r'\s+', '_', col)
        col = re.sub(r'[^a-z0-9_]', '', col)
        col = col.strip('_')
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

    df_pisa = clean_column_names(df_pisa.copy())
    df_esm = clean_column_names(df_esm.copy())
    df_pm7 = clean_column_names(df_pm7.copy())
    
    # Ensure Workon and RGPA are always DataFrames, even if empty or None
    df_workon_cleaned = clean_column_names(df_workon.copy()) if df_workon is not None and not df_workon.empty else pd.DataFrame()
    df_rgpa_cleaned = clean_column_names(df_rgpa.copy()) # RGPA is required, df_rgpa should not be None

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
    today_date_formatted = today_date.strftime("%m/%d/%Y") 

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
                'Completion Date': None,
                'Status': row.get('status'), # <--- Modified: Get Status from PISA data
                'Today': today_date,
                'Channel': 'PISA',
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
    # No changes here, already handles optionality.
    if not df_workon_cleaned.empty:
        if 'key' not in df_workon_cleaned.columns:
            print("Error: 'key' column not found in Workon file (after cleaning). Skipping Workon processing.")
        else:
            df_workon_cleaned['key'] = df_workon_cleaned['key'].astype(str)
            for index, row in df_workon_cleaned.iterrows():
                new_row = {
                    'Barcode': row['key'],
                    'Processor': 'Jayapal',
                    'Channel': 'Workon',    
                    'Category': row.get('action'),
                    'Company code': row.get('company_code'),
                    'Region': row.get('country'),
                    'Vendor number': row.get('vendor_number'),
                    'Vendor Name': row.get('name'),
                    'Status': row.get('status'),
                    'Received Date': row.get('updated'),
                    'Re-Open Date': None,
                    'Allocation Date': today_date_formatted,
                    'Clarification Date': None,
                    'Completion Date': None,
                    'Requester': row.get('applicant'),
                    'Remarks': row.get('summary'),
                    'Aging': None,
                    'Today': today_date
                }
                all_consolidated_rows.append(new_row)
            print(f"Collected {len(df_workon_cleaned)} rows from Workon.")
    else:
        print("Workon file not provided or is empty. Skipping Workon processing.")

    # --- RGPA Processing ---
    if df_rgpa_cleaned.empty:
        print("Warning: RGPA file is empty. Skipping RGPA processing.")
    elif 'key' not in df_rgpa_cleaned.columns:
        print("Error: 'key' column not found in RGPA file (after cleaning). Skipping RGPA processing.")
    elif 'current_assignee' not in df_rgpa_cleaned.columns: # Check for the filter column
        print("Warning: 'current_assignee' column not found in RGPA file (after cleaning). Skipping RGPA processing due to missing filter column.")
    else:
        original_rgpa_count = len(df_rgpa_cleaned)
        # Apply the filter based on 'current_assignee'
        filter_mask = df_rgpa_cleaned['current_assignee'].astype(str).str.contains("VMD GS OSP-NA (GS/OMD-APAC)", na=False)
        df_rgpa_filtered = df_rgpa_cleaned[filter_mask].copy()
        
        print(f"\nRGPA file filtered. Original records: {original_rgpa_count}, Records after filter: {len(df_rgpa_filtered)}")
            
        df_rgpa_filtered['key'] = df_rgpa_filtered['key'].astype(str)
        for index, row in df_rgpa_filtered.iterrows():
            new_row = {
                'Barcode': row['key'],
                'Processor': 'Divya',
                'Channel': 'RGPA',    
                'Category': None,     # As specified, blank
                'Company code': row.get('company_code'),
                'Region': None,       # Will be mapped later in Step 3
                'Vendor number': None, # As specified, blank
                'Vendor Name': None,  # As specified, blank
                'Status': None,       # As specified, blank
                'Received Date': row.get('updated'),
                'Re-Open Date': None, # As specified, blank
                'Allocation Date': today_date_formatted,
                'Clarification Date': None, # As specified, blank
                'Completion Date': None, # As specified, blank
                'Requester': None,    # As specified, blank
                'Remarks': row.get('summary'),
                'Aging': None,        # As specified, blank
                'Today': today_date   # Today's datetime object, will be formatted to m/d/yyyy
            }
            all_consolidated_rows.append(new_row)
        print(f"Collected {len(df_rgpa_filtered)} rows from RGPA.")

    if not all_consolidated_rows:
        return False, "No data collected for consolidation from any source files."

    df_consolidated = pd.DataFrame(all_consolidated_rows)

    for col in CONSOLIDATED_OUTPUT_COLUMNS:
        if col not in df_consolidated.columns:
            df_consolidated[col] = None

    df_consolidated = df_consolidated[CONSOLIDATED_OUTPUT_COLUMNS]

    date_cols_to_process = ['Received Date', 'Re-Open Date', 'Allocation Date', 'Completion Date', 'Clarification Date', 'Today']
    for col in df_consolidated.columns:
        if col in date_cols_to_process:
            df_consolidated[col] = format_date_to_mdyyyy(df_consolidated[col])
        else:
            if df_consolidated[col].dtype == 'object':
                df_consolidated[col] = df_consolidated[col].fillna('')
            elif col in ['Barcode', 'Company code', 'Vendor number']:
                df_consolidated[col] = df_consolidated[col].astype(str).replace('nan', '')

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
        converters = {'Barcode': str, 'Vendor number': str, 'Company code': str}
        df_central = pd.read_excel(central_file_input_path, converters=converters, keep_default_na=False)
        df_central_cleaned = clean_column_names(df_central.copy())

        print("Consolidated (DF) and Central (file) loaded successfully for Step 2!")
    except Exception as e:
        return False, f"Error loading Consolidated (DF) or Central (file) for processing (Step 2): {e}"

    if 'Barcode' not in consolidated_df.columns:
        return False, "Error: 'Barcode' column not found in the consolidated file. Cannot proceed with central file processing (Step 2)."
    if 'barcode' not in df_central_cleaned.columns or 'status' not in df_central_cleaned.columns:
        return False, "Error: 'barcode' or 'status' column not found in the central file after cleaning. Cannot update status (Step 2)."

    consolidated_df['Barcode'] = consolidated_df['Barcode'].astype(str)
    df_central_cleaned['barcode'] = df_central_cleaned['barcode'].astype(str)

    df_central_cleaned['Barcode_compare'] = df_central_cleaned['barcode']

    # Include RGPA in channels for status change if its status needs to affect central file's status
    # Based on your request, RGPA has 'Status': None for new records, so it won't trigger status changes here.
    # If RGPA had a status you wanted to react to, you'd add 'RGPA' here.
    channels_for_status_change = ['PISA', 'ESM', 'PM7'] 
    consolidated_barcodes_for_status_change_set = set(
        consolidated_df[consolidated_df['Channel'].isin(channels_for_status_change)]['Barcode'].unique()
    )
    print(f"Found {len(consolidated_barcodes_for_status_change_set)} unique barcodes from {', '.join(channels_for_status_change)} in consolidated file for Step 2 status updates.")

    def transform_status_if_barcode_exists(row):
        central_barcode = str(row['Barcode_compare'])
        original_central_status = row['status']

        if central_barcode in consolidated_barcodes_for_status_change_set:
            if pd.isna(original_central_status) or \
               (isinstance(original_central_status, str) and original_central_status.strip().lower() in ['', 'n/a', 'na', 'none']):
                return original_central_status

            status_str = str(original_central_status).strip().lower()
            if status_str == 'new':
                return 'Untouched'
            elif status_str == 'completed':
                return 'Reopen'
            elif status_str == 'n/a':
                return 'New'
            else:
                return original_central_status
        else:
            return original_central_status

    df_central_cleaned['status'] = df_central_cleaned.apply(transform_status_if_barcode_exists, axis=1)
    df_central_cleaned = df_central_cleaned.drop(columns=['Barcode_compare'])

    print(f"Updated 'status' column in central file for Step 2 for {len(df_central_cleaned)} records.")

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

        cols_to_rename = {k: v for k, v in common_cols_map.items() if k in df_central_cleaned.columns}
        df_central_cleaned.rename(columns=cols_to_rename, inplace=True)

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

        for col in CONSOLIDATED_OUTPUT_COLUMNS:
            if col not in df_central_cleaned.columns:
                df_central_cleaned[col] = None

    except Exception as e:
        return False, f"Error processing central file (Step 2): {e}"
    print(f"--- Central File Status Processing (Step 2) Complete ---")
    return True, df_central_cleaned

def process_central_file_step3_final_merge_and_needs_review(consolidated_df, updated_existing_central_df, final_central_output_file_path, df_pisa_original, df_esm_original, df_pm7_original, df_workon_original, df_rgpa_original, region_mapping_df):
    """
    Step 3: Handles barcodes present only in consolidated (adds them as new)
            and barcodes present only in central (marks them as 'Needs Review' if not 'Completed').
            Also performs region mapping and final column reordering.
    """
    print(f"\n--- Starting Central File Status Processing (Step 3: Final Merge & Needs Review) ---")

    df_pisa_lookup = clean_column_names(df_pisa_original.copy())
    df_esm_lookup = clean_column_names(df_esm_original.copy())
    df_pm7_lookup = clean_column_names(df_pm7_original.copy())
    
    df_workon_lookup = clean_column_names(df_workon_original.copy()) if df_workon_original is not None and not df_workon_original.empty else pd.DataFrame()
    df_rgpa_lookup = clean_column_names(df_rgpa_original.copy())

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

    df_workon_indexed = pd.DataFrame()
    if not df_workon_lookup.empty: 
        if 'key' in df_workon_lookup.columns:
            df_workon_lookup['key'] = df_workon_lookup['key'].astype(str)
            df_workon_indexed = df_workon_lookup.set_index('key')
            print(f"Workon lookup indexed by 'key'.")
        else:
            print("Warning: 'key' column not found in cleaned Workon lookup. Cannot perform Workon lookups.")
    else:
        print("Workon lookup not created as file was not provided or empty.")

    # RGPA lookup needs to be filtered here if it's used for populating new records
    df_rgpa_indexed = pd.DataFrame()
    if not df_rgpa_lookup.empty: 
        if 'key' in df_rgpa_lookup.columns:
            if 'current_assignee' in df_rgpa_lookup.columns:
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
    else:
        print("Warning: RGPA lookup was empty. No RGPA lookups will be performed for new records.")


    if 'Barcode' not in consolidated_df.columns:
        return False, "Error: 'Barcode' column not found in the consolidated file. Cannot proceed with final central file processing (Step 3)."
    if 'Barcode' not in updated_existing_central_df.columns or 'Status' not in updated_existing_central_df.columns:
        return False, "Error: 'Barcode' or 'Status' column not found in the updated central file. Cannot update status (Step 3)."

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

        # Initialize with values from consolidated_df (which might be None or blank for many)
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
        allocation_date = row_consolidated.get('Allocation Date')


        # Override with more specific source data if available and not already set by consolidated_df
        if channel == 'PISA' and not df_pisa_indexed.empty and barcode in df_pisa_indexed.index:
            pisa_row = df_pisa_indexed.loc[barcode]
            # Only update if current value is None/blank, or if original data is more specific
            vendor_name = pisa_row.get('vendor_name', vendor_name) if pd.notna(pisa_row.get('vendor_name')) else vendor_name
            vendor_number = pisa_row.get('vendor_number', vendor_number) if pd.notna(pisa_row.get('vendor_number')) else vendor_number
            company_code = pisa_row.get('company_code', company_code) if pd.notna(pisa_row.get('company_code')) else company_code
            received_date = pisa_row.get('received_date', received_date) if pd.notna(pisa_row.get('received_date')) else received_date
            status = pisa_row.get('status', status) if pd.notna(pisa_row.get('status')) else status # Capture PISA status here

        elif channel == 'ESM' and not df_esm_indexed.empty and barcode in df_esm_indexed.index:
            esm_row = df_esm_indexed.loc[barcode]
            company_code = esm_row.get('company_code', company_code) if pd.notna(esm_row.get('company_code')) else company_code
            category = esm_row.get('subcategory', category) if pd.notna(esm_row.get('subcategory')) else category
            vendor_name = esm_row.get('vendor_name', vendor_name) if pd.notna(esm_row.get('vendor_name')) else vendor_name
            vendor_number = esm_row.get('vendor_number', vendor_number) if pd.notna(esm_row.get('vendor_number')) else vendor_number
            received_date = esm_row.get('received_date', received_date) if pd.notna(esm_row.get('received_date')) else received_date
            requester = esm_row.get('opened_by', requester) if pd.notna(esm_row.get('opened_by')) else requester
            status = esm_row.get('state', status) if pd.notna(esm_row.get('state')) else status
            remarks = esm_row.get('short_description', remarks) if pd.notna(esm_row.get('short_description')) else remarks

        elif channel == 'PM7' and not df_pm7_indexed.empty and barcode in df_pm7_indexed.index:
            pm7_row = df_pm7_indexed.loc[barcode]
            vendor_name = pm7_row.get('vendor_name', vendor_name) if pd.notna(pm7_row.get('vendor_name')) else vendor_name
            vendor_number = pm7_row.get('vendor_number', vendor_number) if pd.notna(pm7_row.get('vendor_number')) else vendor_number
            company_code = pm7_row.get('company_code', company_code) if pd.notna(pm7_row.get('company_code')) else company_code
            received_date = pm7_row.get('received_date', received_date) if pd.notna(pm7_row.get('received_date')) else received_date
            status = pm7_row.get('task', status) if pd.notna(pm7_row.get('task')) else status

        elif channel == 'Workon' and not df_workon_indexed.empty and barcode in df_workon_indexed.index:
            workon_row = df_workon_indexed.loc[barcode]
            # Workon fields are already quite complete in consolidated_df, but ensure they are used if source data has them
            vendor_name = workon_row.get('name', vendor_name) if pd.notna(workon_row.get('name')) else vendor_name
            vendor_number = workon_row.get('vendor_number', vendor_number) if pd.notna(workon_row.get('vendor_number')) else vendor_number
            company_code = workon_row.get('company_code', company_code) if pd.notna(workon_row.get('company_code')) else company_code
            received_date = workon_row.get('updated', received_date) if pd.notna(workon_row.get('updated')) else received_date
            processor = 'Jayapal' # Hardcoded
            channel = 'Workon'    # Hardcoded
            category = workon_row.get('action', category) if pd.notna(workon_row.get('action')) else category
            region = workon_row.get('country', region) if pd.notna(workon_row.get('country')) else region
            status = workon_row.get('status', status) if pd.notna(workon_row.get('status')) else status
            requester = workon_row.get('applicant', requester) if pd.notna(workon_row.get('applicant')) else requester
            remarks = workon_row.get('summary', remarks) if pd.notna(workon_row.get('summary')) else remarks
            allocation_date = today_date_formatted # Hardcoded

        elif channel == 'RGPA' and not df_rgpa_indexed.empty and barcode in df_rgpa_indexed.index:
            rgpa_row = df_rgpa_indexed.loc[barcode]
            processor = 'Divya' # Hardcoded
            channel = 'RGPA'    # Hardcoded
            company_code = rgpa_row.get('company_code', company_code) if pd.notna(rgpa_row.get('company_code')) else company_code
            received_date = rgpa_row.get('updated', received_date) if pd.notna(rgpa_row.get('updated')) else received_date
            remarks = rgpa_row.get('summary', remarks) if pd.notna(rgpa_row.get('summary')) else remarks
            allocation_date = today_date_formatted # Hardcoded


        new_central_row_data = {
            'Barcode': barcode,
            'Processor': processor if processor is not None else '',
            'Channel': channel,
            'Category': category if category is not None else '',
            'Company code': company_code if company_code is not None else '',
            'Region': region if region is not None else '', 
            'Vendor number': vendor_number if vendor_number is not None else '',
            'Vendor Name': vendor_name if vendor_name is not None else '',
            'Status': status if status is not None else 'New', # Default to 'New' if still None
            'Received Date': received_date, 
            'Re-Open Date': None,
            'Allocation Date': allocation_date, # Use the potentially updated allocation_date
            'Clarification Date': None,
            'Completion Date': None,
            'Requester': requester if requester is not None else '',
            'Remarks': remarks if remarks is not None else '',
            'Aging': None,
            'Today': today_date_formatted
        }

        all_new_central_rows_data.append(new_central_row_data)

    if all_new_central_rows_data:
        df_new_central_rows = pd.DataFrame(all_new_central_rows_data)
        for col in CONSOLIDATED_OUTPUT_COLUMNS:
            if col not in df_new_central_rows.columns:
                df_new_central_rows[col] = None
        df_new_central_rows = df_new_central_rows[CONSOLIDATED_OUTPUT_COLUMNS]
    else:
        df_new_central_rows = pd.DataFrame(columns=CONSOLIDATED_OUTPUT_COLUMNS)

    for col in df_new_central_rows.columns:
        if df_new_central_rows[col].dtype == 'object':
            df_new_central_rows[col] = df_new_central_rows[col].fillna('')
        elif col in ['Barcode', 'Company code', 'Vendor number']:
            df_new_central_rows[col] = df_new_central_rows[col].astype(str).replace('nan', '')

    barcodes_for_needs_review = central_barcodes_set - consolidated_barcodes_set
    print(f"Found {len(barcodes_for_needs_review)} barcodes in central not in any consolidated source.")

    df_final_central = updated_existing_central_df.copy()

    needs_review_barcode_mask = df_final_central['Barcode'].isin(barcodes_for_needs_review)
    is_not_completed_status_mask = ~df_final_central['Status'].astype(str).str.strip().str.lower().eq('completed')
    final_needs_review_condition = needs_review_barcode_mask & is_not_completed_status_mask

    df_final_central.loc[final_needs_review_condition, 'Status'] = 'Needs Review'
    print(f"Updated {final_needs_review_condition.sum()} records to 'Needs Review' where status was not 'Completed'.")

    for col in CONSOLIDATED_OUTPUT_COLUMNS:
        if col not in df_final_central.columns:
            df_final_central[col] = None
    df_final_central = df_final_central[CONSOLIDATED_OUTPUT_COLUMNS]

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
                
                new_mapped_regions = df_final_central['Company code_lookup'].map(region_map)

                if 'Region' not in df_final_central.columns:
                    df_final_central['Region'] = ''
                
                df_final_central['Region'] = df_final_central['Region'].replace('', pd.NA) 
                df_final_central['Region'] = df_final_central['Region'].fillna(new_mapped_regions)
                df_final_central['Region'] = df_final_central['Region'].astype(str).replace('nan', '')
                
                df_final_central = df_final_central.drop(columns=['Company code_lookup'])
                print("Region mapping applied successfully. Existing regions prioritized.")
            else:
                print("Warning: 'Company code' column not found in final central DataFrame. Cannot apply region mapping.")
                if 'Region' not in df_final_central.columns:
                    df_final_central['Region'] = ''
                df_final_central['Region'] = df_final_central['Region'].fillna('')


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

    for col in CONSOLIDATED_OUTPUT_COLUMNS:
        if col not in df_final_central.columns:
            df_final_central[col] = ''
    df_final_central = df_final_central[CONSOLIDATED_OUTPUT_COLUMNS]

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
    return render_template('index.html')

@app.route('/process', methods=['POST'])
def process_files():
    temp_dir = tempfile.mkdtemp(dir=tempfile.gettempdir())

    session.pop('consolidated_output_path', None)
    session.pop('central_output_path', None)
    session.pop('temp_dir', None)

    session['temp_dir'] = temp_dir

    # Use os.path.join for robustness across OS
    REGION_MAPPING_FILE_PATH = os.path.join(BASE_DIR, 'company_code_region_mapping.xlsx') 
    # ^^^ IMPORTANT: Check this path. If 'company_code_region_mapping.xlsx' is in a 'data' folder
    # next to app.py, it would be: os.path.join(BASE_DIR, 'data', 'company_code_region_mapping.xlsx')


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
            if key in request.files and request.files[key].filename != '': 
                file = request.files[key]
                if file.filename.lower().endswith('.xlsx'):
                    filename = secure_filename(file.filename)
                    file_path = os.path.join(temp_dir, filename)
                    file.save(file_path)
                    uploaded_files[key] = file_path
                    flash(f'Optional file "{filename}" uploaded successfully.', 'info')
                else:
                    flash(f'Invalid file type for optional file "{key}". It must be an .xlsx file.', 'warning')
                    uploaded_files[key] = None 
            else:
                flash(f'Optional file "{key}" not provided. Continuing without it.', 'info')
                uploaded_files[key] = None 

        pisa_file_path = uploaded_files['pisa_file']
        esm_file_path = uploaded_files['esm_file']
        pm7_file_path = uploaded_files['pm7_file']
        workon_file_path = uploaded_files['workon_file'] 
        rgpa_file_path = uploaded_files['rgpa_file']
        initial_central_file_input_path = uploaded_files['central_file']

        df_pisa_original = None
        df_esm_original = None
        df_pm7_original = None
        df_workon_original = None 
        df_rgpa_original = None 
        df_region_mapping = pd.DataFrame() 

        try:
            df_pisa_original = pd.read_excel(pisa_file_path)
            df_esm_original = pd.read_excel(esm_file_path)
            df_pm7_original = pd.read_excel(pm7_file_path)
            df_rgpa_original = pd.read_excel(rgpa_file_path) 
            
            if workon_file_path:
                df_workon_original = pd.read_excel(workon_file_path)
            else:
                print("Workon file not read as it was not provided.")
                df_workon_original = pd.DataFrame() 

            if os.path.exists(REGION_MAPPING_FILE_PATH):
                df_region_mapping = pd.read_excel(REGION_MAPPING_FILE_PATH)
                print(f"Successfully loaded region mapping file from: {REGION_MAPPING_FILE_PATH}")
            else:
                flash(f"Warning: Region mapping file not found at {REGION_MAPPING_FILE_PATH}. Region column will be empty for records relying solely on this mapping.", 'warning')
        
        except Exception as e:
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

        return render_template('index.html',
                                central_download_link=url_for('download_file', filename=os.path.basename(final_central_output_file_path))
                              )

    except Exception as e:
        flash(f'An unhandled error occurred during processing: {e}', 'error')
        import traceback
        traceback.print_exc()
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        session.pop('temp_dir', None)
        return redirect(url_for('index'))
    finally:
        pass


@app.route('/download/<filename>', methods=['GET'])
def download_file(filename):
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
    temp_dir = session.get('temp_dir')
    if temp_dir and os.path.exists(temp_dir):
        try:
            shutil.rmtree(temp_dir)
            print(f"DEBUG: Cleaned up temporary directory: {temp_dir}")
            flash('Temporary files cleaned up.', 'info')
        except OSError as e:
            print(f"ERROR: Error removing temporary directory {temp_dir}: {e}")
            flash(f'Error cleaning up temporary files: {e}', 'error')
    session.pop('temp_dir', None)
    session.pop('consolidated_output_path', None)
    session.pop('central_output_path', None)
    return redirect(url_for('index'))

if __name__ == '__main__':
    app.run(debug=True)
