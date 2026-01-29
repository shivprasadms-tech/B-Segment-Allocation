import os
import pandas as pd
from datetime import datetime
import warnings
import shutil
import tempfile
import re
from flask import Flask, request, render_template, redirect, url_for, send_file, flash, session
from werkzeug.utils import secure_filename
import logging

warnings.filterwarnings('ignore')

# Configure logging to DEBUG level to see all messages
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Assuming 'index.py' is in 'api/' and 'templates'/'static' are at the project root
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
template_dir = os.path.join(BASE_DIR, '..', 'templates')
static_dir = os.path.join(BASE_DIR, '..', 'static')

app = Flask(__name__, template_folder=template_dir, static_folder=static_dir)
app.secret_key = os.environ.get('FLASK_SECRET_KEY', 'a_strong_default_secret_key_for_local_dev_only_change_this_in_production')

# --- Global Variables ---
CONSOLIDATED_OUTPUT_COLUMNS = [
    'Barcode', 'Processor', 'Channel', 'Category', 'Company code', 'Region',
    'Vendor number', 'Vendor Name', 'Status', 'Received Date', 'Re-Open Date',
    'Allocation Date', 'Clarification Date', 'Completion Date', 'Requester',
    'Remarks', 'Aging', 'Today'
]

# Define expected output columns for PMD Lookup
PMD_OUTPUT_COLUMNS = [
    'Valid From', 'Bukr.', 'Type', 'EBSNO', 'Supplier Name', 'Street', 'City',
    'Country', 'Zip Code', 'Requested By', 'Pur. approver', 'Pur. release date',
    'Status', 'Assigned'
]

ALLOWED_EXTENSIONS = {'xls', 'xlsx'}

# --- Helper Functions ---

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

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

# --- B-Segment Allocation Functions (No Changes) ---

def consolidate_data_process(df_pisa, df_esm, df_pm7):
    # B-Segment Allocation code - UNCHANGED
    logging.info("Starting primary data consolidation (PISA, ESM, PM7)...")
    logging.info("Input DataFrames for primary consolidation loaded successfully!")

    df_pisa = clean_column_names(df_pisa.copy())
    df_esm = clean_column_names(df_esm.copy())
    df_pm7 = clean_column_names(df_pm7.copy())

    all_consolidated_rows = []
    today_date = datetime.now()
    today_date_formatted = today_date.strftime("%m/%d/%Y")

    # --- PISA Processing ---
    allowed_pisa_users = ["Goswami Sonali", "Patil Jayapal Gowd", "Ranganath Chilamakuri","Sridhar Divya","Sunitha S","Varunkumar N"]
    if 'assigned_user' in df_pisa.columns:
        original_pisa_count = len(df_pisa)
        # Using .copy() after filter to avoid SettingWithCopyWarning later
        df_pisa_filtered = df_pisa[df_pisa['assigned_user'].isin(allowed_pisa_users)].copy()
        logging.info(f"\nPISA file filtered. Original records: {original_pisa_count}, Records after filter: {len(df_pisa_filtered)}")
    else:
        logging.warning("\nWarning: 'assigned_user' column not found in PISA file (after cleaning). No filter applied.")
        df_pisa_filtered = df_pisa.copy()

    if 'barcode' not in df_pisa_filtered.columns:
        logging.error("Error: 'barcode' column not found in PISA file (after cleaning). Skipping PISA processing.")
    else:
        df_pisa_filtered['barcode'] = df_pisa_filtered['barcode'].astype(str)
        for index, row in df_pisa_filtered.iterrows():
            new_row = {
                'Barcode': row['barcode'],
                'Company code': str(row.get('company_code', '')), # Defensive str conversion
                'Vendor number': str(row.get('vendor_number', '')), # Defensive str conversion
                'Received Date': row.get('received_date'),
                'Completion Date': None,
                'Status': str(row.get('status', '')), # Defensive str conversion
                'Today': today_date_formatted,
                'Channel': 'PISA',
                'Vendor Name': str(row.get('vendor_name', '')), # Defensive str conversion
                'Re-Open Date': None,
                'Allocation Date': today_date_formatted,
                'Requester': None, 'Clarification Date': None, 'Aging': None, 'Remarks': None,
                'Region': None, 'Processor': None, 'Category': None
            }
            all_consolidated_rows.append(new_row)
        logging.info(f"Collected {len(df_pisa_filtered)} rows from PISA.")

    # --- ESM Processing ---
    if 'barcode' not in df_esm.columns:
        logging.error("Error: 'barcode' column not found in ESM file (after cleaning). Skipping ESM processing.")
    else:
        df_esm['barcode'] = df_esm['barcode'].astype(str)
        for index, row in df_esm.iterrows():
            new_row = {
                'Barcode': row['barcode'],
                'Received Date': row.get('received_date'),
                'Status': str(row.get('state', '')), # Defensive str conversion
                'Requester': str(row.get('opened_by', '')), # Defensive str conversion
                'Completion Date': row.get('closed') if pd.notna(row.get('closed')) else None,
                'Re-Open Date': row.get('updated') if (str(row.get('state', '')).lower() == 'reopened') else None,
                'Today': today_date_formatted,
                'Remarks': str(row.get('short_description', '')), # Defensive str conversion
                'Channel': 'ESM',
                'Company code': None,'Vendor Name': None, # Keep None, will be filled if needed later
                'Vendor number': None,
                'Allocation Date': today_date_formatted,
                'Clarification Date': None, 'Aging': None,
                'Region': None, 'Processor': None, 'Category': None
            }
            all_consolidated_rows.append(new_row)
        logging.info(f"Collected {len(df_esm)} rows from ESM.")

    # --- PM7 Processing ---
    if 'barcode' not in df_pm7.columns:
        logging.error("Error: 'barcode' column not found in PM7 file (after cleaning). Skipping PM7 processing.")
    else:
        df_pm7['barcode'] = df_pm7['barcode'].astype(str)

        for index, row in df_pm7.iterrows():
            new_row = {
                'Barcode': row['barcode'],
                'Vendor Name': str(row.get('vendor_name', '')), # Defensive str conversion
                'Vendor number': str(row.get('vendor_number', '')), # Defensive str conversion
                'Received Date': row.get('received_date'),
                'Status': str(row.get('task', '')), # Defensive str conversion
                'Today': today_date_formatted,
                'Channel': 'PM7',
                'Company code': str(row.get('company_code', '')), # Defensive str conversion
                'Re-Open Date': None,
                'Allocation Date': today_date_formatted, 'Completion Date': None, 'Requester': None,
                'Clarification Date': None, 'Aging': None,
                'Region': None, 'Processor': None, 'Category': None
            }
            all_consolidated_rows.append(new_row)
        logging.info(f"Collected {len(df_pm7)} rows from PM7.")

    if not all_consolidated_rows:
        logging.info("No data collected for consolidation from PISA, ESM, PM7. Returning empty DataFrame.")
        return pd.DataFrame(columns=CONSOLIDATED_OUTPUT_COLUMNS)

    df_consolidated = pd.DataFrame(all_consolidated_rows)

    # Ensure all required columns are present in the consolidated DF
    for col in CONSOLIDATED_OUTPUT_COLUMNS:
        if col not in df_consolidated.columns:
            df_consolidated[col] = None # Use None initially, will be converted to empty string later if needed

    df_consolidated = df_consolidated[CONSOLIDATED_OUTPUT_COLUMNS]

    # Convert known date columns to datetime objects for consistency
    # This step is crucial for the Aging calculation later
    date_cols_to_process = ['Received Date', 'Re-Open Date', 'Allocation Date', 'Completion Date', 'Clarification Date', 'Today']
    for col in date_cols_to_process:
        if col in df_consolidated.columns:
            df_consolidated[col] = pd.to_datetime(df_consolidated[col], errors='coerce')

    # Convert Barcode, Company code, Vendor number to string *before* using in sets or merges
    for col in ['Barcode', 'Company code', 'Vendor number']:
        if col in df_consolidated.columns:
            df_consolidated[col] = df_consolidated[col].astype(str).replace('nan', '')

    logging.info("--- Primary Consolidated Data Process (PISA, ESM, PM7) Complete ---")
    return df_consolidated

def process_central_file_step2_update_existing(consolidated_df_pisa_esm_pm7, central_file_input_path):
    # B-Segment Allocation code - UNCHANGED
    logging.info(f"\n--- Starting Central File Status Processing (Step 2: Update Existing Barcodes) ---")

    try:
        # Read central file, forcing key columns to string to avoid merge issues
        converters = {'Barcode': str, 'Vendor number': str, 'Company code': str}
        df_central = pd.read_excel(central_file_input_path, converters=converters, keep_default_na=False)
        df_central_cleaned = clean_column_names(df_central.copy())

        # Ensure Barcode in central file is string and replace 'nan'
        if 'barcode' in df_central_cleaned.columns:
            df_central_cleaned['barcode'] = df_central_cleaned['barcode'].astype(str).replace('nan', '')
        else:
            return False, "Error: 'barcode' column not found in the central file after cleaning. Cannot update status (Step 2)."

        # Ensure 'status' column exists for subsequent logic
        if 'status' not in df_central_cleaned.columns:
            df_central_cleaned['status'] = '' # Add empty status if missing
            logging.warning("Warning: 'status' column not found in central file after cleaning. Added empty 'status' column.")

        logging.info("Consolidated (DF) and Central (file) loaded successfully for Step 2!")
    except Exception as e:
        return False, f"Error loading Consolidated (DF) or Central (file) for processing (Step 2): {e}"

    if 'Barcode' not in consolidated_df_pisa_esm_pm7.columns:
        return False, "Error: 'Barcode' column not found in the consolidated (PISA/ESM/PM7) file. Cannot proceed with central file processing (Step 2)."

    # Barcodes from consolidated PISA, ESM, PM7 for status change logic
    # Only if consolidated_df_pisa_esm_pm7 is not empty
    consolidated_barcodes_for_status_change_set = set()
    if not consolidated_df_pisa_esm_pm7.empty:
        consolidated_barcodes_for_status_change_set = set(consolidated_df_pisa_esm_pm7['Barcode'].unique())

    logging.info(f"Found {len(consolidated_barcodes_for_status_change_set)} unique barcodes from PISA/ESM/PM7 in consolidated file for Step 2 status updates.")

    # Apply the status transformation only for central records whose barcodes exist in the consolidated set
    def transform_status_if_barcode_exists(row):
        central_barcode = str(row['barcode']) # Use the cleaned central barcode column
        original_central_status = str(row['status']) # Ensure status is string for comparison

        if central_barcode in consolidated_barcodes_for_status_change_set:
            status_str = original_central_status.strip().lower()
            if status_str == 'new':
                return 'Untouched'
            elif status_str == 'completed':
                return 'Reopen'
            elif status_str == 'n/a':
                return 'New'
            elif status_str in ['', 'na', 'none']: # If it's already 'nan' or empty-like
                return original_central_status
            else:
                return original_central_status
        else:
            return original_central_status # If barcode not in consolidated, keep original status for now (Needs Review handled in Step 3)

    df_central_cleaned['status'] = df_central_cleaned.apply(transform_status_if_barcode_exists, axis=1)
    logging.info(f"Applied status transformation logic for existing central file records ({len(df_central_cleaned)} records processed).")


    # Final cleanup and column remapping for the central file part that has been processed
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
                # Convert to datetime objects for consistency before final string formatting
                df_central_cleaned[col] = pd.to_datetime(df_central_cleaned[col], errors='coerce')
            elif df_central_cleaned[col].dtype == 'object':
                df_central_cleaned[col] = df_central_cleaned[col].fillna('')
            elif col in ['Barcode', 'Vendor number', 'Company code']:
                df_central_cleaned[col] = df_central_cleaned[col].astype(str).replace('nan', '')

        # Ensure all CONSOLIDATED_OUTPUT_COLUMNS are present
        for col in CONSOLIDATED_OUTPUT_COLUMNS:
            if col not in df_central_cleaned.columns:
                df_central_cleaned[col] = None # Use None for missing columns initially

        # Reorder to match CONSOLIDATED_OUTPUT_COLUMNS structure
        df_central_cleaned = df_central_cleaned[CONSOLIDATED_OUTPUT_COLUMNS]

    except Exception as e:
        return False, f"Error processing central file (Step 2) during final cleanup and remapping: {e}"
    logging.info(f"--- Central File Status Processing (Step 2) Complete ---")
    return True, df_central_cleaned


def process_central_file_step3_final_merge_and_needs_review(
    df_consolidated_pisa_esm_pm7, # This now contains only PISA, ESM, PM7 data
    updated_existing_central_df, # This is the central file after step 2 status updates
    final_central_output_file_path,
    df_pisa_original, df_esm_original, df_pm7_original, # Original DFs for potential lookup/validation
    df_workon_original, df_rgba_original, df_smd_original, # Original DFs for direct mapping
    region_mapping_df
):
    # B-Segment Allocation code - UNCHANGED
    logging.info(f"\n--- Starting Central File Status Processing (Step 3: Final Merge & Needs Review) ---")

    today_date = datetime.now()
    today_date_formatted = today_date.strftime("%m/%d/%Y")

    # Start with the central file after Step 2 updates
    df_final_central = updated_existing_central_df.copy()

    logging.debug(f"DEBUG (Step 3): Initial df_final_central Status distribution:\n{df_final_central['Status'].value_counts(dropna=False)}")

    # Get sets of barcodes for efficient lookup
    central_barcodes_set = set(df_final_central['Barcode'].astype(str).unique())

    # Ensure consolidated_pisa_esm_pm7_barcodes_set is empty if df_consolidated_pisa_esm_pm7 is empty
    consolidated_pisa_esm_pm7_barcodes_set = set()
    if not df_consolidated_pisa_esm_pm7.empty:
        consolidated_pisa_esm_pm7_barcodes_set = set(df_consolidated_pisa_esm_pm7['Barcode'].astype(str).unique())

    # --- 1. Add NEW records from PISA/ESM/PM7 to the central file ---
    # These are barcodes in consolidated_pisa_esm_pm7_barcodes_set but NOT in central_barcodes_set
    barcodes_from_pisa_esm_pm7_to_add = consolidated_pisa_esm_pm7_barcodes_set - central_barcodes_set
    logging.info(f"Found {len(barcodes_from_pisa_esm_pm7_to_add)} new barcodes from PISA/ESM/PM7 to add to central. Their status will be 'New'.")

    if not df_consolidated_pisa_esm_pm7.empty and not df_consolidated_pisa_esm_pm7.empty: # Second check is redundant but safe
        df_new_records_from_pisa_esm_pm7 = df_consolidated_pisa_esm_pm7[
            df_consolidated_pisa_esm_pm7['Barcode'].isin(barcodes_from_pisa_esm_pm7_to_add)
        ].copy()
        if not df_new_records_from_pisa_esm_pm7.empty:
            df_new_records_from_pisa_esm_pm7['Status'] = 'New' # Set status for truly new records
            df_final_central = pd.concat([df_final_central, df_new_records_from_pisa_esm_pm7], ignore_index=True)
            logging.info(f"Appended {len(df_new_records_from_pisa_esm_pm7)} new records from PISA/ESM/PM7 with status 'New'.")
        else:
            logging.info("No new PISA/ESM/PM7 records to append from the consolidated data (all already in central or no new barcodes).")
    else:
        logging.info("Consolidated PISA/ESM/PM7 DataFrame was empty, so no new records to append from it.")

    logging.debug(f"DEBUG (Step 3): Status distribution after adding new PISA/ESM/PM7 records:\n{df_final_central['Status'].value_counts(dropna=False)}")

    # --- 2. Mark 'Needs Review' for central records not found in PISA/ESM/PM7 consolidated ---
    # These are barcodes in central_barcodes_set but NOT in consolidated_pisa_esm_pm7_barcodes_set
    barcodes_for_needs_review = central_barcodes_set - consolidated_pisa_esm_pm7_barcodes_set
    logging.info(f"Found {len(barcodes_for_needs_review)} barcodes in original central not in PISA/ESM/PM7 consolidated sources, applying 'Needs Review' logic.")

    # Apply 'Needs Review' only to records whose barcodes are in `barcodes_for_needs_review`
    # AND whose status is NOT 'Completed'.
    needs_review_mask = df_final_central['Barcode'].isin(barcodes_for_needs_review)
    not_completed_mask = ~(df_final_central['Status'].astype(str).str.strip().str.lower() == 'completed')

    # Combine masks and apply 'Needs Review'
    df_final_central.loc[needs_review_mask & not_completed_mask, 'Status'] = 'Needs Review'
    logging.info(f"Updated {(needs_review_mask & not_completed_mask).sum()} records to 'Needs Review'.")
    logging.debug(f"DEBUG (Step 3): Status distribution after 'Needs Review' logic:\n{df_final_central['Status'].value_counts(dropna=False)}")


    # --- 3. Directly map and append Workon P71 records ---
    if df_workon_original is not None and not df_workon_original.empty:
        df_workon_cleaned = clean_column_names(df_workon_original.copy())
        if 'key' not in df_workon_cleaned.columns:
            logging.error("Error: 'key' column not found in Workon file (after cleaning). Skipping Workon processing.")
        else:
            workon_records_to_append = []
            for index, row in df_workon_cleaned.iterrows():
                new_row = {
                    'Barcode': str(row.get('key', '')), # Defensive str conversion
                    'Processor': 'Jayapal',
                    'Channel': 'Workon',
                    'Category': str(row.get('action', '')), # Defensive str conversion
                    'Company code': str(row.get('company_code', '')), # Defensive str conversion
                    'Region': str(row.get('country', '')), # Defensive str conversion
                    'Vendor number': str(row.get('vendor_number', '')), # Defensive str conversion
                    'Vendor Name': str(row.get('name', '')), # Defensive str conversion
                    'Status': str(row.get('status', '')), # Defensive str conversion
                    'Received Date': row.get('updated'),
                    'Re-Open Date': None,
                    'Allocation Date': today_date_formatted,
                    'Clarification Date': None,
                    'Completion Date': None,
                    'Requester': str(row.get('applicant', '')), # Defensive str conversion
                    'Remarks': str(row.get('summary', '')), # Defensive str conversion
                    'Aging': None,
                    'Today': today_date_formatted
                }
                workon_records_to_append.append(new_row)
            if workon_records_to_append:
                df_workon_appended = pd.DataFrame(workon_records_to_append)
                # Ensure all CONSOLIDATED_OUTPUT_COLUMNS are present, filling missing with None
                df_workon_appended = df_workon_appended.reindex(columns=CONSOLIDATED_OUTPUT_COLUMNS)
                df_final_central = pd.concat([df_final_central, df_workon_appended], ignore_index=True)
                logging.info(f"Appended {len(df_workon_appended)} records from Workon P71 directly.")
            else:
                logging.info("No records to append from Workon P71 after mapping.")
    else:
        logging.info("Workon file not provided or is empty. Skipping Workon processing.")
    logging.debug(f"DEBUG (Step 3): Status distribution after Workon append:\n{df_final_central['Status'].value_counts(dropna=False)}")


    # --- 4. Directly map and append RGBA records ---
    logging.info("Attempting to process RGBA records for direct appending.")
    if df_rgba_original is None:
        logging.warning("RGBA original DataFrame is None. Was the RGPA file uploaded and read successfully?")
    elif df_rgba_original.empty:
        logging.info("RGBA original DataFrame is empty. Skipping RGBA processing.")
    else:
        df_rgba_cleaned = clean_column_names(df_rgba_original.copy())
        logging.info(f"RGBA file has {len(df_rgba_cleaned)} records after cleaning column names.")

        # --- FILTER REMOVED ---
        df_rgba_filtered = df_rgba_cleaned.copy()
        logging.info("RGBA 'current_assignee' filter has been explicitly removed. All RGBA records will be considered.")
        # --- END FILTER REMOVED ---

        if df_rgba_filtered.empty:
            logging.info("RGBA DataFrame is empty after (no) filtering. No RGBA records to process.")
        elif 'key' not in df_rgba_filtered.columns:
            logging.error("Error: 'key' column not found in RGBA file after cleaning. Skipping RGBA processing.")
            logging.debug(f"Columns available in filtered RGBA: {df_rgba_filtered.columns.tolist()}")
        else:
            rgba_records_to_append = []
            for index, row in df_rgba_filtered.iterrows():
                # Log a sample of row data for debugging
                if index < 5: # Log first 5 rows for inspection
                    logging.debug(f"Processing RGBA row (sample): Barcode={row.get('key')}, Company_code={row.get('company_code')}, Updated={row.get('updated')}")

                new_row = {
                    'Barcode': str(row.get('key', '')), # Defensive str conversion
                    'Processor': 'Divya',
                    'Channel': 'Workon', # Confirmed: Channel for RGBA is 'Workon'
                    'Category': None,
                    'Company code': str(row.get('company_code', '')), # Defensive str conversion
                    'Region': None, # Will be filled by region mapping later if not present
                    'Vendor number': None,
                    'Vendor Name': None,
                    'Status': None,
                    'Received Date': row.get('updated'),
                    'Re-Open Date': None,
                    'Allocation Date': today_date_formatted,
                    'Clarification Date': None,
                    'Completion Date': None,
                    'Requester': None,
                    'Remarks': str(row.get('summary', '')), # Defensive str conversion
                    'Aging': None,
                    'Today': today_date_formatted
                }
                rgba_records_to_append.append(new_row)
            if rgba_records_to_append:
                df_rgba_appended = pd.DataFrame(rgba_records_to_append)
                df_rgba_appended = df_rgba_appended.reindex(columns=CONSOLIDATED_OUTPUT_COLUMNS)
                df_final_central = pd.concat([df_final_central, df_rgba_appended], ignore_index=True)
                logging.info(f"Successfully appended {len(df_rgba_appended)} records from RGBA directly.")
            else:
                logging.info("No records generated from RGBA for appending after individual row processing (might be due to missing keys or unexpected values).")
    logging.debug(f"DEBUG (Step 3): Status distribution after RGBA append:\n{df_final_central['Status'].value_counts(dropna=False)}")


    # --- 5. Directly map and append SMD records ---
    if df_smd_original is not None and not df_smd_original.empty:
        df_smd_cleaned = clean_column_names(df_smd_original.copy())
        smd_records_to_append = []
        for index, row in df_smd_cleaned.iterrows():
            new_row = {
                'Barcode': None, # As no explicit barcode column was given for SMD, keeping it None.
                'Processor': None,
                'Channel': 'SMD',
                'Category': None,
                'Company code': str(row.get('ekorg', '')), # Defensive str conversion
                'Region': str(row.get('material_field', '')), # Defensive str conversion
                'Vendor number': str(row.get('pmd-sno', '')), # Defensive str conversion
                'Vendor Name': str(row.get('supplier_name', '')), # Defensive str conversion
                'Status': None,
                'Received Date': row.get('request_date'),
                'Re-Open Date': None,
                'Allocation Date': today_date_formatted,
                'Clarification Date': None,
                'Completion Date': None,
                'Requester': str(row.get('requested_by', '')), # Defensive str conversion
                'Remarks': None,
                'Aging': None,
                'Today': today_date_formatted
            }
            smd_records_to_append.append(new_row)
        if smd_records_to_append:
            df_smd_appended = pd.DataFrame(smd_records_to_append)
            df_smd_appended = df_smd_appended.reindex(columns=CONSOLIDATED_OUTPUT_COLUMNS)
            df_final_central = pd.concat([df_final_central, df_smd_appended], ignore_index=True)
            logging.info(f"Appended {len(df_smd_appended)} records from SMD directly.")
        else:
            logging.info("No records to append from SMD after mapping.")
    else:
        logging.info("SMD file not provided or is empty. Skipping SMD processing.")
    logging.debug(f"DEBUG (Step 3): Status distribution after SMD append:\n{df_final_central['Status'].value_counts(dropna=False)}")


    # --- 6. Handle blank Company Code for PM7 channel (Applies to all PM7 records in df_final_central) ---
    logging.info("\n--- Applying PM7 Company Code population logic ---")
    if 'Channel' in df_final_central.columns and 'Company code' in df_final_central.columns and 'Barcode' in df_final_central.columns:
        pm7_blank_cc_mask = (df_final_central['Channel'] == 'PM7') & \
                            (df_final_central['Company code'].astype(str).replace('nan', '').str.strip() == '')

        # Ensure Barcode is not None/empty before slicing
        valid_barcodes_for_pm7 = df_final_central.loc[pm7_blank_cc_mask, 'Barcode'].astype(str).str.strip()
        df_final_central.loc[pm7_blank_cc_mask, 'Company code'] = \
            valid_barcodes_for_pm7.apply(lambda x: x[:4] if len(x) >= 4 else '')

        logging.info(f"Populated Company Code for {pm7_blank_cc_mask.sum()} PM7 records based on Barcode.")
    else:
        logging.warning("Warning: 'Channel', 'Company code', or 'Barcode' columns missing. Skipping PM7 Company Code population logic.")
    logging.debug(f"DEBUG (Step 3): Status distribution after PM7 Company Code logic:\n{df_final_central['Status'].value_counts(dropna=False)}")


    # --- 7. Apply Region Mapping (Applies to all records in df_final_central) ---
    logging.info("\n--- Applying Region Mapping ---")
    if region_mapping_df is None or region_mapping_df.empty:
        logging.warning("Warning: Region mapping file not provided or is empty. Region column will not be populated by external mapping.")
        if 'Region' not in df_final_central.columns:
            df_final_central['Region'] = ''
        df_final_central['Region'] = df_final_central['Region'].fillna('')
    else:
        region_mapping_df = clean_column_names(region_mapping_df.copy())
        if 'r3_coco' not in region_mapping_df.columns or 'region' not in region_mapping_df.columns:
            logging.error("Error: Region mapping file must contain 'r3_coco' and 'region' columns after cleaning. Skipping region mapping.")
            if 'Region' not in df_final_central.columns:
                df_final_central['Region'] = ''
            df_final_central['Region'] = df_final_central['Region'].fillna('')
        else:
            region_map = {}
            for idx, row in region_mapping_df.iterrows():
                coco_key = str(row['r3_coco']).strip().upper()
                if coco_key:
                    region_map[coco_key[:4]] = str(row['region']).strip()

            logging.info(f"Loaded {len(region_map)} unique R/3 CoCo -> Region mappings.")

            if 'Company code' in df_final_central.columns:
                # Ensure 'Company code' column is string before lookup
                df_final_central['Company code_lookup'] = df_final_central['Company code'].astype(str).str.strip().str.upper().str[:4]

                new_mapped_regions = df_final_central['Company code_lookup'].map(region_map)

                if 'Region' not in df_final_central.columns:
                    df_final_central['Region'] = ''

                # Fill NaN (or originally empty string, now pd.NA from fillna) in 'Region' with new_mapped_regions
                # This ensures existing regions are preserved, and only blanks/NaNs get mapped
                df_final_central['Region'] = df_final_central['Region'].replace('', pd.NA).fillna(new_mapped_regions)

                df_final_central['Region'] = df_final_central['Region'].astype(str).replace('nan', '')

                df_final_central = df_final_central.drop(columns=['Company code_lookup'])
                logging.info("Region mapping applied successfully. Existing regions prioritized.")
            else:
                logging.warning("Warning: 'Company code' column not found in final central DataFrame. Cannot apply region mapping.")
                if 'Region' not in df_final_central.columns:
                    df_final_central['Region'] = ''
                df_final_central['Region'] = df_final_central['Region'].fillna('')
    logging.debug(f"DEBUG (Step 3): Status distribution after Region Mapping logic:\n{df_final_central['Status'].value_counts(dropna=False)}")

    # --- 8. Calculate 'Aging' ---
    logging.info("\n--- Calculating 'Aging' column ---")
    if 'Today' in df_final_central.columns and 'Allocation Date' in df_final_central.columns:
        # Ensure 'Today' and 'Allocation Date' are in datetime format for calculation
        df_final_central['Today_dt'] = pd.to_datetime(df_final_central['Today'], errors='coerce')
        df_final_central['Allocation Date_dt'] = pd.to_datetime(df_final_central['Allocation Date'], errors='coerce')

        # Calculate difference and extract days
        # Use .dt.days for timedelta objects, handle NaT results from errors='coerce'
        df_final_central['Aging'] = (df_final_central['Today_dt'] - df_final_central['Allocation Date_dt']).dt.days

        # Replace NaN in 'Aging' with empty string or 0 as per requirement
        # For 'Aging', 0 might be more appropriate than empty string for numerical column
        df_final_central['Aging'] = df_final_central['Aging'].fillna('').astype(str).replace('nan', '') # Convert to string to match other empty values
        # If you prefer 0 for missing dates:
        # df_final_central['Aging'] = df_final_central['Aging'].fillna(0).astype(int)

        # Drop the temporary datetime columns
        df_final_central = df_final_central.drop(columns=['Today_dt', 'Allocation Date_dt'])
        logging.info("'Aging' column calculated successfully.")
    else:
        logging.warning("Warning: 'Today' or 'Allocation Date' columns missing. Cannot calculate 'Aging'.")
        if 'Aging' not in df_final_central.columns:
            df_final_central['Aging'] = ''
        df_final_central['Aging'] = df_final_central['Aging'].fillna('')


    # --- 9. Final formatting and save ---
    # Apply date formatting as MM/DD/YYYY at the very end
    date_cols_in_central_file = [
        'Received Date', 'Re-Open Date', 'Allocation Date',
        'Completion Date', 'Clarification Date', 'Today'
    ]
    for col in CONSOLIDATED_OUTPUT_COLUMNS: # Iterate through all output columns for final formatting
        if col in df_final_central.columns:
            if col in date_cols_in_central_file:
                df_final_central[col] = format_date_to_mdyyyy(df_final_central[col])
            # Ensure other object columns are correctly handled as strings/empty strings
            elif df_final_central[col].dtype == 'object':
                df_final_central[col] = df_final_central[col].fillna('')
            # Barcode, Vendor number, Company code are already handled to str at their source.
            # This handles any remaining cases that might not have been caught
            elif col in ['Barcode', 'Vendor number', 'Company code'] and df_final_central[col].dtype != 'object':
                df_final_central[col] = df_final_central[col].astype(str).replace('nan', '')
        else:
            df_final_central[col] = '' # Ensure all output columns exist, fill missing as empty string

    # Reorder columns to ensure final output matches specification
    df_final_central = df_final_central[CONSOLIDATED_OUTPUT_COLUMNS]
    logging.debug(f"DEBUG: Final Status column before saving:\n{df_final_central['Status'].value_counts(dropna=False)}")
    logging.debug(f"DEBUG: Final sample rows before saving:\n{df_final_central[['Barcode', 'Channel', 'Status', 'Today', 'Allocation Date', 'Aging']].head(10)}")


    try:
        df_final_central.to_excel(final_central_output_file_path, index=False)
        logging.info(f"Final central file (after Step 3) saved to: {final_central_output_file_path}")
        logging.info(f"Total rows in final central file (after Step 3): {len(df_final_central)}")
    except Exception as e:
        return False, f"Error saving final central file (after Step 3): {e}"
    logging.info(f"--- Central File Status Processing (Step 3) Complete ---")
    return True, "Central file processing (Step 3) successful"


# --- B-Segment Allocation Processing Function (now main processing function) ---
def process_b_segment_allocation_core(request_files, temp_dir):
    # B-Segment Allocation code - UNCHANGED
    logging.info("Starting B-Segment Allocation Process...")

    # CORRECTED PATH FOR REGION MAPPING FILE
    # Go up one directory from BASE_DIR (which is 'api/') to get to the project root
    PROJECT_ROOT = os.path.abspath(os.path.join(BASE_DIR, '..')) # Use abspath for clarity
    REGION_MAPPING_FILE_PATH = os.path.join(PROJECT_ROOT, 'company_code_region_mapping.xlsx')

    uploaded_files = {}

    # --- Handle required files ---
    required_file_keys = ['pisa_file', 'esm_file', 'pm7_file', 'rgpa_file', 'b_segment_central_file']
    for key in required_file_keys:
        file = request_files.get(key)
        if not file or file.filename == '':
            return False, f'Missing required file: "{key}". Please upload all required files.', None
        if allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file_path = os.path.join(temp_dir, filename)
            file.save(file_path)
            uploaded_files[key] = file_path
            flash(f'File "{filename}" uploaded successfully.', 'info')
        else:
            return False, f'Invalid file type for "{key}". Please upload an .xlsx file.', None

    # --- Handle optional files ---
    optional_file_keys = ['workon_file', 'smd_file']
    for key in optional_file_keys:
        file = request_files.get(key)
        if file and file.filename != '':
            if allowed_file(file.filename):
                filename = secure_filename(file.filename)
                file_path = os.path.join(temp_dir, filename)
                file.save(file_path)
                uploaded_files[key] = file_path
                flash(f'Optional file "{filename}" uploaded successfully.', 'info')
            else:
                flash(f'Invalid file type for optional file "{key}". It must be an .xlsx file.', 'warning')
                uploaded_files[key] = None # Set to None if invalid
        else:
            logging.info(f'Optional file "{key}" not provided. Continuing without it.')
            uploaded_files[key] = None # Set to None if not provided

    pisa_file_path = uploaded_files['pisa_file']
    esm_file_path = uploaded_files['esm_file']
    pm7_file_path = uploaded_files['pm7_file']
    workon_file_path = uploaded_files['workon_file'] # This will be path or None
    rgba_file_path = uploaded_files['rgpa_file'] # !!! CORRECTED: Retrieve 'rgpa_file' from uploaded_files !!!
    smd_file_path = uploaded_files['smd_file'] # This will be path or None
    initial_central_file_input_path = uploaded_files['b_segment_central_file']

    df_pisa_original = None
    df_esm_original = None
    df_pm7_original = None
    df_workon_original = pd.DataFrame() # Initialize as empty DataFrame
    df_rgba_original = None
    df_smd_original = pd.DataFrame() # Initialize as empty DataFrame
    df_region_mapping = pd.DataFrame()

    try:
        df_pisa_original = pd.read_excel(pisa_file_path)
        df_esm_original = pd.read_excel(esm_file_path)
        df_pm7_original = pd.read_excel(pm7_file_path)
        df_rgba_original = pd.read_excel(rgba_file_path) # Uses the corrected rgba_file_path

        # Handle optional files: check if path exists before reading
        if workon_file_path and os.path.exists(workon_file_path):
            df_workon_original = pd.read_excel(workon_file_path)
        else:
            logging.info("Workon P71 file not loaded (not provided, invalid, or empty).")

        if smd_file_path and os.path.exists(smd_file_path):
            df_smd_original = pd.read_excel(smd_file_path)
        else:
            logging.info("SMD file not loaded (not provided, invalid, or empty).")

        if os.path.exists(REGION_MAPPING_FILE_PATH):
            df_region_mapping = pd.read_excel(REGION_MAPPING_FILE_PATH)
            logging.info(f"Successfully loaded region mapping file from: {REGION_MAPPING_FILE_PATH}")
        else:
            flash(f"Warning: Region mapping file not found at {REGION_MAPPING_FILE_PATH}. Region column will be empty for records relying solely on this mapping.", 'warning')
            logging.warning(f"Region mapping file not found at {REGION_MAPPING_FILE_PATH}. Region column will be empty for records relying solely on this mapping.")


    except Exception as e:
        return False, f"Error loading one or more input Excel files: {e}. Please ensure all files are valid .xlsx formats.", None

    today_str = datetime.now().strftime("%d_%m_%Y_%H%M%S")

    # --- Step 1: Consolidate Data (PISA, ESM, PM7 only) ---
    df_consolidated_pisa_esm_pm7 = consolidate_data_process(
        df_pisa_original, df_esm_original, df_pm7_original
    )

    # Check if df_consolidated_pisa_esm_pm7 is valid for subsequent steps
    if df_consolidated_pisa_esm_pm7.empty and (not df_pisa_original.empty or not df_esm_original.empty or not df_pm7_original.empty):
        logging.warning("Consolidation of PISA/ESM/PM7 files resulted in no data, but input files were provided. Check logs for potential filtering or column errors.")
        flash("Warning: PISA/ESM/PM7 consolidation yielded no records. Check if input files were empty or if data was filtered out.", 'warning')
    elif not df_consolidated_pisa_esm_pm7.empty:
        flash('Primary data consolidation from PISA, ESM, PM7 completed successfully!', 'success')
        # Consolidated output file saving is optional, commented out as it's an intermediate
        # consolidated_output_filename = f'ConsolidatedData_PISA_ESM_PM7_{today_str}.xlsx'
        # consolidated_output_file_path = os.path.join(temp_dir, consolidated_output_filename)
        # try:
        #     df_consolidated_pisa_esm_pm7.to_excel(consolidated_output_file_path, index=False)
        #     logging.info(f"Primary consolidated file saved to: {consolidated_output_file_path}")
        #     session['consolidated_output_path'] = consolidated_output_file_path
        # except Exception as e:
        #     logging.warning(f"Could not save primary consolidated file: {e}")


    # --- Step 2: Update existing central file records based on consolidation (PISA, ESM, PM7 only) ---
    success, result_df = process_central_file_step2_update_existing(
        df_consolidated_pisa_esm_pm7, initial_central_file_input_path
    )
    if not success:
        return False, f'Central File Processing (Step 2) Error: {result_df}', None
    df_central_updated_existing = result_df

    # --- Step 3: Final Merge ---
    final_central_output_filename = f'CentralFile_FinalOutput_{today_str}.xlsx'
    final_central_output_file_path = os.path.join(temp_dir, final_central_output_filename)
    success, message = process_central_file_step3_final_merge_and_needs_review(
        df_consolidated_pisa_esm_pm7, df_central_updated_existing, final_central_output_file_path,
        df_pisa_original, df_esm_original, df_pm7_original,
        df_workon_original, df_rgba_original, df_smd_original, df_region_mapping
    )
    if not success:
        return False, f'Central File Processing (Step 3) Error: {message}', None
    flash('Central file finalized successfully!', 'success')
    session['central_output_path'] = final_central_output_file_path
    return True, 'Processing complete', final_central_output_file_path


def process_pmd_lookup_core(request_files, temp_dir):
    """Encapsulates the PMD Lookup logic."""
    logging.info("Starting PMD Lookup Process...")

    uploaded_files = {}

    # Handle required PMD files
    required_pmd_file_keys = ['pmd_central_file', 'pmd_lookup_file']
    for key in required_pmd_file_keys:
        file = request_files.get(key)
        if not file or file.filename == '':
            return False, f'Missing required PMD file: "{key}". Please upload both PMD files.', None
        if allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file_path = os.path.join(temp_dir, filename)
            file.save(file_path)
            uploaded_files[key] = file_path
            flash(f'PMD file "{filename}" uploaded successfully.', 'info')
        else:
            return False, f'Invalid file type for PMD file "{key}". Please upload an .xlsx file.', None

    pmd_central_file_path = uploaded_files['pmd_central_file']
    pmd_lookup_file_path = uploaded_files['pmd_lookup_file']

    try:
        # Load and clean PMD Central File
        # Use keep_default_na=False to prevent empty strings from being read as NaN
        df_central_pmd_original = pd.read_excel(pmd_central_file_path, keep_default_na=False)
        df_central_pmd = clean_column_names(df_central_pmd_original.copy())

        # Load and clean PMD Dump File
        df_pmd_dump_original = pd.read_excel(pmd_lookup_file_path, keep_default_na=False)
        df_pmd_dump = clean_column_names(df_pmd_dump_original.copy())

        logging.info("PMD Central and PMD Dump files loaded and cleaned.")

    except Exception as e:
        return False, f"Error loading one or both PMD Excel files: {e}. Please ensure they are valid .xlsx formats.", None

    # --- Step 1: Drop 'sl_no' and 'duns' from PMD Dump ---
    cols_to_drop = ['sl_no', 'duns']
    df_pmd_dump.drop(columns=cols_to_drop, errors='ignore', inplace=True)
    logging.info(f"Dropped columns {cols_to_drop} from PMD Dump if they existed.")

    # --- Step 2: Country Exclusion Filtering from PMD Dump ---
    excluded_countries = ['cn', 'id', 'tw', 'hk', 'jp', 'kr', 'my', 'ph', 'sg', 'th', 'vn']
    if 'country' in df_pmd_dump.columns:
        original_dump_count = len(df_pmd_dump)
        df_pmd_dump = df_pmd_dump[
            ~df_pmd_dump['country'].astype(str).str.strip().str.lower().isin(excluded_countries)
        ].copy()
        logging.info(f"Filtered out {original_dump_count - len(df_pmd_dump)} records from PMD Dump based on excluded countries.")
    else:
        logging.warning("PMD Dump file does not contain a 'country' column for exclusion filtering.")

    # --- Validate essential columns for PMD Lookup (after dropping/filtering) ---
    required_central_cols = ['valid_from', 'supplier_name', 'status', 'assigned']
    for col in required_central_cols:
        if col not in df_central_pmd.columns:
            return False, f"Missing required column '{col}' in PMD Central file after cleaning. Please check rules.", None

    required_dump_cols = ['valid_from', 'supplier_name']
    for col in required_dump_cols:
        if col not in df_pmd_dump.columns:
            return False, f"Missing required column '{col}' in PMD Dump file after cleaning. Please check rules.", None

    # --- Pre-processing for lookup keys ---
    # Standardize 'Valid From' dates to a comparable format (e.g., YYYY-MM-DD string)
    df_central_pmd['valid_from_key'] = pd.to_datetime(df_central_pmd['valid_from'], errors='coerce').dt.strftime('%Y-%m-%d').fillna('')
    df_pmd_dump['valid_from_key'] = pd.to_datetime(df_pmd_dump['valid_from'], errors='coerce').dt.strftime('%Y-%m-%d').fillna('')

    df_central_pmd['supplier_name_key'] = df_central_pmd['supplier_name'].astype(str).str.strip().str.lower()
    df_pmd_dump['supplier_name_key'] = df_pmd_dump['supplier_name'].astype(str).str.strip().str.lower()

    # Create composite key for central file for lookup
    df_central_pmd['comp_key'] = df_central_pmd['valid_from_key'] + '__' + df_central_pmd['supplier_name_key']
    
    # Filter central file to only include 'Hold' records for direct lookup and deduplicate
    # Only records with 'hold' status will be available for matching PMD Dump records
    df_central_hold_only = df_central_pmd[
        df_central_pmd['status'].astype(str).str.strip().str.lower() == 'hold'
    ].copy()

    # Deduplicate `df_central_hold_only` by `comp_key` if there are multiple 'Hold' for the same key.
    # Keep the first one encountered (or apply a specific prioritization if needed).
    df_central_hold_only_deduped = df_central_hold_only.drop_duplicates(subset=['comp_key'], keep='first').copy()
    
    # Set index for efficient lookup for 'Hold' status matches
    central_hold_lookup = df_central_hold_only_deduped.set_index('comp_key')
    
    logging.info(f"Central file prepared for 'Hold' status lookup with {len(central_hold_lookup)} unique 'Hold' records.")

    # Create composite key for dump file
    df_pmd_dump['comp_key'] = df_pmd_dump['valid_from_key'] + '__' + df_pmd_dump['supplier_name_key']

    # --- Core Lookup Logic ---
    final_pmd_records = [] # Will hold records from PMD Dump that are 'New' or 'Hold'

    for index, row in df_pmd_dump.iterrows():
        dump_comp_key = row['comp_key']

        if dump_comp_key in central_hold_lookup.index:
            # Match found in `central_hold_lookup`, so its status is 'Hold'
            central_record = central_hold_lookup.loc[dump_comp_key]
            
            new_record = {k: v for k, v in row.drop(['comp_key', 'valid_from_key', 'supplier_name_key']).items()}
            new_record['Status'] = 'Hold'
            new_record['Assigned'] = str(central_record['assigned']).strip() # Get assigned from central 'Hold' record
            final_pmd_records.append(new_record)
            logging.debug(f"PMD Dump record {dump_comp_key} set to 'Hold' (matched central 'Hold' record).")
        else:
            # No match found in central_hold_lookup. This means either:
            # 1. It's a truly new record (not in central at all).
            # 2. It matched a central record with a status other than 'Hold' (e.g., 'Approved', 'New').
            # According to the clarified logic: "if there is no match then make the status as new"
            # This implies if it doesn't match an *already 'Hold'* record in central, it's 'New'.

            new_record = {k: v for k, v in row.drop(['comp_key', 'valid_from_key', 'supplier_name_key']).items()}
            new_record['Status'] = 'New'
            new_record['Assigned'] = '' # No assigned for 'New' records
            final_pmd_records.append(new_record)
            logging.debug(f"PMD Dump record {dump_comp_key} set to 'New' (no match in central 'Hold' records).")

    df_output = pd.DataFrame(final_pmd_records) # This df now contains all 'New' and 'Hold' records derived from PMD Dump.

    # --- Final formatting and column reordering for output ---
    if not df_output.empty:
        # Map cleaned dump column names back to original for the PMD_OUTPUT_COLUMNS
        # Create a mapping from cleaned column names to desired output column names
        cleaned_to_output_map = {clean_column_names(pd.DataFrame(columns=[col])).columns[0]: col for col in PMD_OUTPUT_COLUMNS}
        
        # Rename columns in df_output using this map
        # Only rename columns that actually exist in df_output AND have a mapping
        cols_to_rename_back = {cleaned_col: original_output_col for cleaned_col, original_output_col in cleaned_to_output_map.items() if cleaned_col in df_output.columns and original_output_col not in ['Status', 'Assigned']}
        df_output.rename(columns=cols_to_rename_back, inplace=True)

        # Ensure 'Valid From' is formatted to MM/DD/YYYY
        if 'Valid From' in df_output.columns:
            df_output['Valid From'] = format_date_to_mdyyyy(df_output['Valid From'])
        
        # Add any missing output columns and reorder
        for col in PMD_OUTPUT_COLUMNS:
            if col not in df_output.columns:
                df_output[col] = '' # Add missing columns as empty string
            
            # Ensure all object columns are handled (fillna with empty string)
            if df_output[col].dtype == 'object':
                df_output[col] = df_output[col].fillna('')

        # Final reorder of columns
        df_output = df_output[PMD_OUTPUT_COLUMNS]
    else:
        # If df_output is empty, ensure it still has the correct columns for an empty output file
        df_output = pd.DataFrame(columns=PMD_OUTPUT_COLUMNS)

    today_str = datetime.now().strftime("%d_%m_%Y_%H%M%S")
    pmd_output_filename = f'PMD_Lookup_Result_{today_str}.xlsx'
    pmd_output_file_path = os.path.join(temp_dir, pmd_output_filename)

    try:
        df_output.to_excel(pmd_output_file_path, index=False)
        logging.info(f"PMD Lookup result saved to: {pmd_output_file_path}")
    except Exception as e:
        return False, f"Error saving PMD Lookup result file: {e}", None

    logging.info("--- PMD Lookup Process Complete ---")
    return True, 'PMD Lookup processing successful', pmd_output_file_path


# --- Flask Routes ---

@app.route('/', methods=['GET'])
def index():
    # Clear session download paths when returning to index to avoid stale links
    session.pop('consolidated_output_path', None)
    session.pop('central_output_path', None)
    session.pop('pmd_lookup_output_path', None) # Add for PMD lookup

    # Ensure any residual temp_dir is cleaned up when starting fresh
    temp_dir = session.get('temp_dir')
    if temp_dir and os.path.exists(temp_dir):
        try:
            shutil.rmtree(temp_dir)
            logging.info(f"Cleaned up residual temporary directory: {temp_dir}")
        except OSError as e:
            logging.error(f"Error removing residual temporary directory {temp_dir}: {e}")
    session.pop('temp_dir', None) # Clear session's temp_dir after cleanup attempt

    return render_template('index.html')

@app.route('/process_b_segment_allocation', methods=['POST']) # Specific route for B-Segment
def route_process_b_segment_allocation():
    temp_dir = None
    try:
        temp_dir = tempfile.mkdtemp(dir=tempfile.gettempdir())
        session['temp_dir'] = temp_dir # Store temp_dir in session

        session.pop('consolidated_output_path', None)
        session.pop('central_output_path', None)
        # We don't clear pmd_lookup_output_path here as it's a separate process

        success, message, output_path = process_b_segment_allocation_core(request.files, temp_dir)

        if not success:
            flash(message, 'error')
            return redirect(url_for('index'))

        return render_template('index.html',
                               b_segment_download_link=url_for('download_file', filename=os.path.basename(output_path)))

    except Exception as e:
        flash(f'An unhandled error occurred during B-Segment processing: {e}', 'error')
        logging.error(f'Unhandled error in /process_b_segment_allocation: {e}', exc_info=True)
        return redirect(url_for('index'))
    finally:
        # Keep temp_dir for download, cleanup on /download or /cleanup_session
        pass

@app.route('/process_pmd_lookup', methods=['POST']) # New route for PMD Lookup
def route_process_pmd_lookup():
    temp_dir = None
    try:
        temp_dir = tempfile.mkdtemp(dir=tempfile.gettempdir())
        session['temp_dir'] = temp_dir # Store temp_dir in session

        session.pop('pmd_lookup_output_path', None)
        # We don't clear b_segment related paths here as it's a separate process

        success, message, output_path = process_pmd_lookup_core(request.files, temp_dir)

        if not success:
            flash(message, 'error')
            return redirect(url_for('index'))

        session['pmd_lookup_output_path'] = output_path # Store for download
        return render_template('index.html',
                               pmd_lookup_download_link=url_for('download_file', filename=os.path.basename(output_path)))

    except Exception as e:
        flash(f'An unhandled error occurred during PMD Lookup processing: {e}', 'error')
        logging.error(f'Unhandled error in /process_pmd_lookup: {e}', exc_info=True)
        return redirect(url_for('index'))
    finally:
        # Keep temp_dir for download, cleanup on /download or /cleanup_session
        pass


@app.route('/download/<filename>', methods=['GET'])
def download_file(filename):
    file_path_in_temp = None
    temp_dir = session.get('temp_dir')

    logging.info(f"DEBUG: Download requested for filename: {filename}")
    logging.info(f"DEBUG: Session temp_dir: {temp_dir}")

    if not temp_dir:
        logging.error("DEBUG: temp_dir not found in session.")
        flash('File not found for download or session expired. Please re-run the process.', 'error')
        return redirect(url_for('index'))

    # Check against the specific download paths for B-Segment and PMD Lookup
    b_segment_session_path = session.get('central_output_path')
    pmd_lookup_session_path = session.get('pmd_lookup_output_path')

    if b_segment_session_path and os.path.basename(b_segment_session_path) == filename:
        file_path_in_temp = os.path.join(temp_dir, filename)
        logging.info(f"DEBUG: Matched B-Segment central file. Reconstructed path: {file_path_in_temp}")
    elif pmd_lookup_session_path and os.path.basename(pmd_lookup_session_path) == filename:
        file_path_in_temp = os.path.join(temp_dir, filename)
        logging.info(f"DEBUG: Matched PMD Lookup result file. Reconstructed path: {file_path_in_temp}")
    else:
        logging.error(f"DEBUG: Filename '{filename}' did not match any known session output files.")

    if file_path_in_temp and os.path.exists(file_path_in_temp):
        logging.info(f"DEBUG: File '{file_path_in_temp}' exists. Attempting to send.")
        try:
            response = send_file(
                file_path_in_temp,
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                as_attachment=True,
                download_name=filename
            )
            return response
        except Exception as e:
            logging.error(f"ERROR: Exception while sending file '{file_path_in_temp}': {e}", exc_info=True)
            flash(f'Error providing download: {e}. Please try again.', 'error')
            return redirect(url_for('index'))
    else:
        logging.error(f"DEBUG: File '{filename}' not found for download or session data missing/expired. Full path attempted: {file_path_in_temp}")
        flash('File not found for download or session expired. Please re-run the process.', 'error')
        return redirect(url_for('index'))

@app.route('/cleanup_session', methods=['GET'])
def cleanup_session():
    temp_dir = session.get('temp_dir')
    if temp_dir and os.path.exists(temp_dir):
        try:
            shutil.rmtree(temp_dir)
            logging.info(f"Cleaned up temporary directory: {temp_dir}")
            flash('Temporary files cleaned up.', 'info')
        except OSError as e:
            logging.error(f"ERROR: Error removing temporary directory {temp_dir}: {e}")
            flash(f'Error cleaning up temporary files: {e}', 'error')
    session.pop('temp_dir', None)
    session.pop('consolidated_output_path', None)
    session.pop('central_output_path', None)
    session.pop('pmd_lookup_output_path', None) # Clear PMD lookup path as well
    return redirect(url_for('index'))

if __name__ == '__main__':
    app.run(debug=True)
