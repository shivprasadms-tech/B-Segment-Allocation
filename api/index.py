import os
import pandas as pd
from datetime import datetime
import warnings
import shutil
import tempfile
import re
from flask import Flask, request, render_template, redirect, url_for, send_file, flash, session
from werkzeug.utils import secure_filename
import io # Added for PMD process
import logging # Added for PMD process

warnings.filterwarnings('ignore')

# Configure logging for better error visibility
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

BASE_DIR = os.path.dirname(os.path.abspath(__file__)) # This would be 'your-vercel-project/api'
template_dir = os.path.join(BASE_DIR, '..', 'templates') # This would point to 'your-vercel-project/templates'
static_dir = os.path.join(BASE_DIR, '..', 'static')     # This would point to 'your-vercel-project/static'

app = Flask(__name__, template_folder=template_dir, static_folder=static_dir)

# Pass static_folder to the Flask constructor
app = Flask(__name__, template_folder=template_dir, static_folder=static_dir)
app.secret_key = os.environ.get('FLASK_SECRET_KEY', 'a_strong_default_secret_key_for_local_dev_only_change_this_in_production')

# --- Global Variables ---
CONSOLIDATED_OUTPUT_COLUMNS = [
    'Barcode', 'Processor', 'Channel', 'Category', 'Company code', 'Region',
    'Vendor number', 'Vendor Name', 'Status', 'Received Date', 'Re-Open Date',
    'Allocation Date', 'Clarification Date', 'Completion Date', 'Requester',
    'Remarks', 'Aging', 'Today'
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

def consolidate_data_process(df_pisa, df_esm, df_pm7, df_workon, df_rgpa, df_smd, consolidated_output_file_path):
    """
    Reads PISA, ESM, PM7, Workon, RGPA, and SMD Excel files (now passed as DFs), filters PISA & RGPA,
    consolidates data, and saves it to a new Excel file.
    """
    logging.info("Starting data consolidation process for B-Segment...")
    logging.info("All input DataFrames loaded successfully!")

    df_pisa = clean_column_names(df_pisa.copy())
    df_esm = clean_column_names(df_esm.copy())
    df_pm7 = clean_column_names(df_pm7.copy())
    
    df_workon_cleaned = clean_column_names(df_workon.copy()) if df_workon is not None and not df_workon.empty else pd.DataFrame()
    df_rgpa_cleaned = clean_column_names(df_rgpa.copy()) 
    df_smd_cleaned = clean_column_names(df_smd.copy()) if df_smd is not None and not df_smd.empty else pd.DataFrame()

    all_consolidated_rows = []
    today_date = datetime.now()
    today_date_formatted = today_date.strftime("%m/%d/%Y") 

    # --- PISA Processing ---
    allowed_pisa_users = ["Goswami Sonali", "Patil Jayapal Gowd", "Ranganath Chilamakuri","Sridhar Divya","Sunitha S","Varunkumar N"]
    if 'assigned_user' in df_pisa.columns:
        original_pisa_count = len(df_pisa)
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
                'Company code': row.get('company_code'),
                'Vendor number': row.get('vendor_number'),
                'Received Date': row.get('received_date'),
                'Completion Date': None, 'Status': None , 'Today': today_date_formatted, 'Channel': 'PISA',
                'Vendor Name': row.get('vendor_name'),
                'Re-Open Date': None, 'Allocation Date': today_date_formatted,
                'Requester': None, 'Clarification Date': None, 'Aging': None, 'Remarks': None,
                'Region': None,
                'Processor': None, 'Category': None
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
                'Status': row.get('state'),
                'Requester': row.get('opened_by'),
                'Completion Date': row.get('closed') if pd.notna(row.get('closed')) else None,
                'Re-Open Date': row.get('updated') if (row.get('state') or '').lower() == 'reopened' else None,
                'Today': today_date_formatted, 'Remarks': row.get('short_description'),
                'Channel': 'ESM',
                'Company code': None,'Vendor Name': None,
                'Vendor number': None, 'Allocation Date': today_date_formatted,
                'Clarification Date': None, 'Aging': None,
                'Region': None,
                'Processor': None,
                'Category': None
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
                'Vendor Name': row.get('vendor_name'),
                'Vendor number': row.get('vendor_number'),
                'Received Date': row.get('received_date'),
                'Status': row.get('task'),
                'Today': today_date_formatted,
                'Channel': 'PM7',
                'Company code': row.get('company_code'),
                'Re-Open Date': None,
                'Allocation Date': today_date_formatted, 'Completion Date': None, 'Requester': None,
                'Clarification Date': None, 'Aging': None,
                'Region': None,
                'Processor': None, 'Category': None
            }
            all_consolidated_rows.append(new_row)
        logging.info(f"Collected {len(df_pm7)} rows from PM7.")

    # --- Workon P71 Processing (Optional) ---
    if not df_workon_cleaned.empty:
        if 'key' not in df_workon_cleaned.columns:
            logging.error("Error: 'key' column not found in Workon file (after cleaning). Skipping Workon processing.")
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
                    'Today': today_date_formatted
                }
                all_consolidated_rows.append(new_row)
            logging.info(f"Collected {len(df_workon_cleaned)} rows from Workon.")
    else:
        logging.info("Workon file not provided or is empty. Skipping Workon processing.")

    # --- RGPA Processing ---
    if df_rgpa_cleaned.empty:
        logging.warning("Warning: RGPA file is empty. Skipping RGPA processing.")
    elif 'key' not in df_rgpa_cleaned.columns:
        logging.error("Error: 'key' column not found in RGPA file (after cleaning). Skipping RGPA processing.")
    else:
        original_rgpa_count = len(df_rgpa_cleaned)
        df_rgpa_filtered = df_rgpa_cleaned.copy() 
        if 'current_assignee' in df_rgpa_cleaned.columns:
            filter_mask = df_rgpa_cleaned['current_assignee'].astype(str).str.contains("VMD GS OSP-NA (GS/OMD-APAC)", na=False)
            df_rgpa_filtered = df_rgpa_cleaned[filter_mask].copy()
            logging.info(f"\nRGPA file filtered. Original records: {original_rgpa_count}, Records after filter: {len(df_rgpa_filtered)}")
        else:
            logging.warning("Warning: 'current_assignee' column not found in RGPA file (after cleaning). No filter applied for RGPA.")
            
        df_rgpa_filtered['key'] = df_rgpa_filtered['key'].astype(str)
        for index, row in df_rgpa_filtered.iterrows():
            new_row = {
                'Barcode': row['key'],
                'Processor': 'Divya',
                'Channel': 'RGPA',    
                'Category': None,     
                'Company code': row.get('company_code'),
                'Region': None,       
                'Vendor number': None,
                'Vendor Name': None,  
                'Status': None,       
                'Received Date': row.get('updated'),
                'Re-Open Date': None, 
                'Allocation Date': today_date_formatted,
                'Clarification Date': None,
                'Completion Date': None,
                'Requester': None,    
                'Remarks': row.get('summary'),
                'Aging': None,        
                'Today': today_date_formatted   
            }
            all_consolidated_rows.append(new_row)
        logging.info(f"Collected {len(df_rgpa_filtered)} rows from RGPA.")

    # --- SMD Processing (New Channel) ---
    if not df_smd_cleaned.empty:
        for index, row in df_smd_cleaned.iterrows():
            new_row = {
                'Barcode': None, # As no explicit barcode column was given for SMD, keeping it None.
                'Processor': None,
                'Channel': 'SMD',    
                'Category': None, # No Category mapping specified for SMD
                'Company code': row.get('ekorg'),
                'Region': row.get('material_field'),       
                'Vendor number': row.get('pmd_sno'), # Cleaned column name for PMD-SNO
                'Vendor Name': row.get('supplier_name'),  
                'Status': None,       
                'Received Date': row.get('request_date'),
                'Re-Open Date': None, 
                'Allocation Date': today_date_formatted, # As requested
                'Clarification Date': None,
                'Completion Date': None,
                'Requester': row.get('requested_by'), # Cleaned column name for Requested by
                'Remarks': None, # No Remarks mapping specified for SMD
                'Aging': None,        
                'Today': today_date_formatted # As requested
            }
            all_consolidated_rows.append(new_row)
        logging.info(f"Collected {len(df_smd_cleaned)} rows from SMD.")
    else:
        logging.info("SMD file not provided or is empty. Skipping SMD processing.")


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
        logging.info(f"Consolidated file saved to: {consolidated_output_file_path}")
    except Exception as e:
        return False, f"Error saving consolidated file: {e}"
    logging.info("--- Consolidated Data Process Complete for B-Segment ---")
    return True, df_consolidated

def process_central_file_step2_update_existing(consolidated_df, central_file_input_path):
    """
    Step 2: Updates status of *existing* central file records based on consolidated data.
    SMD records do NOT participate in this step as per requirements.
    """
    logging.info(f"\n--- Starting Central File Status Processing (Step 2: Update Existing Barcodes) for B-Segment ---")

    try:
        converters = {'Barcode': str, 'Vendor number': str, 'Company code': str}
        df_central = pd.read_excel(central_file_input_path, converters=converters, keep_default_na=False)
        df_central_cleaned = clean_column_names(df_central.copy())

        logging.info("Consolidated (DF) and Central (file) loaded successfully for Step 2!")
    except Exception as e:
        return False, f"Error loading Consolidated (DF) or Central (file) for processing (Step 2): {e}"

    if 'Barcode' not in consolidated_df.columns:
        return False, "Error: 'Barcode' column not found in the consolidated file. Cannot proceed with central file processing (Step 2)."
    if 'barcode' not in df_central_cleaned.columns or 'status' not in df_central_cleaned.columns:
        return False, "Error: 'barcode' or 'status' column not found in the central file after cleaning. Cannot update status (Step 2)."

    consolidated_df['Barcode'] = consolidated_df['Barcode'].astype(str)
    df_central_cleaned['barcode'] = df_central_cleaned['barcode'].astype(str)

    df_central_cleaned['Barcode_compare'] = df_central_cleaned['barcode']

    # SMD records are explicitly excluded from influencing existing central file status
    channels_for_status_change = ['PISA', 'ESM', 'PM7'] 
    consolidated_barcodes_for_status_change_set = set(
        consolidated_df[consolidated_df['Channel'].isin(channels_for_status_change)]['Barcode'].unique()
    )
    logging.info(f"Found {len(consolidated_barcodes_for_status_change_set)} unique barcodes from {', '.join(channels_for_status_change)} in consolidated file for Step 2 status updates.")

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

    logging.info(f"Updated 'status' column in central file for Step 2 for {len(df_central_cleaned)} records.")

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
    logging.info(f"--- Central File Status Processing (Step 2) Complete for B-Segment ---")
    return True, df_central_cleaned

def process_central_file_step3_final_merge_and_needs_review(consolidated_df, updated_existing_central_df, final_central_output_file_path, df_pisa_original, df_esm_original, df_pm7_original, df_workon_original, df_rgpa_original, df_smd_original, region_mapping_df):
    """
    Step 3: Handles barcodes present only in consolidated (adds them as new)
            and barcodes present only in central (marks them as 'Needs Review' if not 'Completed').
            Also performs region mapping and final column reordering.
    SMD records will contribute to new additions, but not to 'Needs Review' for existing central records.
    """
    logging.info(f"\n--- Starting Central File Status Processing (Step 3: Final Merge & Needs Review) for B-Segment ---")

    df_pisa_lookup = clean_column_names(df_pisa_original.copy())
    df_esm_lookup = clean_column_names(df_esm_original.copy())
    df_pm7_lookup = clean_column_names(df_pm7_original.copy())
    
    df_workon_lookup = clean_column_names(df_workon_original.copy()) if df_workon_original is not None and not df_workon_original.empty else pd.DataFrame()
    df_rgpa_lookup = clean_column_names(df_rgpa_original.copy())
    df_smd_lookup = clean_column_names(df_smd_original.copy()) if df_smd_original is not None and not df_smd_original.empty else pd.DataFrame()

    df_pisa_indexed = pd.DataFrame()
    if 'barcode' in df_pisa_lookup.columns:
        df_pisa_lookup['barcode'] = df_pisa_lookup['barcode'].astype(str)
        df_pisa_indexed = df_pisa_lookup.set_index('barcode')
        logging.info(f"PISA lookup indexed by 'barcode'.")
    else:
        logging.warning("Warning: 'barcode' column not found in cleaned PISA lookup. Cannot perform PISA lookups.")

    df_esm_indexed = pd.DataFrame()
    if 'barcode' in df_esm_lookup.columns:
        df_esm_lookup['barcode'] = df_esm_lookup['barcode'].astype(str)
        df_esm_indexed = df_esm_lookup.set_index('barcode')
        logging.info(f"ESM lookup indexed by 'barcode'.")
    else:
        logging.warning("Warning: 'barcode' column not found in cleaned ESM lookup. Cannot perform ESM lookups.")

    df_pm7_indexed = pd.DataFrame()
    if 'barcode' in df_pm7_lookup.columns:
        df_pm7_lookup['barcode'] = df_pm7_lookup['barcode'].astype(str)
        df_pm7_indexed = df_pm7_lookup.set_index('barcode')
        logging.info(f"PM7 lookup indexed by 'barcode'.")
    else:
        logging.warning("Warning: 'barcode' column not found in cleaned PM7 lookup. Cannot perform PM7 lookups.")

    df_workon_indexed = pd.DataFrame()
    if not df_workon_lookup.empty: 
        if 'key' in df_workon_lookup.columns:
            df_workon_lookup['key'] = df_workon_lookup['key'].astype(str)
            df_workon_indexed = df_workon_lookup.set_index('key')
            logging.info(f"Workon lookup indexed by 'key'.")
        else:
            logging.warning("Warning: 'key' column not found in cleaned Workon lookup. Cannot perform Workon lookups.")
    else:
        logging.info("Workon lookup not created as file was not provided or empty.")

    df_rgpa_indexed = pd.DataFrame()
    if not df_rgpa_lookup.empty: 
        if 'key' in df_rgpa_lookup.columns:
            if 'current_assignee' in df_rgpa_lookup.columns:
                df_rgpa_lookup_filtered = df_rgpa_lookup[
                    df_rgpa_lookup['current_assignee'].astype(str).str.contains("VMD GS OSP-NA (GS/OMD-APAC)", na=False)
                ].copy()
                df_rgpa_lookup_filtered['key'] = df_rgpa_lookup_filtered['key'].astype(str)
                df_rgpa_indexed = df_rgpa_lookup_filtered.set_index('key')
                logging.info(f"RGPA lookup (filtered by assignee) indexed by 'key'.")
            else:
                logging.warning("Warning: 'current_assignee' column not found in RGPA lookup. RGPA lookup is not filtered by assignee.")
                df_rgpa_lookup['key'] = df_rgpa_lookup['key'].astype(str)
                df_rgpa_indexed = df_rgpa_lookup.set_index('key')
        else:
            logging.warning("Warning: 'key' column not found in cleaned RGPA lookup. Cannot perform RGPA lookups.")
    else:
        logging.info("Warning: RGPA lookup was empty. No RGPA lookups will be performed for new records.")

    # SMD lookup for enriching new records
    df_smd_indexed = pd.DataFrame()
    if not df_smd_lookup.empty:
        logging.info("SMD lookup not indexed as a 'Barcode' equivalent column was not specified for enrichment.")
    else:
        logging.info("SMD lookup not created as file was not provided or empty.")

    if 'Barcode' not in consolidated_df.columns:
        return False, "Error: 'Barcode' column not found in the consolidated file. Cannot proceed with final central file processing (Step 3)."
    if 'Barcode' not in updated_existing_central_df.columns or 'Status' not in updated_existing_central_df.columns:
        return False, "Error: 'Barcode' or 'Status' column not found in the updated central file. Cannot update status (Step 3)."

    # Barcodes from all channels (including SMD) that are new to the central file will be added
    consolidated_barcodes_set = set(consolidated_df['Barcode'].astype(str).replace('None', '').unique()) # Handle potential None barcodes from SMD
    central_barcodes_set = set(updated_existing_central_df['Barcode'].astype(str).unique())

    barcodes_to_add = consolidated_barcodes_set - central_barcodes_set
    # Filter out empty strings if Barcode is None for some channels
    barcodes_to_add = {b for b in barcodes_to_add if b != ''}

    logging.info(f"Found {len(barcodes_to_add)} new barcodes in consolidated file to add to central.")

    # When adding new records, we use the consolidated_df, which already has the SMD data mapped
    df_new_records_from_consolidated = consolidated_df[consolidated_df['Barcode'].isin(barcodes_to_add) | 
                                                      ((consolidated_df['Barcode'].isna() | (consolidated_df['Barcode'] == '')) & 
                                                       consolidated_df['Channel'] == 'SMD') # Add SMD records with no barcode
                                                      ].copy()

    all_new_central_rows_data = []
    today_date_formatted = datetime.now().strftime("%m/%d/%Y") 

    for index, row_consolidated in df_new_records_from_consolidated.iterrows():
        barcode = row_consolidated['Barcode']
        channel = row_consolidated['Channel']

        # Start with values already in consolidated_df, which includes basic SMD mappings
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

        # Additional lookups for other channels if they were not fully populated during initial consolidation
        # (This part is mostly redundant if consolidate_data_process already populates fully,
        # but kept for robustness if new records need more detailed enrichment from original sources)
        if channel == 'PISA' and not df_pisa_indexed.empty and barcode in df_pisa_indexed.index:
            pisa_row = df_pisa_indexed.loc[barcode]
            vendor_name = pisa_row.get('vendor_name', vendor_name)
            vendor_number = pisa_row.get('vendor_number', vendor_number)
            company_code = pisa_row.get('company_code', company_code)
            received_date = pisa_row.get('received_date', received_date)

        elif channel == 'ESM' and not df_esm_indexed.empty and barcode in df_esm_indexed.index:
            esm_row = df_esm_indexed.loc[barcode]
            company_code = esm_row.get('company_code', company_code)
            category = esm_row.get('subcategory', category)
            vendor_name = esm_row.get('vendor_name', vendor_name)
            vendor_number = esm_row.get('vendor_number', vendor_number)
            received_date = esm_row.get('received_date', received_date)
            requester = esm_row.get('opened_by', requester)
            status = esm_row.get('state', status)
            remarks = esm_row.get('short_description', remarks)

        elif channel == 'PM7' and not df_pm7_indexed.empty and barcode in df_pm7_indexed.index:
            pm7_row = df_pm7_indexed.loc[barcode]
            vendor_name = pm7_row.get('vendor_name', vendor_name)
            vendor_number = pm7_row.get('vendor_number', vendor_number)
            company_code = pm7_row.get('company_code', company_code)
            received_date = pm7_row.get('received_date', received_date)
            status = pm7_row.get('task', status)

        elif channel == 'Workon' and not df_workon_indexed.empty and barcode in df_workon_indexed.index:
            workon_row = df_workon_indexed.loc[barcode]
            vendor_name = workon_row.get('name', vendor_name)
            vendor_number = workon_row.get('vendor_number', vendor_number)
            company_code = workon_row.get('company_code', company_code)
            received_date = workon_row.get('updated', received_date)
            processor = 'Jayapal' 
            category = workon_row.get('action', category)
            region = workon_row.get('country', region)
            status = workon_row.get('status', status)
            requester = workon_row.get('applicant', requester)
            remarks = workon_row.get('summary', remarks)
        
        elif channel == 'RGPA' and not df_rgpa_indexed.empty and barcode in df_rgpa_indexed.index:
            rgpa_row = df_rgpa_indexed.loc[barcode]
            processor = 'Divya' 
            company_code = rgpa_row.get('company_code', company_code)
            received_date = rgpa_row.get('updated', received_date)
            remarks = rgpa_row.get('summary', remarks)
        
        # SMD records already have their mapped columns from consolidated_df
        # No further lookup based on barcode is performed here for SMD as per current understanding
        
        new_central_row_data = {
            'Barcode': barcode if barcode != '' else None, # Store empty string as None for Barcode if it came from SMD with no barcode
            'Processor': processor if processor is not None else '',
            'Channel': channel,
            'Category': category if category is not None else '',
            'Company code': company_code if company_code is not None else '',
            'Region': region if region is not None else '', 
            'Vendor number': vendor_number if vendor_number is not None else '',
            'Vendor Name': vendor_name if vendor_name is not None else '',
            'Status': status if status is not None else 'New', 
            'Received Date': received_date, 
            'Re-Open Date': None,
            'Allocation Date': today_date_formatted, 
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
            # For Barcode, if it's None, we want to keep it empty string, not 'nan'
            df_new_central_rows[col] = df_new_central_rows[col].astype(str).replace('nan', '')

    # Barcodes for 'Needs Review' should EXCLUDE SMD records because SMD has no barcode influence
    consolidated_barcodes_for_needs_review_set = set(
        consolidated_df[~consolidated_df['Channel'].isin(['SMD'])]['Barcode'].astype(str).replace('None', '').unique()
    )
    central_barcodes_set_for_needs_review = set(updated_existing_central_df['Barcode'].astype(str).unique())

    barcodes_for_needs_review = central_barcodes_set_for_needs_review - consolidated_barcodes_for_needs_review_set
    logging.info(f"Found {len(barcodes_for_needs_review)} barcodes in central not in PISA/ESM/PM7/Workon/RGPA consolidated sources.")

    df_final_central = updated_existing_central_df.copy()

    needs_review_barcode_mask = df_final_central['Barcode'].isin(barcodes_for_needs_review)
    is_not_completed_status_mask = ~df_final_central['Status'].astype(str).str.strip().str.lower().eq('completed')
    final_needs_review_condition = needs_review_barcode_mask & is_not_completed_status_mask

    df_final_central.loc[final_needs_review_condition, 'Status'] = 'Needs Review'
    logging.info(f"Updated {final_needs_review_condition.sum()} records to 'Needs Review' where status was not 'Completed'.")

    for col in CONSOLIDATED_OUTPUT_COLUMNS:
        if col not in df_final_central.columns:
            df_final_central[col] = None
    df_final_central = df_final_central[CONSOLIDATED_OUTPUT_COLUMNS]

    # Combine existing central and newly added rows
    df_final_central = pd.concat([df_final_central, df_new_central_rows], ignore_index=True)

    # --- Handle blank Company Code for PM7 channel ---
    logging.info("\n--- Applying PM7 Company Code population logic ---")
    if 'Channel' in df_final_central.columns and 'Company code' in df_final_central.columns and 'Barcode' in df_final_central.columns:
        pm7_blank_cc_mask = (df_final_central['Channel'] == 'PM7') & \
                            (df_final_central['Company code'].astype(str).replace('nan', '').str.strip() == '')

        df_final_central.loc[pm7_blank_cc_mask, 'Company code'] = \
            df_final_central.loc[pm7_blank_cc_mask, 'Barcode'].astype(str).str[:4]

        logging.info(f"Populated Company Code for {pm7_blank_cc_mask.sum()} PM7 records based on Barcode.")
    else:
        logging.warning("Warning: 'Channel', 'Company code', or 'Barcode' columns missing. Skipping PM7 Company Code population logic.")

    # --- Apply Region Mapping ---
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
                df_final_central['Company code_lookup'] = df_final_central['Company code'].astype(str).str.strip().str.upper().str[:4]
                
                new_mapped_regions = df_final_central['Company code_lookup'].map(region_map)

                if 'Region' not in df_final_central.columns:
                    df_final_central['Region'] = ''
                
                df_final_central['Region'] = df_final_central['Region'].replace('', pd.NA) 

                df_final_central['Region'] = df_final_central['Region'].fillna(new_mapped_regions)

                df_final_central['Region'] = df_final_central['Region'].astype(str).replace('nan', '')
                
                df_final_central = df_final_central.drop(columns=['Company code_lookup'])
                logging.info("Region mapping applied successfully. Existing regions prioritized.")
            else:
                logging.warning("Warning: 'Company code' column not found in final central DataFrame. Cannot apply region mapping.")
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
        logging.info(f"Final central file (after Step 3) saved to: {final_central_output_file_path}")
        logging.info(f"Total rows in final central file (after Step 3): {len(df_final_central)}")
    except Exception as e:
        return False, f"Error saving final central file (after Step 3): {e}"
    logging.info(f"--- Central File Status Processing (Step 3) Complete for B-Segment ---")
    return True, "Central file processing (Step 3) successful"


# --- B-Segment Allocation Processing Function ---
def process_b_segment_allocation(request_files, temp_dir):
    """Encapsulates the B-Segment Allocation logic."""
    logging.info("Starting B-Segment Allocation Process...")

    REGION_MAPPING_FILE_PATH = os.path.join(BASE_DIR, 'company_code_region_mapping.xlsx') 

    uploaded_files = {}
    required_file_keys = ['pisa_file', 'esm_file', 'pm7_file', 'rgpa_file', 'b_segment_central_file'] # Renamed for clarity
    optional_file_keys = ['workon_file', 'smd_file']

    # Handle required files
    for key in required_file_keys:
        file = request_files.get(key)
        if not file or file.filename == '':
            return False, f'Missing required file: "{key}". Please upload all required files.', None
        if allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file_path = os.path.join(temp_dir, filename)
            file.save(file_path)
            uploaded_files[key] = file_path
            flash(f'File "{filename}" uploaded successfully for B-Segment.', 'info')
        else:
            return False, f'Invalid file type for "{key}". Please upload an .xlsx file.', None
    
    # Handle optional files
    for key in optional_file_keys:
        file = request_files.get(key)
        if file and file.filename != '': 
            if allowed_file(file.filename):
                filename = secure_filename(file.filename)
                file_path = os.path.join(temp_dir, filename)
                file.save(file_path)
                uploaded_files[key] = file_path
                flash(f'Optional file "{filename}" uploaded successfully for B-Segment.', 'info')
            else:
                flash(f'Invalid file type for optional file "{key}". It must be an .xlsx file.', 'warning')
                uploaded_files[key] = None 
        else:
            logging.info(f'Optional file "{key}" not provided for B-Segment. Continuing without it.')
            uploaded_files[key] = None 

    pisa_file_path = uploaded_files['pisa_file']
    esm_file_path = uploaded_files['esm_file']
    pm7_file_path = uploaded_files['pm7_file']
    workon_file_path = uploaded_files['workon_file'] 
    rgpa_file_path = uploaded_files['rgpa_file']
    smd_file_path = uploaded_files['smd_file']
    initial_central_file_input_path = uploaded_files['b_segment_central_file']

    df_pisa_original = None
    df_esm_original = None
    df_pm7_original = None
    df_workon_original = None 
    df_rgpa_original = None 
    df_smd_original = None
    df_region_mapping = pd.DataFrame() 

    try:
        df_pisa_original = pd.read_excel(pisa_file_path)
        df_esm_original = pd.read_excel(esm_file_path)
        df_pm7_original = pd.read_excel(pm7_file_path)
        df_rgpa_original = pd.read_excel(rgpa_file_path) 
        
        if workon_file_path:
            df_workon_original = pd.read_excel(workon_file_path)
        else:
            df_workon_original = pd.DataFrame() 
        
        if smd_file_path:
            df_smd_original = pd.read_excel(smd_file_path)
        else:
            df_smd_original = pd.DataFrame()

        if os.path.exists(REGION_MAPPING_FILE_PATH):
            df_region_mapping = pd.read_excel(REGION_MAPPING_FILE_PATH)
            logging.info(f"Successfully loaded region mapping file from: {REGION_MAPPING_FILE_PATH}")
        else:
            flash(f"Warning: Region mapping file not found at {REGION_MAPPING_FILE_PATH}. Region column will be empty for records relying solely on this mapping.", 'warning')
    
    except Exception as e:
        return False, f"Error loading one or more input Excel files for B-Segment: {e}. Please ensure all files are valid .xlsx formats.", None

    today_str = datetime.now().strftime("%d_%m_%Y_%H%M%S")

    # --- Step 1: Consolidate Data ---
    consolidated_output_filename = f'ConsolidatedData_B_Segment_{today_str}.xlsx'
    consolidated_output_file_path = os.path.join(temp_dir, consolidated_output_filename)
    success, result = consolidate_data_process(
        df_pisa_original, df_esm_original, df_pm7_original, df_workon_original, df_rgpa_original, df_smd_original, consolidated_output_file_path 
    )

    if not success:
        return False, f'B-Segment Consolidation Error: {result}', None
    df_consolidated = result
    flash('Data consolidation for B-Segment completed successfully!', 'success')
    session['b_segment_consolidated_output_path'] = consolidated_output_file_path

    # --- Step 2: Update existing central file records based on consolidation ---
    success, result_df = process_central_file_step2_update_existing(
        df_consolidated, initial_central_file_input_path
    )
    if not success:
        return False, f'B-Segment Central File Processing (Step 2) Error: {result_df}', None
    df_central_updated_existing = result_df

    # --- Step 3: Final Merge (Add new barcodes, mark 'Needs Review', and apply Region Mapping) ---
    final_central_output_filename = f'CentralFile_B_Segment_FinalOutput_{today_str}.xlsx'
    final_central_output_file_path = os.path.join(temp_dir, final_central_output_filename)
    success, message = process_central_file_step3_final_merge_and_needs_review(
        df_consolidated, df_central_updated_existing, final_central_output_file_path,
        df_pisa_original, df_esm_original, df_pm7_original, df_workon_original, df_rgpa_original, df_smd_original, df_region_mapping 
    )
    if not success:
        return False, f'B-Segment Central File Processing (Step 3) Error: {message}', None
    flash('B-Segment central file finalized successfully!', 'success')
    session['b_segment_central_output_path'] = final_central_output_file_path
    return True, 'B-Segment processing complete', final_central_output_file_path

# --- PMD Lookup Processing Function ---
def process_pmd_lookup(request_files, temp_dir):
    """Encapsulates the PMD Lookup logic."""
    logging.info("Starting PMD Lookup Process...")

    # -------------------- FILE VALIDATION --------------------
    pmd_central_file = request_files.get('pmd_central_file') # Renamed for clarity
    pmd_lookup_file = request_files.get('pmd_lookup_file')

    if not pmd_central_file or pmd_central_file.filename == '' or \
       not pmd_lookup_file or pmd_lookup_file.filename == '':
        return False, 'Both PMD Central File and PMD Dump File are required.', None

    if not (allowed_file(pmd_central_file.filename) and allowed_file(pmd_lookup_file.filename)):
        return False, 'Only Excel files (.xls, .xlsx) are allowed for PMD processing.', None

    try:
        # -------------------- READ FILES --------------------
        central_df = pd.read_excel(io.BytesIO(pmd_central_file.read()))
        pmd_df = pd.read_excel(io.BytesIO(pmd_lookup_file.read()))

        # -------------------- REQUIRED COLUMNS --------------------
        central_required = ['Valid From', 'Supplier Name', 'Status', 'Assigned']
        pmd_required = ['Valid From', 'Supplier Name']

        for col in central_required:
            if col not in central_df.columns:
                raise KeyError(f"PMD Central file missing column: {col}")

        for col in pmd_required:
            if col not in pmd_df.columns:
                raise KeyError(f"PMD Dump file missing column: {col}")

        # -------------------- DATE NORMALIZATION --------------------
        central_df['Valid From_dt'] = pd.to_datetime(central_df['Valid From'], errors='coerce')
        pmd_df['Valid From_dt'] = pd.to_datetime(pmd_df['Valid From'], errors='coerce')

        central_df.dropna(subset=['Valid From_dt', 'Supplier Name'], inplace=True)
        pmd_df.dropna(subset=['Valid From_dt', 'Supplier Name'], inplace=True)

        # -------------------- CREATE MATCH KEY --------------------
        central_df['comp_key'] = (
            central_df['Valid From_dt'].dt.strftime('%Y-%m-%d') + '__' +
            central_df['Supplier Name'].astype(str).str.strip()
        )

        pmd_df['comp_key'] = (
            pmd_df['Valid From_dt'].dt.strftime('%Y-%m-%d') + '__' +
            pmd_df['Supplier Name'].astype(str).str.strip()
        )

        # -------------------- CENTRAL LOOKUP (NO JOIN) --------------------
        central_lookup = central_df.set_index('comp_key')[['Status', 'Assigned']]

        # -------------------- BUSINESS LOGIC --------------------
        def determine_status(row):
            # No match → New
            if row['comp_key'] not in central_lookup.index:
                return 'New', None

            central_status = central_lookup.loc[row['comp_key'], 'Status']
            central_assigned = central_lookup.loc[row['comp_key'], 'Assigned']

            # Match + Approved → Ignore
            if isinstance(central_status, str) and central_status.lower() == 'approved':
                return None, None

            # Match + Not Approved → Hold
            return 'Hold', central_assigned

        pmd_df[['Status', 'Assigned']] = pmd_df.apply(
            lambda r: determine_status(r),
            axis=1,
            result_type='expand'
        )

        # Remove ignored rows (where status became None due to being Approved)
        final_df = pmd_df[pmd_df['Status'].notna()].copy()

        # -------------------- FORMAT & OUTPUT --------------------
        final_df['Valid From'] = final_df['Valid From_dt'].dt.strftime('%Y-%m-%d %I:%M %p')

        output_columns = [
            'Valid From', 'Bukr.', 'Type', 'EBSNO', 'Supplier Name', 'Street',
            'City', 'Country', 'Zip Code', 'Requested By', 'Pur. approver',
            'Pur. release date', 'Status', 'Assigned'
        ]

        # Select columns, ensuring they exist in final_df
        final_df = final_df[[col for col in output_columns if col in final_df.columns]]

        # -------------------- CREATE EXCEL --------------------
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            final_df.to_excel(writer, index=False, sheet_name='PMD Result')

        output.seek(0)
        
        today_str = datetime.now().strftime("%d_%m_%Y_%H%M%S")
        output_filename = f'PMD_Lookup_Result_{today_str}.xlsx'
        output_file_path = os.path.join(temp_dir, output_filename)
        
        # Save to temp file for consistent download handling
        with open(output_file_path, 'wb') as f:
            f.write(output.getvalue())

        flash('PMD Lookup processed successfully!', 'success')
        session['pmd_lookup_output_path'] = output_file_path
        return True, 'PMD Lookup processing complete', output_file_path

    except Exception as e:
        logging.error(f"Error during PMD Lookup processing: {e}", exc_info=True)
        return False, f"PMD Lookup Error: {e}", None


# --- Flask Routes ---

@app.route('/', methods=['GET'])
def index():
    # Clear session download paths when returning to index to avoid stale links
    session.pop('b_segment_central_output_path', None)
    session.pop('pmd_lookup_output_path', None)
    
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

@app.route('/process_b_segment_allocation', methods=['POST'])
def route_process_b_segment_allocation():
    temp_dir = None
    try:
        temp_dir = tempfile.mkdtemp(dir=tempfile.gettempdir())
        session['temp_dir'] = temp_dir # Store temp_dir in session

        session.pop('b_segment_consolidated_output_path', None)
        session.pop('b_segment_central_output_path', None)

        success, message, output_path = process_b_segment_allocation(request.files, temp_dir)

        if not success:
            flash(message, 'error')
            return redirect(url_for('index'))
        
        return render_template('index.html',
                               b_segment_download_link=url_for('download_file', filename=os.path.basename(output_path), process_type='b_segment'))

    except Exception as e:
        flash(f'An unhandled error occurred during B-Segment Allocation: {e}', 'error')
        logging.error(f'Unhandled error in /process_b_segment_allocation: {e}', exc_info=True)
        return redirect(url_for('index'))
    finally:
        # Keep temp_dir for download, cleanup on /download or /cleanup_session
        pass

@app.route('/process_pmd_lookup', methods=['POST'])
def route_process_pmd_lookup():
    temp_dir = None
    try:
        temp_dir = tempfile.mkdtemp(dir=tempfile.gettempdir())
        session['temp_dir'] = temp_dir # Store temp_dir in session

        session.pop('pmd_lookup_output_path', None)

        success, message, output_path = process_pmd_lookup(request.files, temp_dir)

        if not success:
            flash(message, 'error')
            return redirect(url_for('index'))
        
        return render_template('index.html',
                               pmd_lookup_download_link=url_for('download_file', filename=os.path.basename(output_path), process_type='pmd_lookup'))

    except Exception as e:
        flash(f'An unhandled error occurred during PMD Lookup: {e}', 'error')
        logging.error(f'Unhandled error in /process_pmd_lookup: {e}', exc_info=True)
        return redirect(url_for('index'))
    finally:
        # Keep temp_dir for download, cleanup on /download or /cleanup_session
        pass

@app.route('/download/<process_type>/<filename>', methods=['GET']) # Modified to include process_type
def download_file(process_type, filename):
    file_path_in_temp = None
    temp_dir = session.get('temp_dir')

    logging.info(f"DEBUG: Download requested for filename: {filename} for process: {process_type}")
    logging.info(f"DEBUG: Session temp_dir: {temp_dir}")
    
    if not temp_dir:
        logging.error("DEBUG: temp_dir not found in session.")
        flash('File not found for download or session expired. Please re-run the process.', 'error')
        return redirect(url_for('index'))

    if process_type == 'b_segment':
        session_path = session.get('b_segment_central_output_path')
    elif process_type == 'pmd_lookup':
        session_path = session.get('pmd_lookup_output_path')
    else:
        logging.error(f"DEBUG: Unknown process_type '{process_type}' for download.")
        flash('Invalid download request.', 'error')
        return redirect(url_for('index'))

    if session_path and os.path.basename(session_path) == filename:
        file_path_in_temp = os.path.join(temp_dir, filename)
        logging.info(f"DEBUG: Matched {process_type} file. Reconstructed path: {file_path_in_temp}")
    else:
        logging.error(f"DEBUG: Filename '{filename}' did not match any known session output files for {process_type}.")

    if file_path_in_temp and os.path.exists(file_path_in_temp):
        logging.info(f"DEBUG: File '{file_path_in_temp}' exists. Attempting to send.")
        try:
            response = send_file(
                file_path_in_temp,
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                as_attachment=True,
                download_name=filename
            )
            # After successful download, remove the file and clean up the temp directory if only one process
            # For simplicity now, cleanup happens via /cleanup_session or when temp_dir is reused.
            # A more robust solution might track which files belong to which process in the temp_dir.
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
    session.pop('b_segment_consolidated_output_path', None)
    session.pop('b_segment_central_output_path', None)
    session.pop('pmd_lookup_output_path', None)
    return redirect(url_for('index'))

if __name__ == '__main__':
    app.run(debug=True)
