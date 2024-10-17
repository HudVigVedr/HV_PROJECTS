import requests
import pyodbc
import json
import smtplib
from email.mime.text import MIMEText
import time
import sys
sys.path.append('C:/Python/HV_PROJECTS')
import _AUTH
import _DEF

# Script metadata
script_name = "BC_Check_Posting_Group"
script_cat = "BC"

# Database connection details
connection_string = f"DRIVER=ODBC Driver 17 for SQL Server;SERVER={_AUTH.server};DATABASE={_AUTH.database};UID={_AUTH.username};PWD={_AUTH.password}"

# API URLs for countries, vendors, and customers
api_url = _AUTH.end_REST_MS_BC
api_url2 = _AUTH.end_REST_BOLTRICS_BC
api_table_countries = "countriesRegions"
api_table_customers = "customers"
api_table_vendors = "vendors"

# Full API URLs with placeholders for company names
api_full_countries = f"{api_url}/{api_table_countries}?company="
api_full_customers = f"{api_url2}/{api_table_customers}?no,name,countryRegionCode,genBusPostingGroup,customerPostingGroup,vatBusPostingGroup&company="
api_full_vendors = f"{api_url2}/{api_table_vendors}?no,name,countryRegionCode,genBusPostingGroup,vendorPostingGroup,vatBusPostingGroup&company="

# Hardcoded list of EU country codes
EU_COUNTRY_CODES = [
    "AT", "BE", "BG", "HR", "CY", "CZ", "DK", "EE", "FI", "FR", "DE", "GR", "HU", "IE", "IT", "LV", 
    "LT", "LU", "MT", "NL", "PL", "PT", "RO", "SK", "SI", "ES", "SE"
]

def get_correct_posting_group(country_code):
    """Determines the correct posting group based on the country code."""
    if country_code == "NL":
        return "BINNENLAND"
    elif country_code in EU_COUNTRY_CODES:
        return "EU"
    else:
        return "EXPORT"

def check_posting_groups(entity_data, country_data, is_customer=True):
    """Checks posting groups from customers or vendors and identifies mismatches."""
    mismatches = []
    
    for entity in entity_data:
        country_code = entity['countryRegionCode']
        correct_posting_group = country_data.get(country_code, "Unknown")
        
        # Check three posting groups depending on whether it's customer or vendor data
        if is_customer:
            posting_groups = {
                'customerPostingGroup': entity['customerPostingGroup'],
                'genBusPostingGroup': entity['genBusPostingGroup'],
                'vatBusPostingGroup': entity['vatBusPostingGroup']
            }
        else:
            posting_groups = {
                'vendorPostingGroup': entity['vendorPostingGroup'],
                'genBusPostingGroup': entity['genBusPostingGroup'],
                'vatBusPostingGroup': entity['vatBusPostingGroup']
            }

        for posting_group_key, posting_group_value in posting_groups.items():
            if posting_group_value != correct_posting_group:
                mismatches.append({
                    'Src': 'Customer' if is_customer else 'Vendor',
                    'Entity': entity['no'],
                    'Name': entity['name'],
                    'Country': entity['countryRegionCode'],
                    posting_group_key: posting_group_value,
                    f'Correct{posting_group_key.capitalize()}': correct_posting_group
                })

    return mismatches

if __name__ == "__main__":
    print("Checking Countries vs PostingGroups...")
    connection = pyodbc.connect(connection_string)
    start_time = _DEF.datetime.now()
    overall_status = "Success"
    total_inserted_rows = 0

    try:
        # Fetch company names from the database
        company_names = _DEF.get_company_names(connection)

        for company_name in company_names:
            # Fetch country data
            api_countries = f"{api_full_countries}{company_name}"
            country_data_generator = _DEF.make_api_request(api_countries, _AUTH.client_id, _AUTH.client_secret, _AUTH.token_url)

            country_data = {}
            for country in country_data_generator:
                if country and 'code' in country and 'displayName' in country:
                    # Process valid data using 'code' and 'displayName'
                    posting_group = get_correct_posting_group(country['code'])
                    country_data[country['code']] = posting_group
                else:
                    # Log the problematic record for debugging
                    error_details = f"Missing 'code' or 'displayName' in API response for company {company_name}: {country}"
                    _DEF.log_status(connection, "Error", script_cat, script_name, start_time, _DEF.datetime.now(), 0, error_details, company_name, api_countries)
                    print(f"Warning: {error_details}")

            # Fetch customers data and check posting groups
            api_customers = f"{api_full_customers}{company_name}"
            customer_data_generator = _DEF.make_api_request(api_customers, _AUTH.client_id, _AUTH.client_secret, _AUTH.token_url)
            customer_data = list(customer_data_generator)
            customer_mismatches = check_posting_groups(customer_data, country_data, is_customer=True)

            # Fetch vendors data and check posting groups
            api_vendors = f"{api_full_vendors}{company_name}"
            vendor_data_generator = _DEF.make_api_request(api_vendors, _AUTH.client_id, _AUTH.client_secret, _AUTH.token_url)
            vendor_data = list(vendor_data_generator)
            vendor_mismatches = check_posting_groups(vendor_data, country_data, is_customer=False)

            # Combine mismatches
            all_mismatches = customer_mismatches + vendor_mismatches

            if all_mismatches:
                # If mismatches, create an Excel report and send via email
                report_file = _DEF.create_excel_report(all_mismatches, f"mismatch_report_{company_name}.xlsx")
                _DEF.send_email_mfa_attachment(f"Posting Group Mismatch -> {script_name} / {script_cat}", f"Mismatches found for {company_name}. See attached report.", _AUTH.email_sender, _AUTH.email_recipient, _AUTH.guid_blink, _AUTH.email_client_id, _AUTH.email_client_secret, report_file, )

            # Insert data into SQL if needed and log status
            inserted_rows = _DEF.count_rows(customer_data + vendor_data)  # Assuming all rows inserted
            total_inserted_rows += inserted_rows
            _DEF.log_status(connection, "Success", script_cat, script_name, start_time, _DEF.datetime.now(), int((_DEF.datetime.now() - start_time).total_seconds() / 60), inserted_rows, "Data processed successfully", company_name, api_countries)

    except Exception as e:
        overall_status = "Error"
        error_details = str(e)
        print(f"An error occurred: {e}")
        _DEF.log_status(connection, "Error", script_cat, script_name, start_time, _DEF.datetime.now(), int((_DEF.datetime.now() - start_time).total_seconds() / 60), 0, error_details, "None", "N/A")

        _DEF.send_email_mfa(f"ErrorLog -> {script_name} / {script_cat}", error_details, _AUTH.email_sender, _AUTH.email_recipient, _AUTH.guid_blink, _AUTH.email_client_id, _AUTH.email_client_secret)

    finally:
        if overall_status == "Success":
            success_message = f"Total rows processed: {total_inserted_rows}."
            _DEF.log_status(connection, "Success", script_cat, script_name, start_time, _DEF.datetime.now(), int((_DEF.datetime.now() - start_time).total_seconds() / 60), total_inserted_rows, success_message, "All", "N/A")