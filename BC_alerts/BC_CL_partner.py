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

script_name = "CL partners"
script_cat = "Errors_BC"


# API endpoint URL (same as before) -> aanvullen
api_url = _AUTH.end_Odata_BC
api_table = "clDateInt"
api_full = api_url + "/Company('"
api_full2 = "')/" + api_table + "/$count?$filter=(Central_Layer_Partner_ID eq '_CENTRAL')"
bc_page = "&page=11242117"

connection_string = f"DRIVER=ODBC Driver 17 for SQL Server;SERVER={_AUTH.server};DATABASE={_AUTH.database};UID={_AUTH.username};PWD={_AUTH.password}"

   
if __name__ == "__main__":
    print(f"Checking {script_name} in BC...")
    connection = pyodbc.connect(connection_string)
    threshold = 0
    total_mismatches = 0

    start_time = _DEF.datetime.now()
    overall_status = "Success"

    try:
        company_names = _DEF.get_company_names(connection)

        companies_to_skip = ["Consol HV", "Geervliet", "Heenvliet", "MS Geervliet", "MS Rhoon", "Noordvliet", "Zuidvliet", "Haringvliet", "Hoogvliet"]  # Add the names of companies you want to skip

        for company_name in company_names:
            # Check if the company_name is in the list of companies to skip
            if company_name in companies_to_skip:
                print(f"Skipping {company_name}...")
                continue  

            full_uri = _AUTH.BC_URi + company_name + bc_page

            api = f"{api_full}{company_name}{api_full2}"
            api_response = _DEF.make_api_request_count(api, _AUTH.client_id, _AUTH.client_secret, _AUTH.token_url)

            api_row_count = _DEF.count_api_rows(api_response)
        

            if api_row_count != 1:
                overall_status = "Error"
                total_mismatches += 1
                error_details = f"Mismatch in row count for {company_name}: API rows {api_row_count}"
                _DEF.log_status(connection, "Error", script_cat, script_name, start_time, _DEF.datetime.now(), int((_DEF.datetime.now() - start_time).total_seconds() / 60), api_row_count, error_details, company_name, "N/A")

                _DEF.send_email_mfa(f"ErrorLog -> {script_name} / {script_cat}", error_details,  _AUTH.email_sender,  _AUTH.email_recipient, _AUTH.guid_blink, _AUTH.email_client_id, _AUTH.email_client_secret)

    except Exception as e:
            overall_status = "Error"
            total_mismatches += 1
            error_details = f"An error occurred for {company_name}: {str(e)}"
            print(error_details)
            _DEF.log_status(connection, "Error", script_cat, script_name, start_time, _DEF.datetime.now(), int((_DEF.datetime.now() - start_time).total_seconds() / 60), 0, error_details, company_name, "N/A")
                
            _DEF.send_email_mfa(f"ErrorLog -> {script_name} / {script_cat}", error_details,  _AUTH.email_sender,  _AUTH.email_recipient, _AUTH.guid_blink, _AUTH.email_client_id, _AUTH.email_client_secret)

    # Final logging
    if overall_status == "Success":
        success_message = "All companies have the correct CL master"
        _DEF.log_status(connection, "Success", script_cat, script_name, start_time, _DEF.datetime.now(), int((_DEF.datetime.now() - start_time).total_seconds() / 60), 0, success_message, "All", "N/A")
    else:
        error_summary = f"Total companies with mismatches or errors: {total_mismatches}."
        _DEF.log_status(connection, "Error", script_cat, script_name, start_time, _DEF.datetime.now(), int((_DEF.datetime.now() - start_time).total_seconds() / 60), 0, error_summary, "Multiple", "N/A")