import requests
import pyodbc
import json
import smtplib
from email.mime.text import MIMEText
import time
import pandas as pd
from sqlalchemy import create_engine

import sys
sys.path.append('C:/Python/HV_PROJECTS')
import _AUTH
import _DEF 

script_name = "CL master"
script_cat = "Errors_BC"


# API endpoint URL (same as before) -> aanvullen
api_url = _AUTH.end_Odata_BC
api_table = "clDateInt"
api_full = api_url + "/Company('"
api_full2 = "')/" + api_table + "/$count?$filter=(Central_Layer_Partner_ID ne '')"
api = api_full + api_full2
bc_page = "&page=11242117"

values_to_skip = ["Consol HV"]

connection_string = f"DRIVER=ODBC Driver 17 for SQL Server;SERVER={_AUTH.server};DATABASE={_AUTH.database};UID={_AUTH.username};PWD={_AUTH.password}"


# Function to count data rows from API
def count_api_rows(data):
    return int(data)

if __name__ == "__main__":
    print(f"Checking errors {script_name} in BC...")
    connection = pyodbc.connect(connection_string)
    overall_status = "Success"
    total_mismatches = 0

    start_time = _DEF.datetime.now()
    company_names = _DEF.get_company_names_skip(connection, values_to_skip)

    for company_name in company_names:
        try:
            api = f"{api_full}{company_name}{api_full2}"
            #api = f"{api_full}BMA"
            api_response = _DEF.make_api_request_count(api, _AUTH.client_id, _AUTH.client_secret, _AUTH.token_url)
            api_row_count = count_api_rows(api_response)


            if api_row_count == 0:
                overall_status = "Error"
                total_mismatches += 1
                error_details = f"Mismatch in row count for {company_name}: API rows {api_row_count}"
                _DEF.log_status(connection, "Error", script_cat, script_name, start_time, _DEF.datetime.now(), int((_DEF.datetime.now() - start_time).total_seconds() / 60), abs(api_row_count), error_details, company_name, "N/A")

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
        success_message = "All companies have a CL master."
        _DEF.log_status(connection, "Success", script_cat, script_name, start_time, _DEF.datetime.now(), int((_DEF.datetime.now() - start_time).total_seconds() / 60), 0, success_message, "All", "N/A")
    else:
        error_summary = f"Total companies with mismatches or errors: {total_mismatches}."
        _DEF.log_status(connection, "Error", script_cat, script_name, start_time, _DEF.datetime.now(), int((_DEF.datetime.now() - start_time).total_seconds() / 60), "N/A", error_summary, "Multiple", "N/A")