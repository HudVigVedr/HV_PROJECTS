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

script_name = "Errors in JobQuee"
script_cat = "Errors_BC"


# API endpoint URL (same as before) -> aanvullen
api_url = _AUTH.end_Odata_BC
api_table = "JQ_erorrs"
api_full = api_url + "/Company('"
api_full2 = "')/" + api_table + "?$select=Object_ID_to_Run,Object_Caption_to_Run,Status&$filter=Status eq 'Error'"
bc_page = "&page=672"

connection_string = f"DRIVER=ODBC Driver 17 for SQL Server;SERVER={_AUTH.server};DATABASE={_AUTH.database};UID={_AUTH.username};PWD={_AUTH.password}"

   
if __name__ == "__main__":
    print("Checking errors jobques in BC...")
    connection = pyodbc.connect(connection_string)
    threshold = 0

    start_time = _DEF.datetime.now()
    overall_status = "Success"

    try:
        company_names = _DEF.get_company_names(connection)

        for company_name in company_names:

            full_uri = _AUTH.BC_URi + company_name + bc_page

            api = f"{api_full}{company_name}{api_full2}"
            access_token = _DEF.get_access_token(_AUTH.client_id, _AUTH.client_secret, _AUTH.token_url)

            if access_token:
                api_data_generator = _DEF.make_api_request(api, _AUTH.client_id, _AUTH.client_secret, _AUTH.token_url)
                row_count = _DEF.count_rows(api_data_generator)

                if row_count > threshold:
                    overall_status = "Error"
                    error_details = f"{row_count} errors found."
                    _DEF.log_status(connection, "Error", script_cat, script_name, start_time, _DEF.datetime.now(), int((_DEF.datetime.now() - start_time).total_seconds() / 60), row_count, error_details, company_name, full_uri)

    except Exception as e:
        overall_status = "Error"
        error_details = str(e)
        print(f"An error occurred: {e}")
        # Log the exception as a generic error
        _DEF.log_status(connection, "Error", script_cat, script_name, start_time, _DEF.datetime.now(), int((_DEF.datetime.now() - start_time).total_seconds() / 60), 0, error_details, "None", full_uri)

    finally:
        if overall_status == "Success":
            # Log a success entry if no errors were found for any company
            _DEF.log_status(connection, "Success", script_cat, script_name, start_time, _DEF.datetime.now(), int((_DEF.datetime.now() - start_time).total_seconds() / 60), 0, "No errors", "None", full_uri)