import requests
import pyodbc
import json
import smtplib
from email.mime.text import MIMEText
from datetime import datetime, timedelta

import sys
sys.path.append('C:/Python/HV_PROJECTS')
import _AUTH
import _DEF 

script_name = "new CL messages older then 5 minutes"
script_cat = "Errors_BC"

def get_m20m_time():
    twenty_minutes_ago = datetime.now() - timedelta(hours= 2, minutes= 5)
    return twenty_minutes_ago.strftime('%H:%M:%S')

twenty_minutes_ago_time = get_m20m_time()

def get_current_date():
    return datetime.now().strftime('%Y-%m-%d')

current_date = get_current_date()

# API endpoint URL (same as before) -> aanvullen
api_url = _AUTH.end_REST_BOLTRICS_BC
api_table = "diMessages"
api_full = api_url + "/" + api_table + "?$filter=(status eq 'New') and contains(messageType, 'CDM') and (receivedAt lt " + current_date + "T" + twenty_minutes_ago_time + ".000Z)&company="
bc_page = "&page=11242113"


if __name__ == "__main__":
    print(f"Checking {script_name} in BC...")
    connection = pyodbc.connect(_AUTH.connection_string)
    threshold = 0

    start_time = _DEF.datetime.now()
    overall_status = "Success"

    try:
        company_names = _DEF.get_company_names(connection)

        for company_name in company_names:
            full_uri = _AUTH.BC_URi + company_name + bc_page
            api = f"{api_full}{company_name}"
            access_token = _DEF.get_access_token(_AUTH.client_id, _AUTH.client_secret, _AUTH.token_url)

            if access_token:
                api_data_generator = _DEF.make_api_request(api, _AUTH.client_id, _AUTH.client_secret, _AUTH.token_url)
                row_count = _DEF.count_rows(api_data_generator)

                if row_count > threshold:
                    overall_status = "Error"
                    error_details = f"{row_count} errors found."
                    _DEF.log_status(connection, "Error", script_cat, script_name, start_time, _DEF.datetime.now(), int((_DEF.datetime.now() - start_time).total_seconds() / 60), row_count, error_details, company_name, full_uri)
                    
                    _DEF.send_email_mfa(f"ErrorLog -> {script_name} / {script_cat} / {company_name}", 
                        f"{error_details} in {company_name}\nClick on {full_uri} to resolve the issue(s).",
                        _AUTH.email_sender,  
                        _AUTH.email_recipient,
                        _AUTH.guid_blink, 
                        _AUTH.email_client_id, 
                        _AUTH.email_client_secret
                    )

    except Exception as e:
        overall_status = "Error"
        error_details = str(e)
        print(f"An error occurred: {e}")
        # Log the exception as a generic error
        _DEF.log_status(connection, "Error", script_cat, script_name, start_time, _DEF.datetime.now(), int((_DEF.datetime.now() - start_time).total_seconds() / 60), 0, error_details, "None", full_uri)

        _DEF.send_email_mfa(f"ErrorLog -> {script_name} / {script_cat} / {company_name}", 
                        f"{error_details} in {company_name}\nClick on {full_uri} to resolve the issue(s).",
                        _AUTH.email_sender,  
                        _AUTH.email_recipient,
                        _AUTH.guid_blink, 
                        _AUTH.email_client_id, 
                        _AUTH.email_client_secret
                    )
    finally:
        if overall_status == "Success":
            # Log a success entry if no errors were found for any company
            _DEF.log_status(connection, "Success", script_cat, script_name, start_time, _DEF.datetime.now(), int((_DEF.datetime.now() - start_time).total_seconds() / 60), 0, "No errors", "None", full_uri)