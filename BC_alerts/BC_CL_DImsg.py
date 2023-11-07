import requests
import pyodbc
import json
import smtplib
from email.mime.text import MIMEText
import time

import sys
sys.path.append('C:/HV_PROJECTS')
import _AUTH
import _DEF 


# API endpoint URL (same as before) -> aanvullen
api_url = _AUTH.end_REST_BOLTRICS_BC
api_table = "diMessages"
api_full = api_url + "/" + api_table + "?$filter=(status eq 'Failed') and contains(messageType, 'CDM')&company="

connection_string = f"DRIVER=ODBC Driver 17 for SQL Server;SERVER={_AUTH.server};DATABASE={_AUTH.database};UID={_AUTH.username};PWD={_AUTH.password}"

def count_rows(api_data_generator):
    """Count the number of rows in the API data generator"""
    return sum(1 for _ in api_data_generator)

   
if __name__ == "__main__":
    print("Script started")
    connection = pyodbc.connect(connection_string)
    threshold = 0  # Set the threshold
    companies_above_threshold = []  # List to hold companies above the threshold

    try:

        # Get a list of company names from SQL Server
        company_names = _DEF.get_company_names(connection)

        for company_name in company_names:
            print(f"Processing company: {company_name}")
            api = f"{api_full}{company_name}"
            access_token = _DEF.get_access_token(_AUTH.client_id, _AUTH.client_secret, _AUTH.token_url)

            if access_token:
                api_data_generator = _DEF.make_api_request(api, _AUTH.client_id, _AUTH.client_secret, _AUTH.token_url)
                row_count = count_rows(api_data_generator)

                if row_count > threshold:
                    companies_above_threshold.append((company_name, row_count))


    finally:
        email_body = f"Errors in DImsg -> Central layers:\n\n"
        for company, count in companies_above_threshold:
            email_body += f"{company} - {count} errors.\n"
        
        if companies_above_threshold:
            _DEF.send_email(
                'CL - erros DImsg',
                email_body,
                _AUTH.email_recipient,
                _AUTH.email_sender,
                _AUTH.smtp_server,
                _AUTH.smtp_port,
                _AUTH.email_username,
                _AUTH.email_password
            )
