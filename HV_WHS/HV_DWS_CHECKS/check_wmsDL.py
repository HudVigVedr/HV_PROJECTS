# ==> This script will skip records that already exist <==


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

script_name = "Check wmsDL"
script_cat = "DWH"

# SQL Server connection settings
connection_string = f"DRIVER=ODBC Driver 17 for SQL Server;SERVER={_AUTH.server};DATABASE={_AUTH.database};UID={_AUTH.username};PWD={_AUTH.password}"

sql_table = "dbo.BC_wmsDH"

# API endpoint URL (same as before) -> aanvullen
api_url = _AUTH.end_REST_BOLTRICS_BC
api_table = "wmsDocumentHeaders"
api_full = api_url + "/" + api_table + "?company="
#api_full = api_url + "/" + api_table + "?company="

# Function to count data rows from API
def count_api_rows(data):
    return len(data)

# Function to count rows in SQL table for a specific company
def count_sql_rows(connection, sql_table, company_name):
    cursor = connection.cursor()
    sql_query = f"SELECT COUNT(*) FROM {sql_table} WHERE [Entity] = ?"
    cursor.execute(sql_query, (company_name,))
    result = cursor.fetchone()
    return result[0] if result else 0

if __name__ == "__main__":
    print(f"Comparing {api_table} row counts...")
    connection = pyodbc.connect(connection_string)
    overall_status = "Success"
    total_mismatches = 0

    start_time = _DEF.datetime.now()
    company_names = _DEF.get_company_names(connection)

    for company_name in company_names:
        try:
            api = f"{api_full}{company_name}"
            api_data_generator = _DEF.make_api_request(api, _AUTH.client_id, _AUTH.client_secret, _AUTH.token_url)

            api_data = list(api_data_generator)
            api_row_count = count_api_rows(api_data)
            sql_row_count = count_sql_rows(connection, sql_table, company_name)

            if api_row_count != sql_row_count:
                overall_status = "Error"
                total_mismatches += 1
                error_details = f"Mismatch in row count for {company_name}: API rows {api_row_count}, SQL rows {sql_row_count}"
                _DEF.log_status(connection, "Error", script_cat, script_name, start_time, _DEF.datetime.now(), int((_DEF.datetime.now() - start_time).total_seconds() / 60), abs(api_row_count - sql_row_count), error_details, company_name, "N/A")

                _DEF.send_email(
                f"ErrorLog -> {script_name} / {script_cat}",
                error_details,
                _AUTH.email_recipient,
                _AUTH.email_sender,
                _AUTH.smtp_server,
                _AUTH.smtp_port,
                _AUTH.email_username,
                _AUTH.email_password
            )        


        except Exception as e:
            overall_status = "Error"
            total_mismatches += 1
            error_details = f"An error occurred for {company_name}: {str(e)}"
            print(error_details)
            _DEF.log_status(connection, "Error", script_cat, script_name, start_time, _DEF.datetime.now(), int((_DEF.datetime.now() - start_time).total_seconds() / 60), 0, error_details, company_name, "N/A")

            _DEF.send_email(
            f"ErrorLog -> {script_name} / {script_cat}",
            error_details,
            _AUTH.email_recipient,
            _AUTH.email_sender,
            _AUTH.smtp_server,
            _AUTH.smtp_port,
            _AUTH.email_username,
            _AUTH.email_password
        )        


    # Final logging
    if overall_status == "Success":
        success_message = "All companies have matching row counts between API and SQL."
        _DEF.log_status(connection, "Success", script_cat, script_name, start_time, _DEF.datetime.now(), int((_DEF.datetime.now() - start_time).total_seconds() / 60), 0, success_message, "All", "N/A")
    else:
        error_summary = f"Total companies with mismatches or errors: {total_mismatches}."
        _DEF.log_status(connection, "Error", script_cat, script_name, start_time, _DEF.datetime.now(), int((_DEF.datetime.now() - start_time).total_seconds() / 60), 0, error_summary, "Multiple", "N/A")