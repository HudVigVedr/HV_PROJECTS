# ==> This script will skip records that already exist <==


import requests
import pyodbc
import json
import smtplib
from email.mime.text import MIMEText
import time
from datetime import datetime


import sys
sys.path.append('C:/Python/HV_PROJECTS')
import _AUTH
import _DEF 

script_name = "Check wmsDL"
script_cat = "DWH_checks"

# SQL Server connection settings
connection_string = f"DRIVER=ODBC Driver 17 for SQL Server;SERVER={_AUTH.server};DATABASE={_AUTH.database};UID={_AUTH.username};PWD={_AUTH.password}"

sql_table = "dbo.relation_log"

# API endpoint URL (same as before) -> aanvullen
api_url = _AUTH.end_REST_MS_BC
api_table = "vendors"
api_full = api_url + "/" + api_table + "/$count"
#api_full = api_url + "/" + api_table + "?company="
api_vendor = api_url + "/vendors/$count?company="
api_customer = api_url + "/customers/$count?company="
api_filter = "&$filter=systemCreatedAt gt "
api_filter2 = "T00:00:00.000Z) and (systemCreatedAt lt "
api_filter3 = "T23:59:59.000Z)"
# Function to count data rows from API
def count_api_rows(data):
    return int(data)


def log_relations(connection, date, Entity, new_customers, ttl_customers, new_Vendors, ttl_vendors):
    """Log the count into the dbo.relations_log table"""
    cursor = connection.cursor()
    sql = "INSERT INTO dbo.relations_log (date, Entity, new_customers, ttl_customers, new_Vendors, ttl_vendors) VALUES (?, ?, ?, ?,? ,?)"
    cursor.execute(sql, date, Entity, new_customers, ttl_customers, new_Vendors, ttl_vendors)
    connection.commit()


if __name__ == "__main__":
    print(f"Inserting relation count into sql/staging..")
    connection = pyodbc.connect(connection_string)
    overall_status = "Success"
    total_mismatches = 0

    start_time = _DEF.datetime.now()
    company_names = _DEF.get_company_names(connection)

    date = datetime.now().date()

    for company_name in company_names:
        try:
            api_vendor = f"{api_vendor}{company_name}{api_filter}{date}"
            api_customer = f"{api_customer}{company_name}{api_filter}{date}"
            api_vendor_full = f"{api_vendor}{company_name}{api_filter}{date}{api_filter2}{date}{api_filter3}"
            api_customer_full = f"{api_customer}{company_name}{api_filter}{date}{api_filter2}{date}{api_filter3}"
            #api = f"{api_full}BMA"
            api_response_vendor_new = _DEF.make_api_request_count(api_vendor_full, _AUTH.client_id, _AUTH.client_secret, _AUTH.token_url)
            api_response_customer_new = _DEF.make_api_request_count(api_customer_full, _AUTH.client_id, _AUTH.client_secret, _AUTH.token_url)
            api_response_vendor = _DEF.make_api_request_count(api_vendor, _AUTH.client_id, _AUTH.client_secret, _AUTH.token_url)
            api_response_customer = _DEF.make_api_request_count(api_customer, _AUTH.client_id, _AUTH.client_secret, _AUTH.token_url)

            api_row_count_vendor_new = api_response_vendor_new
            api_row_count_customer_new = api_response_customer_new
            api_row_count_vendor = api_response_vendor
            api_row_count_customer = api_response_customer
            

            log_relations(connection, date, company_name, api_row_count_customer_new, api_row_count_customer, api_row_count_vendor, api_row_count_vendor_new)


        except Exception as e:
            overall_status = "Error"
            total_mismatches += 1
            error_details = f"An error occurred for {company_name}: {str(e)}"
            print(error_details)
            _DEF.log_status(connection, "Error", script_cat, script_name, start_time, _DEF.datetime.now(), int((_DEF.datetime.now() - start_time).total_seconds() / 60), 0, error_details, company_name, "N/A")

            _DEF.send_email_mfa(f"ErrorLog -> {script_name} / {script_cat}", error_details,  _AUTH.email_sender,  _AUTH.email_recipient, _AUTH.guid_blink, _AUTH.email_client_id, _AUTH.email_client_secret)    


    # Final logging
    if overall_status == "Success":
        success_message = "Relation count is inserted"
        _DEF.log_status(connection, "Success", script_cat, script_name, start_time, _DEF.datetime.now(), int((_DEF.datetime.now() - start_time).total_seconds() / 60), 0, success_message, "All", "N/A")
    else:
        error_summary = f"Total companies with mismatches or errors: {total_mismatches}."
        _DEF.log_status(connection, "Error", script_cat, script_name, start_time, _DEF.datetime.now(), int((_DEF.datetime.now() - start_time).total_seconds() / 60), 0, error_summary, "Multiple", "N/A")