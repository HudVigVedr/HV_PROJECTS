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

script_name = "BC_wmsRecordLink"
script_cat = "DWH_extract"
script_type = "Copying" 

# SQL Server connection settings
connection_string = f"DRIVER=ODBC Driver 17 for SQL Server;SERVER={_AUTH.server};DATABASE={_AUTH.database};UID={_AUTH.username};PWD={_AUTH.password}"
sql_table = "BC_wmsRecordLink"

# API endpoint URL (same as before) -> aanvullen
api_url = _AUTH.end_REST_BOLTRICS_BC
api_table = "wmsRecordLinks"
api_full = api_url + "/" + api_table + "?" + "$filter=(tableNo eq 122) or (tableNo eq 0) and (created gt "+ _DEF.yesterday_date +"T00:00:00Z)&company="
#api_full = api_url + "/" + api_table + "?" + "$filter=(tableNo eq 0)&company="
#api_full = api_url + "/" + api_table + "?" + "$filter=(tableNo eq 122)&company="

def record_exists(connection, no, entity):
    cursor = connection.cursor()
    query = f"SELECT COUNT(1) FROM {sql_table} WHERE [id] = ? AND [Entity] = ?"
    cursor.execute(query, (no, entity))
    result = cursor.fetchone()[0] > 0
    return result


def insert_or_update_data_into_sql(connection, data, sql_table, company_name):
    for item in data:
        # Prepare the data for MERGE
        values = [item[key] for key in item] + [company_name]
        columns = list(item.keys()) + ["Entity"]
        
        # Build the SQL MERGE statement
        merge_sql = f"""
        MERGE {sql_table} AS Target
        USING (SELECT ? AS [{columns[0]}]""" + "".join([f", ? AS [{col}]" for col in columns[1:]]) + """) AS Source
        ON Target.[id] = Source.[id] AND Target.[Entity] = Source.[Entity]
        WHEN MATCHED THEN
            UPDATE SET
            """ + ", ".join([f"Target.[{col}] = Source.[{col}]" for col in columns[1:]]) + """
        WHEN NOT MATCHED THEN
            INSERT (""" + ", ".join([f"[{col}]" for col in columns]) + """)
            VALUES (""" + ", ".join([f"Source.[{col}]" for col in columns]) + """);
        """
        
        # Execute the MERGE statement
        cursor = connection.cursor()
        cursor.execute(merge_sql, tuple(values))
        connection.commit()


if __name__ == "__main__":
    print("Incremental refresh BC_wmsRecordLink to SQL/Staging...")
    connection = pyodbc.connect(connection_string)
    threshold = 0

    start_time = _DEF.datetime.now()
    overall_status = "Success"
    total_inserted_rows = 0

    try:
        company_names = _DEF.get_company_names2(connection)

        for company_name in company_names:
            api = f"{api_full}{company_name}"
            api_data_generator = _DEF.make_api_request(api, _AUTH.client_id, _AUTH.client_secret, _AUTH.token_url)
            print(api)
            data_to_insert = list(api_data_generator)
            row_count = len(data_to_insert)

            if row_count > threshold:
                insert_or_update_data_into_sql(connection, data_to_insert, sql_table, company_name)        
                inserted_rows = _DEF.count_rows(data_to_insert)
                total_inserted_rows += inserted_rows

                if inserted_rows != row_count:
                    overall_status = "Error"
                    error_details = f"Expected to insert {row_count} rows, but only {inserted_rows} were inserted."
                    _DEF.log_status(connection, "Error", script_cat, script_name, start_time, _DEF.datetime.now(), int((_DEF.datetime.now() - start_time).total_seconds() / 60), row_count - inserted_rows, error_details, company_name, api)

                    _DEF.send_email_mfa(f"ErrorLog -> {script_name} / {script_cat}", error_details,  _AUTH.email_sender,  _AUTH.email_recipient, _AUTH.guid_blink, _AUTH.email_client_id, _AUTH.email_client_secret)  

    except Exception as e:
        overall_status = "Error"
        error_details = str(e)
        print(f"An error occurred: {e}")
        _DEF.log_status(connection, "Error", script_cat, script_name, start_time, _DEF.datetime.now(), int((_DEF.datetime.now() - start_time).total_seconds() / 60), 0, error_details, "None", "N/A")

        _DEF.send_email_mfa(f"ErrorLog -> {script_name} / {script_cat}", error_details,  _AUTH.email_sender,  _AUTH.email_recipient, _AUTH.guid_blink, _AUTH.email_client_id, _AUTH.email_client_secret)  

    finally:
        if overall_status == "Success":
            success_message = f"Total rows inserted/updated: {total_inserted_rows}."
            _DEF.log_status(connection, "Success", script_cat, script_name, start_time, _DEF.datetime.now(), int((_DEF.datetime.now() - start_time).total_seconds() / 60), total_inserted_rows, success_message, "All", "N/A")

        elif overall_status == "Error":
            # Additional logging for error scenario can be added here if needed
            pass