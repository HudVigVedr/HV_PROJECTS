import requests
import pyodbc
import json
import smtplib
from email.mime.text import MIMEText
import time
import csv
from io import StringIO

import sys
sys.path.append('C:/Python/HV_PROJECTS')
import _AUTH
import _DEF 

# SQL Server connection settings
connection_string = f"DRIVER=ODBC Driver 17 for SQL Server;SERVER={_AUTH.server};DATABASE={_AUTH.database};UID={_AUTH.username};PWD={_AUTH.password}"
sql_table = "dbo.VS_voyage_cargoes"

# API endpoint URL (same as before) -> aanvullen
api_url = _AUTH.end_veson
api_table = "whs_voyage_cargo"
api_full = api_url + "/" + api_table + _AUTH.vs_token

script_name = "VS_voyage_cargo"
script_cat = "DWH"

def delete_sql_table(connection):
    cursor = connection.cursor()
    cursor.execute(f"DELETE FROM {sql_table}")
    connection.commit()


def fetch_csv_and_write_to_sql(connection):
    total_inserted_rows = 0
    try:

        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()

        # Fetch data from API
        response = requests.get(api_full)
        response.raise_for_status()

        # Parse CSV data
        f = StringIO(response.text)
        reader = csv.reader(f, delimiter=',')

        # Skip header row if your CSV has one
        next(reader)

        # Insert data into SQL table
        for row in reader:
            cursor.execute('INSERT INTO ' + sql_table + ' VALUES ( ?, ?, ?, ?, ?, ?, ?, ? ,? ,?, ?, ?, ?, ?, ?, ?, ?, ? ,? ,?, ?, ?, ?, ?, ?, ?, ?, ? ,? ,?, ?, ?, ?, ?, ?, ?, ?, ? ,? , ?, ?, ?, ?, ?, ?, ?, ?, ? ,? ,?, ?, ?, ?, ?, ?, ?, ?, ? ,? ,?, ?, ?, ?, ?, ?, ?, ?, ? ,? ,?, ?, ?, ?, ?, ?, ?, ?, ? ,? ,?, ?, ?, ?, ?, ?, ?, ?, ? ,? ,?, ?, ?, ?, ?, ?, ?, ?, ? ,? ,?, ?, ?, ?, ?, ?, ?, ?, ? ,? ,?, ?, ?, ?, ?, ?, ?, ?, ? ,? ,?, ?, ?, ?, ?, ?, ?, ?, ? ,? ,?, ?, ?, ?, ?, ?, ?, ?, ? ,? ,?, ?, ?, ?, ?, ?, ?, ?, ? ,? ,?, ?, ?, ?, ?, ?, ?, ?, ? ,? ,?, ?, ?, ?, ?, ?, ?, ?, ? ,? ,?, ?, ?, ?, ?, ?, ?, ?, ? ,? ,?, ?, ?, ?, ?, ?, ?, ?, ? ,? ,?, ?, ?, ?, ?, ?, ?, ?, ? ,? ,?, ?, ?, ?, ?, ?, ?, ?, ? ,? ,?, ?, ?, ?, ?, ?, ?, ?, ? ,? ,?, ?, ?, ?, ?, ?, ?, ?, ? ,? ,?, ?, ?, ?, ?, ?, ?, ?, ? ,? ,?, ?, ?, ?, ?, ?, ?, ?, ? ,? ,?, ?, ?, ?, ?, ?, ?, ?, ? ,? ,?, ?, ?, ?, ?, ?, ?, ?, ? ,? ,?, ?, ?, ?, ?, ?, ?, ?, ? ,? ,?, ?, ?, ?, ?, ?, ?, ?, ? ,? ,?, ?, ?, ?, ?, ?, ?, ?, ? ,? ,?)', row)
            total_inserted_rows += 1 
        # Commit changes and close connection
        conn.commit()


    except Exception as e:
         print(f"Error: {e}")
    finally:
        if conn is not None:
            conn.close()

    return total_inserted_rows

 

if __name__ == "__main__":
    print(f"Copying {script_name} to SQL/Staging...")
    connection = None
    try:
        connection = pyodbc.connect(connection_string)
        start_time = _DEF.datetime.now()
        overall_status = "Success"
        total_inserted_rows = 0

        delete_sql_table(connection)
        total_inserted_rows = fetch_csv_and_write_to_sql(connection)

        success_message = f"Total rows inserted: {total_inserted_rows}."
        _DEF.log_status(connection, "Success", script_cat, script_name, start_time, _DEF.datetime.now(), int((_DEF.datetime.now() - start_time).total_seconds() / 60), total_inserted_rows, success_message, "All", "N/A")

    except Exception as e:
        overall_status = "Error"
        error_details = str(e)
        print(f"An error occurred: {e}")
        _DEF.log_status(connection, "Error", script_cat, script_name, start_time, _DEF.datetime.now(), int((_DEF.datetime.now() - start_time).total_seconds() / 60), 0, error_details, "All", "N/A")

        _DEF.send_email_mfa(f"ErrorLog -> {script_name} / {script_cat}", error_details,  _AUTH.email_sender,  _AUTH.email_recipient, _AUTH.guid_blink, _AUTH.email_client_id, _AUTH.email_client_secret)   

    finally:
        if connection is not None:
            connection.close()
