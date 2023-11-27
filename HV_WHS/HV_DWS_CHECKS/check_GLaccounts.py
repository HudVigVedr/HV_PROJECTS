import requests
import pyodbc
import json
import smtplib
from email.mime.text import MIMEText
import time
import datetime

import sys
sys.path.append('C:/Python/HV_PROJECTS')
import _AUTH
import _DEF 

script_name = "Check GLaccounts"
script_cat = "DWH"

# SQL Server connection settings
sql_connection_string = f"DRIVER=ODBC Driver 17 for SQL Server;SERVER={_AUTH.server};DATABASE={_AUTH.database};UID={_AUTH.username};PWD={_AUTH.password}"

def check_values_in_tables(connection):
    try:
        # Perform the SQL query to check if values in [No] column of BC_GLaccounts exist in [Ledger] column of Mapping
        query = """
        SELECT DISTINCT BC.[no] 
        FROM dbo.BC_GLaccounts BC
        WHERE NOT EXISTS (
            SELECT 1
            FROM dbo.BC_GLaccounts_mapping M
            WHERE M.[Ledgernummer] = BC.[no]
        )
        """
        cursor = connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()

        if rows:
            # Values in BC_GLaccounts not found in Mapping
            missing_records = [row[0] for row in rows]
            error_message = f"Values in BC_GLaccounts not found in Mapping: {', '.join(str(record) for record in missing_records)}"
            return len(missing_records), error_message  # Return the count of missing records and the error message
        else:
            return 0, ""  # No missing records found
    except Exception as e:
        return -1, f"An error occurred while checking values in tables: {str(e)}"

if __name__ == "__main__":
    print("Checking unique GL accounts...")
    start_time = datetime.datetime.now()
    overall_status = "Success"
    connection = pyodbc.connect(sql_connection_string)
    total_missing_records = 0  # Initialize total missing records count to zero

    try:
        total_missing_records, error_message = check_values_in_tables(connection)  # Check values between tables and update total missing records
        if total_missing_records == -1:
            raise Exception(error_message)
        
        if total_missing_records > 0:
            overall_status = "Error"
            error_details = f"Total missing records: {total_missing_records}. {error_message}"  # Include total missing records in the error message
            print(error_details)
            _DEF.log_status(connection, "Error", script_cat, script_name, start_time, datetime.datetime.now(),
                            int((datetime.datetime.now() - start_time).total_seconds() / 60), total_missing_records, error_details, "None", "N/A")
        
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

        else:
            success_message = f"No missing records found. Duration: {duration} minutes."
            print(success_message)
            _DEF.log_status(connection, "Success", script_cat, script_name, start_time, datetime.datetime.now(),
                            int((datetime.datetime.now() - start_time).total_seconds() / 60), 0, success_message, "All", "N/A")
    except Exception as e:
        overall_status = "Error"
        error_details = f"An error occurred: {e}. Total missing records: {total_missing_records}"  # Include total missing records in the error message
        print(error_details)
        _DEF.log_status(connection, "Error", script_cat, script_name, start_time, datetime.datetime.now(),
                        int((datetime.datetime.now() - start_time).total_seconds() / 60), total_missing_records, error_details, "None", "N/A")
        
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

    finally:
        end_time = datetime.datetime.now()
        duration = int((end_time - start_time).total_seconds() / 60)

        connection.close()
