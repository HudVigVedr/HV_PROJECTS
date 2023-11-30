from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
import pandas as pd
import pyodbc
import io
import datetime

# Importing custom authentication module
import sys
sys.path.append('C:/Python/HV_PROJECTS')
import _AUTH
import _DEF

script_name = "Unique GLaccounts"
script_cat = "DWH"


# Local Excel file path
#file_path = r"C:\Users\ThomLems\Hudig & Veder\Rapportage - Documents\HV_WHS\Bronbestanden\Grootboekschema.xlsx"  # replace with your actual file name
file_path = r"C:\Users\beheerder\Hudig & Veder(1)\Rapportage - Bronbestanden\Grootboekschema.xlsx"  

# SQL database details
sql_connection_string = f"DRIVER=ODBC Driver 17 for SQL Server;SERVER={_AUTH.server};DATABASE={_AUTH.database};UID={_AUTH.username};PWD={_AUTH.password}"
table_name = 'BC_GLaccounts_mapping'

def delete_sql_table(connection):
    print("Deleting SQL table")
    cursor = connection.cursor()
    cursor.execute(f"DELETE FROM {table_name}")
    connection.commit()

def read_excel(file_path):
    try:
        df = pd.read_excel(file_path, engine='openpyxl')
        return df
    except Exception as e:
        print(f"An error occurred while reading the Excel file: {e}")
        return None

def import_to_sql(df, sql_connection_string, table_name):
    conn = pyodbc.connect(sql_connection_string)
    cursor = conn.cursor()
    total_inserted_rows = 0

    try:
        for index, row in df.iterrows():
            cursor.execute(f"""
                INSERT INTO {table_name} ([Ledgernummer]) 
                values(?)""", 
                row['Ledgernummer']
            )
            total_inserted_rows += 1

        conn.commit()
        return total_inserted_rows
    except Exception as e:
        print(f"An error occurred while inserting into SQL: {e}")
        return total_inserted_rows
    finally:
        cursor.close()
        conn.close()

# Main execution
if __name__ == "__main__":
    print("Copying unique GL accounts to SQL/Staging...")
    start_time = datetime.datetime.now()
    overall_status = "Success"
    total_inserted_rows = 0
    connection = pyodbc.connect(sql_connection_string)

    delete_sql_table(connection)

    try:
        df = read_excel(file_path)
        if df is not None:
            total_inserted_rows = import_to_sql(df, sql_connection_string, table_name)
            if total_inserted_rows != len(df):
                overall_status = "Error"
                error_details = f"Expected to insert {len(df)} rows, but only {total_inserted_rows} were inserted."
                _DEF.log_status(connection, "Error", script_cat, script_name, start_time, datetime.datetime.now(), int((datetime.datetime.now() - start_time).total_seconds() / 60), 0, error_details, "N/A", "N/A")
    
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
        error_details = str(e)
        print(f"An error occurred: {e}")
        _DEF.log_status(connection, "Error", script_cat, script_name, start_time, datetime.datetime.now(), int((datetime.datetime.now() - start_time).total_seconds() / 60), 0, error_details, "None", "N/A")

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

        if overall_status == "Success":
            success_message = f"Total rows inserted: {total_inserted_rows}. Duration: {duration} minutes."
            print(success_message)
            _DEF.log_status(connection, "Success", script_cat, script_name, start_time, end_time, duration, total_inserted_rows, success_message, "All", "N/A")
        else:
            error_summary = f"Script execution failed. Duration: {duration} minutes."
            print(error_summary)
            _DEF.log_status(connection, "Error", script_cat, script_name, start_time, end_time, duration, total_inserted_rows, error_summary, "All", "N/A")

        connection.close()