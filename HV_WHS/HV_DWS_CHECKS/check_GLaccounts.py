import pyodbc
import pandas as pd
from email.mime.text import MIMEText
import time
import datetime
import os

import sys
sys.path.append('C:/Python/HV_PROJECTS')
import _AUTH
import _DEF 

script_name = ""
script_cat = ""

sql_connection_string = f"DRIVER=ODBC Driver 17 for SQL Server;SERVER={_AUTH.server};DATABASE={_AUTH.database};UID={_AUTH.username};PWD={_AUTH.password}"

def check_values_in_tables(BC_GLaccounts, server_file_path):
    try:
        if not os.path.exists(server_file_path):
            raise Exception(f"File not found at path: {server_file_path}")

        # Read the Excel file into a DataFrame using pandas
        excel_data = pd.read_excel(server_file_path, dtype={'no': str})  # Ensure 'no' is treated as a string

        # Get unique values from the 'no' column in the SQL table as strings
        unique_values_sql = BC_GLaccounts['no'].astype(str).unique()

        # Find missing values by comparing with the Excel data
        missing_values = [value for value in unique_values_sql if value not in excel_data['no'].tolist()]

        if missing_values:
            total_missing_records = len(missing_values)
            error_message = f"Values missing in Excel: {', '.join(missing_values)}"
            return total_missing_records, error_message
        else:
            return 0, ""
    except Exception as e:
        return -1, f"An error occurred while checking values in Excel: {str(e)}"

if __name__ == "__main__":
    print("Checking unique GL accounts between SQL table and Excel on server path...")
    server_file_path = r"C:\Python\HV_PROJECTS\Grootboekschema.xlsx"

    try:
        connection = pyodbc.connect(sql_connection_string)
        
        # Load unique values from the SQL Server table's 'no' column as strings
        query = """SELECT DISTINCT CAST(no AS NVARCHAR(255)) AS no
                FROM BC_GLaccounts
                WHERE [Entity] NOT IN ('Consol HV ', 'Hartel', 'Stella', 'Geervliet', 'Heenvliet', 'MS Geervliet', 'MS Rhoon', 'Noordvliet', 'Zuidvliet', 'Haringvliet', 'Hoogvliet')"""
        BC_GLaccounts = pd.read_sql(query, connection)

        total_missing_records, error_message = check_values_in_tables(BC_GLaccounts, server_file_path)
        
        if total_missing_records == -1:
            raise Exception(error_message)

        if total_missing_records > 0:
            print(f"Total missing records: {total_missing_records}. {error_message}")
        else:
            print("No missing records found.")
    
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if 'connection' in locals():
            connection.close()