import requests
import pyodbc
import json
import smtplib
from email.mime.text import MIMEText
import time
import csv
import sys
sys.path.append('C:/HV-PROJECTS')
import _AUTH
import _DEF 

# SQL Server connection settings
connection_string = f"DRIVER=ODBC Driver 17 for SQL Server;SERVER={_AUTH.server};DATABASE={_AUTH.database};UID={_AUTH.username};PWD={_AUTH.password}"
sql_table = "dbo.VS_voyages"

# API endpoint URL (same as before) -> aanvullen
api_url = _AUTH.end_veson
api_table = "Fixtures_WHS_HV"
api_full = api_url + "/" + api_table + _AUTH.vs_token

# Delete function
def delete_sql_table(connection):
    print("Deleting SQL table")
    cursor = connection.cursor()
    cursor.execute(f"DELETE FROM {sql_table}")
    connection.commit()

# Function to Fetch Data from API
def fetch_data_from_api(url):
    response = requests.get(url)
    response.raise_for_status()  # This will raise an exception for HTTP errors
    return response.text  # Returns the content of the response, in this case, a CSV file

# Function to Insert Data into SQL Server
def insert_data_into_sql(connection, data, table):
    cursor = connection.cursor()

    # Prepare the SQL insert statement
    column_names = data[0].keys()
    placeholders = ', '.join(['?'] * len(column_names))
    sql_insert = f"INSERT INTO {table} ({', '.join(column_names)}) VALUES ({placeholders})"

    # Insert each row
    for row in data:
        cursor.execute(sql_insert, tuple(row.values()))

    connection.commit()

if __name__ == "__main__":
    # Fetch data from API
    csv_data = fetch_data_from_api(api_full)

    # Convert CSV data to a list of dictionaries
    data_to_insert = []
    csv_reader = csv.DictReader(csv_data.splitlines())
    for row in csv_reader:
        data_to_insert.append(row)

    # Insert data into SQL
    try:
        with pyodbc.connect(connection_string) as conn:
            insert_data_into_sql(conn, data_to_insert, sql_table)
            print("Data inserted successfully")
    except Exception as e:
        print(f"An error occurred: {e}")