import requests
import pyodbc
import json
import _AUTH
import _DEF as _DEF
import smtplib
from email.mime.text import MIMEText
import time

# SQL Server connection settings
connection_string = f"DRIVER=ODBC Driver 17 for SQL Server;SERVER={_AUTH.server};DATABASE={_AUTH.database};UID={_AUTH.username};PWD={_AUTH.password}"
#connection_string2 = f"DRIVER=ODBC Driver 17 for SQL Server;SERVER=HV-db;DATABASE=Staging;UID=hheij;PWD=ByMus&060R6f"
sql_table = "dbo.VSVoyages"

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

# Function to insert data into SQL Server
def insert_data_into_sql(connection, data, sql_table):
    cursor = connection.cursor()

    # Extract column names from the first row of the data
    column_names = data[0].keys()

    # SQL insert statement
    sql_insert = f"""
        INSERT INTO {sql_table} ({", ".join(column_names)})
        VALUES ({", ".join(["?" for _ in column_names])})
    """

    # Insert each row of data into the SQL table
    for row in data:
        values = list(row.values())
        print(values)  # Print the values being inserted
        cursor.execute(sql_insert, tuple(values))

    connection.commit()


   
if __name__ == "__main__":
    print("Script started")
    start_time = time.time()  # Record start time
    rows_inserted = 0  # Initialize counter for rows inserted
    successes = []  # List to hold successful company names
    failures = []  # List to hold failed company names
    rows_inserted_per_iteration = {}  # Dictionary to hold rows inserted per iteration

    try:
        # Establish the SQL Server connection
        connection = pyodbc.connect(connection_string)
        print("Establishing SQL Server connection")
        delete_sql_table(connection)

        api = f"{api_full}"  # No need to append company_name
        api_data_generator = _DEF.make_api_request_vs(api)  # Update function call according to the new signature

        try:
            if api_data_generator:
                iteration_rows_inserted = 0  # Initialize counter for rows inserted in this iteration
                for api_data in api_data_generator:
                    #print(f"Processing data")
                    #print(f"Type of api_data: {type(api_data)}")  # Debugging print statement
                    insert_data_into_sql(connection, [api_data], sql_table)
                    rows_inserted += 1 
                    iteration_rows_inserted += 1  # Increment rows_inserted

        except Exception as e:
            failures.append(str(e))

    finally:
        print("Closing SQL Server connection")
        connection.close()
        end_time = time.time()  # Record end time
        duration = (end_time - start_time) / 60  # Calculate duration
        duration_minutes_rounded = round(duration, 2)

        # Print results
        print(f"Total rows inserted: {rows_inserted}")
        print(f"Total time taken: {duration} minutes")

        # Prepare email content
        email_body = f"The script completed in {duration_minutes_rounded} minutes with {rows_inserted} total rows inserted.\n\n"
        if successes:
            email_body += "Successes:\n" + "\n".join(successes) + "\n\n"
        if failures:
            email_body += "Failures:\n" + "\n".join(failures) + "\n\n"

        # Include rows inserted per iteration in email
        for company, rows in rows_inserted_per_iteration.items():
            email_body += f"Rows inserted for {company}: {rows}\n"

        # Send email
        _DEF.send_email(
            'HV-WHS / Script Summary - VS_Fixtures',
            email_body,
            _AUTH.email_recipient,
            _AUTH.email_sender,
            _AUTH.smtp_server,
            _AUTH.smtp_port,
            _AUTH.email_username,
            _AUTH.email_password
        )
