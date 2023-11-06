import requests
import pyodbc
import json
import smtplib
from email.mime.text import MIMEText
import time

import sys
sys.path.append('C:/HV-WHS')
import _AUTH
import _DEF 

# SQL Server connection settings
connection_string = f"DRIVER=ODBC Driver 17 for SQL Server;SERVER={_AUTH.server};DATABASE={_AUTH.database};UID={_AUTH.username};PWD={_AUTH.password}"
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

    sql_insert = f"""
        INSERT INTO {sql_table} (
            [Agreed Ets Cost]
            ,[Agreed Ets Emission]
            ,[Ballast Port]
            ,[Barges]
            ,[Basis P&L]
            ,[Basis vs. Compare]
            ,[Bl Complete]
            ,[Boats]
            ,[Bunk Calc Method]
            ,[Cancel]
            ,[Cargo Booking Nos List]
            ,[Cargo Counterparty Short Names]
            ,[Cargo External Refs List]
            ,[Cargo Grades List]
            ,[Cargo List]
            ,[Cargo Short Names List]
            ,[Chtr Specialist]
            ,[Claim Pending]
            ,[Clean]
            ,[Closed By]
            ,[Closed on Gmt]
            ,[CO2 Quantity]
            ,[Coated]
            ,[Commence Date Gmt]
            ,[Commence Date Local]
            ,[Company Code]
            ,[Compare P&L]
            ,[Complete Date Gmt]
            ,[Complete Date Local]
            ,[Consecutive YN]
            ,[Contract IDs]
            ,[Counterparty Company No]
            ,[Counterparty Short Name]
            ,[Counterparty Type]
            ,[Curr Base]
            ,[Current Port Name]
            ,[Current Port Status]
            ,[Da Desk]
            ,[Daily Cost]
            ,[Daily Cost String]
            ,[Department No]
            ,[Disable Backup Fuel Types]
            ,[Disable Bnkr Warnings]
            ,[Disch Approved]
            ,[Drydock]
            ,[EEOI]
            ,[Entry Date Gmt]
            ,[Equipment List]
            ,[Estimate ID]
            ,[Ets CO2 Cost]
            ,[Ets CO2 Price]
            ,[Ets CO2 Price Currency]
            ,[Ets CO2 Price Exchange Rate]
            ,[Ets CO2 Quantity]
            ,[External Ref]
            ,[FD Manager]
            ,[Finance Coordinator]
            ,[First Load Port]
            ,[First Tci]
            ,[First Tco]
            ,[Fixture No]
            ,[Forecast]
            ,[Fuel Zone Set ID]
            ,[Heating]
            ,[High Risk]
            ,[Ice]
            ,[Include in Cover]
            ,[Is LS Voyage]
            ,[Key Users]
            ,[Last Discharge Port]
            ,[Last Tci]
            ,[Last Tco]
            ,[Last Update Gmt]
            ,[Last User]
            ,[Laycan Alerts]
            ,[Legacy Tow Voyage]
            ,[Legal Hold]
            ,[Lob Code]
            ,[Local UTC Offset]
            ,[Loss Control Notes]
            ,[Loss Control Status]
            ,[Low Sulfur Only]
            ,[LS Miles]
            ,[LS Port Days]
            ,[LS Sea Days]
            ,[Market]
            ,[Market Daily Hire Rate]
            ,[Market Valuation Adjustment]
            ,[Market Valuation Correlation]
            ,[Market Valuation Lumpsum Amount Base]
            ,[Market Valuation Rate Base]
            ,[Market Valuation Rate Unit]
            ,[Market Valuation Symbol]
            ,[Max Daily Boil Off Ballast]
            ,[Max Daily Boil Off Laden]
            ,[MVE Rate]
            ,[Natural Boil Off Ballast Speed]
            ,[Natural Boil Off Laden Speed]
            ,[Next Port Name]
            ,[Next Port Status]
            ,[Next Voyage No]
            ,[Nomination Date]
            ,[Non Pool]
            ,[Offhire Voyage]
            ,[Open Date]
            ,[Operations Report]
            ,[Opr Type]
            ,[Ops Coordinator]
            ,[Ops Coordinator 2]
            ,[P&L Report]
            ,[Parent Vessel Code]
            ,[Parent Voyage No]
            ,[Performance Report]
            ,[Piracy Disabled]
            ,[Piracy Enabled]
            ,[Pool]
            ,[Pool Fee]
            ,[Previous Voyage No]
            ,[Remarks]
            ,[Reposition Port]
            ,[Restrict Edits]
            ,[Scrubber Type]
            ,[Segment]
            ,[Support Job]
            ,[TC/HF Code]
            ,[Tci Bnk Adj]
            ,[Tco Brokers List]
            ,[TCO Chart Coordinator]
            ,[TCO Code]
            ,[Team No]
            ,[Total Load Cargo Volume]
            ,[Total Port Days]
            ,[Total Sale CP Cargo Volume]
            ,[Total Sea Days]
            ,[TR Bunker]
            ,[Track Barge Robs]
            ,[Trade Area Name]
            ,[Trade Area No]
            ,[Transport Work]
            ,[Tve Valid]
            ,[Use Scrubbers]
            ,[Vessel Code]
            ,[Vessel Daily Cost]
            ,[Vessel Name]
            ,[Voyage Aer]
            ,[Voyage Cii Rating]
            ,[Voyage Controller]
            ,[Voyage No]
            ,[Voyage P&L Variance]
            ,[Voyage Range GMT]
            ,[Voyage Reference]
            ,[Voyage Status]
            ,[Voyage Status Code]
            ,[Voyage Template ID]
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    for item in data:
        values = list(item.values())
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
                    print(f"Processing data")
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
