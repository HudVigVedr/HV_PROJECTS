import requests
import pyodbc
import json
import smtplib
from email.mime.text import MIMEText
import time

import sys
sys.path.append('H:/HV-PROJECTS')
import _AUTH
import _DEF 

# SQL Server connection settings
connection_string = f"DRIVER=ODBC Driver 17 for SQL Server;SERVER={_AUTH.server};DATABASE={_AUTH.database};UID={_AUTH.username};PWD={_AUTH.password}"

sql_table = "dbo.BC_wmsDH"
print("SQL Server connection string created")


# API endpoint URL (same as before) -> aanvullen
api_url = _AUTH.end_REST_BOLTRICS_BC
api_table = "wmsDocumentHeaders"
api_full = api_url + "/" + api_table + "?" + "$select=announcedDate,announcedTime,arrivedDate,arrivedTime,attribute01,attribute02,attribute03,attribute04,attribute05,attribute06,attribute07,attribute08,attribute09,attribute10,billToCustomerName,billToCustomerNo,createdDateTime,createdUserID,deliveryDate,departedDate,documentDate,documentType,estimatedDepartureDate,id,modifiedDateTime,modifiedUserID,movementType,no,portFromName,portToName,postingDate,sellToCustomerName,sellToCustomerNo,shortcutDimension2Code,statusCode,vesselNo,voyageNo&$filter=systemModifiedAt gt "+ _DEF.yesterday_date +"T00:00:00Z&company="



# Delete function
def delete_sql_table(connection):
    print("Deleting SQL table")
    cursor = connection.cursor()
    cursor.execute(f"DELETE FROM {sql_table}")
    connection.commit()

# Function to insert data into SQL Server
def insert_data_into_sql(connection, data, sql_table, company_name):
    
    cursor = connection.cursor()

    sql_insert = f"""
        INSERT INTO {sql_table} (
            [ODataEtag]
            ,[Id]
            ,[DocumentType]
            ,[No]
            ,[SellToCustomerNo]
            ,[SellToCustomerName]
            ,[BillToCustomerNo]
            ,[BillToCustomerName]
            ,[VoyageNo]
            ,[MovementType]
            ,[DocumentDate]
            ,[PostingDate]
            ,[StatusCode]
            ,[CreatedDateTime]
            ,[CreatedUserID]
            ,[ModifiedDateTime]
            ,[ModifiedUserID]
            ,[AnnouncedDate]
            ,[AnnouncedTime]
            ,[ArrivedDate]
            ,[ArrivedTime]
            ,[DepartedDate]
            ,[DeliveryDate]
            ,[EstimatedDepartureDate]
            ,[VesselNo]
            ,[ShortcutDimension2Code]
            ,[Attribute01]
            ,[Attribute02]
            ,[Attribute03]
            ,[Attribute04]
            ,[Attribute05]
            ,[Attribute06]
            ,[Attribute07]
            ,[Attribute08]
            ,[Attribute09]
            ,[Attribute10]
            ,[PortFromName]
            ,[PortToName]
            ,[Entity]
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
 # SQL Query to update an existing record
    sql_update = f"""
        UPDATE {sql_table} (
            [ODataEtag]
            ,[Id]
            ,[DocumentType]
            ,[No]
            ,[SellToCustomerNo]
            ,[SellToCustomerName]
            ,[BillToCustomerNo]
            ,[BillToCustomerName]
            ,[VoyageNo]
            ,[MovementType]
            ,[DocumentDate]
            ,[PostingDate]
            ,[StatusCode]
            ,[CreatedDateTime]
            ,[CreatedUserID]
            ,[ModifiedDateTime]
            ,[ModifiedUserID]
            ,[AnnouncedDate]
            ,[AnnouncedTime]
            ,[ArrivedDate]
            ,[ArrivedTime]
            ,[DepartedDate]
            ,[DeliveryDate]
            ,[EstimatedDepartureDate]
            ,[VesselNo]
            ,[ShortcutDimension2Code]
            ,[Attribute01]
            ,[Attribute02]
            ,[Attribute03]
            ,[Attribute04]
            ,[Attribute05]
            ,[Attribute06]
            ,[Attribute07]
            ,[Attribute08]
            ,[Attribute09]
            ,[Attribute10]
            ,[PortFromName]
            ,[PortToName]
            ,[Entity]
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    

    sql_check_exists = f"""
        SELECT 1 FROM {sql_table}
        WHERE [No] = ? AND [Entity] = ?
    """


    for item in data:
        values = list(item.values())
        entity_id = item.get('No')
        values.append(company_name)  # Add company name to the list of values

        # Check if the record exists
        cursor.execute(sql_check_exists, (entity_id, company_name))
        if cursor.fetchone():
            # Record exists, perform an update
            cursor.execute(sql_update, (*values, entity_id, company_name))
        else:
            # Record does not exist, perform an insert
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
        #connection1 = pyodbc.connect(connection_string2)
        print("Establishing SQL Server connection")
        connection = pyodbc.connect(connection_string)

        # Get a list of company names from SQL Server
        company_names = _DEF.get_company_names(connection)

        for company_name in company_names:
            print(f"Processing company: {company_name}")
            iteration_rows_inserted = 0  # Initialize counter for rows inserted in this iteration
            api = f"{api_full}{company_name}"  
            access_token = _DEF.get_access_token(_AUTH.client_id, _AUTH.client_secret, _AUTH.token_url)  

            if access_token:
                api_data_generator = _DEF.make_api_request(api, _AUTH.client_id, _AUTH.client_secret, _AUTH.token_url)

            try:
                if api_data_generator:
                    for api_data in api_data_generator:
                        #print(f"Type of api_data: {type(api_data)}")  # Debugging print statement
                        insert_data_into_sql(connection, [api_data], sql_table, company_name)
                        rows_inserted += 1 
                        iteration_rows_inserted += 1  # Increment rows_inserted

                    successes.append(company_name)  # Record successful iteration
                    rows_inserted_per_iteration[company_name] = iteration_rows_inserted  # Record rows inserted in this iteration

            except Exception as e:
                failures.append((company_name, str(e)))


    finally:
        print("Closing SQL Server connection")
        connection.close()
        end_time = time.time()  # Record end time
        duration = (end_time - start_time )/60 # Calculate duration
        duration_minutes_rounded = round(duration, 2)

        # Print results
        print(f"Total rows inserted: {rows_inserted}")
        print(f"Total time taken: {duration} minutes")

        
        # Prepare email content
        email_body = f"The script completed in {duration_minutes_rounded} minutes with {rows_inserted} total rows inserted.\n\n"
        if successes:
            email_body += "Successes:\n" + "\n".join(successes) + "\n\n"
        if failures:
            email_body += "Failures:\n" + "\n".join(f"{company}: {error}" for company, error in failures)

        # Include rows inserted per iteration in email
        for company, rows in rows_inserted_per_iteration.items():
            email_body += f"Rows inserted for {company}: {rows}\n"

        # Send email
        _DEF.send_email(
            'HV-WHS / Script Summary - BC_wmsDH',
            email_body,
            _AUTH.email_recipient,
            _AUTH.email_sender,
            _AUTH.smtp_server,
            _AUTH.smtp_port,
            _AUTH.email_username,
            _AUTH.email_password
        )
