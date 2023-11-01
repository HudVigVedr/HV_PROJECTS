import requests
import pyodbc
import json
import _AUTH
import _DEF
import smtplib
from email.mime.text import MIMEText
import time

# SQL Server connection settings
connection_string = f"DRIVER=ODBC Driver 17 for SQL Server;SERVER={_AUTH.server};DATABASE={_AUTH.database};UID={_AUTH.username};PWD={_AUTH.password}"
#connection_string2 = f"DRIVER=ODBC Driver 17 for SQL Server;SERVER=HV-db;DATABASE=Staging;UID=hheij;PWD=ByMus&060R6f"
sql_table = "dbo.BC_GLaccounts"
print("SQL Server connection string created")

# API endpoint URL (same as before) -> aanvullen
api_url = _AUTH.end_REST_BOLTRICS_BC
api_table = "generalLedgerAccounts"
api_full = api_url + "/" + api_table + "?company="

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
            [etag]
            ,[id]
            ,[systemCreatedAt]
            ,[systemCreatedBy]
            ,[systemModifiedAt]
            ,[systemModifiedBy]
            ,[no]
            ,[name]
            ,[searchName]
            ,[accountType]
            ,[globalDimension1Code]
            ,[globalDimension2Code]
            ,[accountCategory]
            ,[incomeBalance]
            ,[debitCredit]
            ,[no2]
            ,[comment]
            ,[blocked]
            ,[directPosting]
            ,[reconciliationAccount]
            ,[newPage]
            ,[noOfBlankLines]
            ,[indentation]
            ,[lastModifiedDateTime]
            ,[lastDateModified]
            ,[balanceAtDate]
            ,[netChange]
            ,[budgetedAmount]
            ,[totaling]
            ,[balance]
            ,[budgetAtDate]
            ,[consolTranslationMethod]
            ,[consolDebitAcc]
            ,[consolCreditAcc]
            ,[genPostingType]
            ,[genBusPostingGroup]
            ,[genProdPostingGroup]
            ,[debitAmount]
            ,[creditAmount]
            ,[automaticExtTexts]
            ,[budgetedDebitAmount]
            ,[budgetedCreditAmount]
            ,[taxAreaCode]
            ,[taxLiable]
            ,[taxGroupCode]
            ,[vatBusPostingGroup]
            ,[vatProdPostingGroup]
            ,[vatAmt]
            ,[additionalCurrencyNetChange]
            ,[addCurrencyBalanceAtDate]
            ,[additionalCurrencyBalance]
            ,[exchangeRateAdjustment]
            ,[addCurrencyDebitAmount]
            ,[addCurrencyCreditAmount]
            ,[defaultICPartnerGLAccNo]
            ,[accountSubcategoryEntryNo]
            ,[accountSubcategoryDescript]
            ,[costTypeNo]
            ,[defaultDeferralTemplateCode]
            ,[apiAccountType]
            ,[omitDefaultDescrInJnl]
            ,[availableForPurchase3PL]
            ,[availableForSales3PL]
            ,[pictureMediaEditLink]
            ,[pictureMediaReadLink]
            ,[Entity]
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? ,?)
    """

    for item in data:
        values = list(item.values())
        values.append(company_name)  # add company name to the list of values
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

        delete_sql_table(connection)

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
            'HV-WHS / Script Summary - BC_FIN_GLaccounts',
            email_body,
            _AUTH.email_recipient,
            _AUTH.email_sender,
            _AUTH.smtp_server,
            _AUTH.smtp_port,
            _AUTH.email_username,
            _AUTH.email_password
        )
