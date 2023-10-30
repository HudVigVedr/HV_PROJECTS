import requests
import pyodbc
import json
import AUTH
import _DEF
import smtplib
from email.mime.text import MIMEText
from datetime import datetime, timedelta

# SQL Server connection settings
connection_string = f"DRIVER=ODBC Driver 17 for SQL Server;SERVER={AUTH.server};DATABASE={AUTH.database};UID={AUTH.username};PWD={AUTH.password}"
connection_string2 = f"DRIVER=ODBC Driver 17 for SQL Server;SERVER=HV-db;DATABASE=Staging;UID=hheij;PWD=ByMus&060R6f"
sql_table = "dbo.TestGLentries"

from datetime import datetime, timedelta

def get_yesterday_date():
    yesterday = datetime.now() - timedelta(days=1)
    return yesterday.strftime('%Y-%m-%d')

yesterday_date = get_yesterday_date()

# API endpoint URL (same as before) -> aanvullen
api_url = AUTH.end_REST_BC
api_table = "generalLedgerEntries"
api_full = api_url + "/" + api_table + "?$filter=systemModifiedAt gt "+ yesterday_date +"T00:00:00Z&company="

# delete function SQL table
def delete_sql_table(connection):
    cursor = connection.cursor()
    cursor.execute(f"DELETE FROM {sql_table}")
    connection.commit()


# Function to insert data into SQL Server
def insert_data_into_sql(connection, data, sql_table, company_name):
    cursor = connection.cursor()

    sql_update = f"UPDATE {sql_table} SET id = ?, systemCreatedAt = ?, systemModifiedBy = ? WHERE no = ?"

        for item in data:

            # Check if row exists
            cursor.execute(f"SELECT * FROM {sql_table} WHERE no = ?", (item['no'],))
            row = cursor.fetchone()

            if row is None:
                    cursor.execute(sql_insert, (
                    item['id'], 
                    item['systemCreatedAt'], 
                    item['systemModifiedBy'],
                    item['no']
                ))
            else:
                cursor.execute(sql_update, (
                    item['id'], 
                    item['systemCreatedAt'], 
                    item['systemModifiedBy'],
                    item['no']
                ))



    sql_insert = f"""
        INSERT INTO {sql_table} (
            [odata_etag],
            [id],
            [systemCreatedAt],
            [systemCreatedBy],
            [systemModifiedAt],
            [systemModifiedBy],
            [entryNo],
            [gLAccountNo],
            [postingDate],
            [documentType],
            [documentNo],
            [description],
            [balAccountNo],
            [amount],
            [globalDimension1Code],
            [globalDimension2Code],
            [userID],
            [sourceCode],
            [systemCreatedEntry],
            [priorYearEntry],
            [jobNo],
            [quantity],
            [vatAmount],
            [businessUnitCode],
            [journalBatchName],
            [reasonCode],
            [genPostingType],
            [genBusPostingGroup],
            [genProdPostingGroup],
            [balAccountType],
            [transactionNo],
            [debitAmount],
            [creditAmount],
            [documentDate],
            [externalDocumentNo],
            [sourceType],
            [sourceNo],
            [noSeries],
            [taxAreaCode],
            [taxLiable],
            [taxGroupCode],
            [useTax],
            [vatBusPostingGroup],
            [vatProdPostingGroup],
            [additionalCurrencyAmount],
            [addCurrencyDebitAmount],
            [addCurrencyCreditAmount],
            [closeIncomeStatementDimID],
            [icPartnerCode],
            [reversed],
            [reversedByEntryNo],
            [reversedEntryNo],
            [gLAccountName],
            [journalTemplName],
            [dimensionSetID],
            [shortcutDimension3Code],
            [shortcutDimension4Code],
            [shortcutDimension5Code],
            [shortcutDimension6Code],
            [shortcutDimension7Code],
            [shortcutDimension8Code],
            [lastDimCorrectionEntryNo],
            [lastDimCorrectionNode],
            [dimensionChangesCount],
            [prodOrderNo],
            [faEntryType],
            [faEntryNo],
            [comment],
            [accountId],
            [lastModifiedDateTime],
            [documentLineNo3PL],
            [wmsDocumentType],
            [wmsDocumentNo],
            [wmsDocumentLineNo],
            [tmsDocumentType],
            [tmsDocumentNo],
            [tmsDocumentSequenceNo],
            [tmsDocumentLineNo],
            [ultimo],
            [Entity]  -- add Entity column
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    for item in data:
        values = list(item.values())
        values.append(company_name)  # add company name to the list of values
        cursor.execute(sql_insert, tuple(values))

    connection.commit()

if __name__ == "__main__":
    try:
        # Establish the SQL Server connection
        connection1 = pyodbc.connect(connection_string2)
        connection = pyodbc.connect(connection_string)

        # Get a list of company names from SQL Server
        company_names = _DEF.get_company_names(connection1)

        delete_sql_table(connection)

        for company_name in company_names:
            api = f"{api_full}{company_name}"  
            access_token = _DEF.get_access_token(AUTH.client_id, AUTH.client_secret, AUTH.token_url)  

            if access_token:
                api_data = _DEF.make_api_request(api, access_token) 

                if api_data:
                    insert_data_into_sql(connection, api_data['value'] , sql_table, company_name) 

                _DEF.send_email(
                    'Script Success',
                    'The script completed successfully.',
                    AUTH.email_recipient,
                    AUTH.email_sender,
                    AUTH.smtp_server,
                    AUTH.smtp_port,
                    AUTH.email_username,
                    AUTH.email_password
                )

    except Exception as e:
        print(f"An error occurred for {company_name}: {e}")

        _DEF.send_email(
            'Script Failure',
            f'The script failed with the following error:\n\n{e}',
            AUTH.email_recipient,
            AUTH.email_sender,
            AUTH.smtp_server,
            AUTH.smtp_port,
            AUTH.email_username,
            AUTH.email_password
        )

    finally:
        connection.close()


