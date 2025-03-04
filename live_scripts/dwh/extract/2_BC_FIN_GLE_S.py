# ==> This script will skip records that already exist <==


import requests
import pyodbc
import json
import smtplib
from email.mime.text import MIMEText
import time

import sys
sys.path.append('C:/Python/HV_PROJECTS')
import _AUTH
import _DEF 

script_name = "BC_GLentries (S)"
script_cat = "DWH_extract"


# SQL Server connection settings
connection_string = f"DRIVER=ODBC Driver 17 for SQL Server;SERVER={_AUTH.server};DATABASE={_AUTH.database};UID={_AUTH.username};PWD={_AUTH.password}"

sql_table = "dbo.BC_GLentries"
entryno = "entryNo"

# API endpoint URL 
api_url = _AUTH.end_REST_BOLTRICS_BC
api_table = "generalLedgerEntries"
#api_full = api_url + "/" + api_table + "?$filter=systemModifiedAt gt "+ _DEF.yesterday_date +"T00:00:00Z&company="
api_full = api_url + "/" + api_table + "?company="

columns_insert = ["[@odata.etag]"
            ,"id"
            ,"systemCreatedAt"
            ,"systemCreatedBy"
            ,"systemModifiedAt"
            ,"systemModifiedBy"
            ,"entryNo"
            ,"gLAccountNo"
            ,"postingDate"
            ,"documentType"
            ,"documentNo"
            ,"description"
            ,"balAccountNo"
            ,"amount"
            ,"globalDimension1Code"
            ,"globalDimension2Code"
            ,"userID"
            ,"sourceCode"
            ,"systemCreatedEntry"
            ,"priorYearEntry"
            ,"jobNo"
            ,"quantity"
            ,"vatAmount"
            ,"businessUnitCode"
            ,"journalBatchName"
            ,"reasonCode"
            ,"genPostingType"
            ,"genBusPostingGroup"
            ,"genProdPostingGroup"
            ,"balAccountType"
            ,"transactionNo"
            ,"debitAmount"
            ,"creditAmount"
            ,"documentDate"
            ,"externalDocumentNo"
            ,"sourceType"
            ,"sourceNo"
            ,"noSeries"
            ,"taxAreaCode"
            ,"taxLiable"
            ,"taxGroupCode"
            ,"useTax"
            ,"vatBusPostingGroup"
            ,"vatProdPostingGroup"
            ,"additionalCurrencyAmount"
            ,"addCurrencyDebitAmount"
            ,"addCurrencyCreditAmount"
            ,"closeIncomeStatementDimID"
            ,"icPartnerCode"
            ,"reversed"
            ,"reversedByEntryNo"
            ,"reversedEntryNo"
            ,"gLAccountName"
            ,"journalTemplName"
            ,"dimensionSetID"
            ,"shortcutDimension3Code"
            ,"shortcutDimension4Code"
            ,"shortcutDimension5Code"
            ,"shortcutDimension6Code"
            ,"shortcutDimension7Code"
            ,"shortcutDimension8Code"
            ,"lastDimCorrectionEntryNo"
            ,"lastDimCorrectionNode"
            ,"dimensionChangesCount"
            ,"prodOrderNo"
            ,"faEntryType"
            ,"faEntryNo"
            ,"comment"
            ,"accountId"
            ,"lastModifiedDateTime"
            ,"documentLineNo3PL"
            ,"wmsDocumentType"
            ,"wmsDocumentNo"
            ,"wmsDocumentLineNo"
            ,"tmsDocumentType"
            ,"tmsDocumentNo"
            ,"tmsDocumentSequenceNo"
            ,"tmsDocumentLineNo"
            ,"ultimo"
            ,"Entity" ]

def get_max_entry_no_per_entity(connection):
    query = f"""
        SELECT Entity, MAX({entryno}) as MaxEntryNo
        FROM {sql_table}
        GROUP BY Entity
    """
    cursor = connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    
    # Convert results to a dictionary for easy lookup
    return {row[0]: row[1] for row in results}



if __name__ == "__main__":
    print("Incremental refresh BC_GLentries to SQL/Staging...")
    connection = pyodbc.connect(connection_string)
    threshold = 0

    start_time = _DEF.datetime.now()
    overall_status = "Success"
    total_inserted_rows = 0

    try:
        company_names = _DEF.get_company_names(connection)
        max_entry_nos = get_max_entry_no_per_entity(connection)

        for company_name in company_names:
            
            max_entry_no = max_entry_nos.get(company_name, 0)
            print(max_entry_no)
            #api = f"{api_full}BMA"
            api = f"{api_full}{company_name}&$filter=entryNo gt {max_entry_no}"
            print(api)
           
            api_data_generator = _DEF.make_api_request(api, _AUTH.client_id, _AUTH.client_secret, _AUTH.token_url)

            data_to_insert = list(api_data_generator)
            row_count = len(data_to_insert)

            if row_count > threshold:
                _DEF.insert_data_into_sql(connection, data_to_insert, sql_table, company_name, columns_insert)
                inserted_rows = _DEF.count_rows(data_to_insert)  # Assuming all rows are successfully inserted
                total_inserted_rows += inserted_rows

                if inserted_rows != row_count:
                    overall_status = "Error"
                    error_details = f"Expected to insert {row_count} rows, but only {inserted_rows} were inserted."
                    _DEF.log_status(connection, "Error", script_cat, script_name, start_time, _DEF.datetime.now(), int((_DEF.datetime.now() - start_time).total_seconds() / 60), row_count - inserted_rows, error_details, company_name, api)

                    _DEF.send_email_mfa(f"ErrorLog -> {script_name} / {script_cat}", error_details,  _AUTH.email_sender,  _AUTH.email_recipient, _AUTH.guid_blink, _AUTH.email_client_id, _AUTH.email_client_secret)    


    except Exception as e:
        overall_status = "Error"
        error_details = str(e)
        print(f"An error occurred: {e}")
        _DEF.log_status(connection, "Error", script_cat, script_name, start_time, _DEF.datetime.now(), int((_DEF.datetime.now() - start_time).total_seconds() / 60), 0, error_details, "None", "N/A")

        _DEF.send_email_mfa(f"ErrorLog -> {script_name} / {script_cat}", error_details,  _AUTH.email_sender,  _AUTH.email_recipient, _AUTH.guid_blink, _AUTH.email_client_id, _AUTH.email_client_secret)    

    finally:
        if overall_status == "Success":
            success_message = f"Total rows inserted: {total_inserted_rows}."
            _DEF.log_status(connection, "Success", script_cat, script_name, start_time, _DEF.datetime.now(), int((_DEF.datetime.now() - start_time).total_seconds() / 60), total_inserted_rows, success_message, "All", "N/A")

        elif overall_status == "Error":
            # Additional logging for error scenario can be added here if needed
            pass