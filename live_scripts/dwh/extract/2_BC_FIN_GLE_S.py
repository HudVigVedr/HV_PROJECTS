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

script_name = "BC_GLentries (S)_Bulk"
script_cat = "DWH_extract"

# SQL Server connection settings
connection_string = f"DRIVER=ODBC Driver 17 for SQL Server;SERVER={_AUTH.server};DATABASE={_AUTH.database};UID={_AUTH.username};PWD={_AUTH.password}"
sql_table = "dbo.BC_GLentries"
staging_table =  "_staging_" + sql_table 
entryno = "entryNo"

# API endpoint URL 
api_url = _AUTH.end_REST_BOLTRICS_BC
api_table = "generalLedgerEntries"
api_full = api_url + "/" + api_table + "?company="

columns_insert = [
    "[@odata.etag]",
    "id",
    "systemCreatedAt",
    "systemCreatedBy",
    "systemModifiedAt",
    "systemModifiedBy",
    "entryNo",
    "gLAccountNo",
    "postingDate",
    "documentType",
    "documentNo",
    "description",
    "balAccountNo",
    "amount",
    "globalDimension1Code",
    "globalDimension2Code",
    "userID",
    "sourceCode",
    "systemCreatedEntry",
    "priorYearEntry",
    "jobNo",
    "quantity",
    "vatAmount",
    "businessUnitCode",
    "journalBatchName",
    "reasonCode",
    "genPostingType",
    "genBusPostingGroup",
    "genProdPostingGroup",
    "balAccountType",
    "transactionNo",
    "debitAmount",
    "creditAmount",
    "documentDate",
    "externalDocumentNo",
    "sourceType",
    "sourceNo",
    "noSeries",
    "taxAreaCode",
    "taxLiable",
    "taxGroupCode",
    "useTax",
    "vatBusPostingGroup",
    "vatProdPostingGroup",
    "additionalCurrencyAmount",
    "addCurrencyDebitAmount",
    "addCurrencyCreditAmount",
    "closeIncomeStatementDimID",
    "icPartnerCode",
    "reversed",
    "reversedByEntryNo",
    "reversedEntryNo",
    "gLAccountName",
    "journalTemplName",
    "dimensionSetID",
    "shortcutDimension3Code",
    "shortcutDimension4Code",
    "shortcutDimension5Code",
    "shortcutDimension6Code",
    "shortcutDimension7Code",
    "shortcutDimension8Code",
    "lastDimCorrectionEntryNo",
    "lastDimCorrectionNode",
    "dimensionChangesCount",
    "prodOrderNo",
    "faEntryType",
    "faEntryNo",
    "comment",
    "accountId",
    "lastModifiedDateTime",
    "documentLineNo3PL",
    "wmsDocumentType",
    "wmsDocumentNo",
    "wmsDocumentLineNo",
    "tmsDocumentType",
    "tmsDocumentNo",
    "tmsDocumentSequenceNo",
    "tmsDocumentLineNo",
    "ultimo",
    "Entity"
]

def get_max_entry_no_per_entity(connection):
    query = f"""
        SELECT Entity, MAX({entryno}) as MaxEntryNo
        FROM {sql_table}
        GROUP BY Entity
    """
    cursor = connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    return {row[0]: row[1] for row in results}

def truncate_table(connection, table_name):
    cursor = connection.cursor()
    cursor.execute(f"TRUNCATE TABLE {table_name}")
    connection.commit()

def bulk_insert_staging(connection, data, staging_table, company_name, columns):
    if not data:
        return
    placeholders = ", ".join(["?"] * len(columns))
    columns_sql = ", ".join([f"[{col}]" for col in columns])
    insert_sql = f"INSERT INTO {staging_table} ({columns_sql}) VALUES ({placeholders})"
    
    values = []
    for item in data:
        row = []
        for col in columns:
            if col == "Entity":
                row.append(company_name)
            else:
                row.append(item.get(col, None))
        values.append(row)
    
    cursor = connection.cursor()
    cursor.executemany(insert_sql, values)
    connection.commit()

def insert_new_records(connection, staging_table, target_table, columns):
    """
    Insert records from the staging table into the target table only if they don't already exist.
    Existence is determined by matching entryNo and Entity.
    """
    columns_sql = ", ".join([f"[{col}]" for col in columns])
    sql_insert = f"""
        INSERT INTO {target_table} ({columns_sql})
        SELECT {columns_sql}
        FROM {staging_table} S
        WHERE NOT EXISTS (
            SELECT 1 FROM {target_table} T
            WHERE T.entryNo = S.entryNo AND T.Entity = S.Entity
        )
    """
    cursor = connection.cursor()
    cursor.execute(sql_insert)
    connection.commit()

if __name__ == "__main__":
    print("Incremental refresh BC_GLentries (skipping existing records) using staging...")
    connection = pyodbc.connect(connection_string)
    threshold = 0
    start_time = _DEF.datetime.now()
    overall_status = "Success"
    total_inserted_rows = 0

    try:
        company_names = _DEF.get_company_names(connection)
        max_entry_nos = get_max_entry_no_per_entity(connection)
        
        # Clear out staging table for a fresh load.
        truncate_table(connection, staging_table)

        for company_name in company_names:
            max_entry_no = max_entry_nos.get(company_name, 0)
            api = f"{api_full}{company_name}&$filter=entryNo gt {max_entry_no}"
            print(f"Fetching data for {company_name} with max entryNo {max_entry_no}")
            print(api)
            api_data_generator = _DEF.make_api_request(api, _AUTH.client_id, _AUTH.client_secret, _AUTH.token_url)
            data_to_insert = list(api_data_generator)
            row_count = len(data_to_insert)
            
            if row_count > threshold:
                bulk_insert_staging(connection, data_to_insert, staging_table, company_name, columns_insert)
                total_inserted_rows += row_count
                print(f"Staging: Inserted {row_count} rows for company {company_name}.")
        
        if total_inserted_rows > 0:
            # Insert only new records from staging to target.
            insert_new_records(connection, staging_table, sql_table, columns_insert)
            print("Inserted new records from staging to target table.")
        else:
            print("No new records found to process.")
    
    except Exception as e:
        overall_status = "Error"
        error_details = str(e)
        print(f"An error occurred: {e}")
        _DEF.log_status(connection, "Error", script_cat, script_name, start_time, _DEF.datetime.now(),
                        int((_DEF.datetime.now() - start_time).total_seconds() / 60), 0,
                        error_details, "None", "N/A")
        _DEF.send_email_mfa(f"ErrorLog -> {script_name} / {script_cat}",
                            error_details, _AUTH.email_sender, _AUTH.email_recipient,
                            _AUTH.guid_blink, _AUTH.email_client_id, _AUTH.email_client_secret)
    
    finally:
        if overall_status == "Success":
            success_message = f"Total rows processed: {total_inserted_rows}."
            _DEF.log_status(connection, "Success", script_cat, script_name, start_time, _DEF.datetime.now(),
                            int((_DEF.datetime.now() - start_time).total_seconds() / 60), total_inserted_rows,
                            success_message, "All", "N/A")
        connection.close()
