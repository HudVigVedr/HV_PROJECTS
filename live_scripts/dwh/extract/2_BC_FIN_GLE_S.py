import requests
import pyodbc
import json
import sys
from email.mime.text import MIMEText
import time

sys.path.append('C:/Python/HV_PROJECTS')
import _AUTH
import _DEF

script_name = "BC_GLentries_Bulk"
script_cat = "DWH_extract"
script_type = "Copying"

# SQL Server connection settings
connection_string = (
    f"DRIVER=ODBC Driver 17 for SQL Server;"
    f"SERVER={_AUTH.server};DATABASE={_AUTH.database};UID={_AUTH.username};PWD={_AUTH.password}"
)

# Target and staging tables
target_table = "BC_GLentries"
staging_table = "_staging_" + target_table
entryno = "entryNo"

# API endpoint
api_url = _AUTH.end_REST_BOLTRICS_BC
api_table = "generalLedgerEntries"
select = "$select=id,systemCreatedAt,systemCreatedBy,systemModifiedAt,systemModifiedBy,entryNo,gLAccountNo,postingDate,documentType,documentNo,description,balAccountNo,amount,globalDimension1Code,globalDimension2Code,userID,sourceCode,systemCreatedEntry,priorYearEntry,jobNo,quantity,vatAmount,businessUnitCode,journalBatchName,reasonCode,genPostingType,genBusPostingGroup,genProdPostingGroup,balAccountType,transactionNo,debitAmount,creditAmount,documentDate,externalDocumentNo,sourceType,sourceNo,noSeries,taxAreaCode,taxLiable,taxGroupCode,useTax,vatBusPostingGroup,vatProdPostingGroup,additionalCurrencyAmount,addCurrencyDebitAmount,addCurrencyCreditAmount,closeIncomeStatementDimID,icPartnerCode,reversed,reversedByEntryNo,reversedEntryNo,gLAccountName,journalTemplName,dimensionSetID,shortcutDimension3Code,shortcutDimension4Code,shortcutDimension5Code,shortcutDimension6Code,shortcutDimension7Code,shortcutDimension8Code,lastDimCorrectionEntryNo,lastDimCorrectionNode,dimensionChangesCount,prodOrderNo,faEntryType,faEntryNo,comment,accountId,lastModifiedDateTime,documentLineNo3PL,wmsDocumentType,wmsDocumentNo,wmsDocumentLineNo,tmsDocumentType,tmsDocumentNo,tmsDocumentSequenceNo,tmsDocumentLineNo,ultimo"
api_full = f"{api_url}/{api_table}?{select}&company="

columns_insert = [

    "id", "systemCreatedAt", "systemCreatedBy", "systemModifiedAt", "systemModifiedBy",
    "entryNo", "gLAccountNo", "postingDate", "documentType", "documentNo", "description",
    "balAccountNo", "amount", "globalDimension1Code", "globalDimension2Code", "userID",
    "sourceCode", "systemCreatedEntry", "priorYearEntry", "jobNo", "quantity", "vatAmount",
    "businessUnitCode", "journalBatchName", "reasonCode", "genPostingType", "genBusPostingGroup",
    "genProdPostingGroup", "balAccountType", "transactionNo", "debitAmount", "creditAmount",
    "documentDate", "externalDocumentNo", "sourceType", "sourceNo", "noSeries", "taxAreaCode",
    "taxLiable", "taxGroupCode", "useTax", "vatBusPostingGroup", "vatProdPostingGroup",
    "additionalCurrencyAmount", "addCurrencyDebitAmount", "addCurrencyCreditAmount",
    "closeIncomeStatementDimID", "icPartnerCode", "reversed", "reversedByEntryNo",
    "reversedEntryNo", "gLAccountName", "journalTemplName", "dimensionSetID",
    "shortcutDimension3Code", "shortcutDimension4Code", "shortcutDimension5Code",
    "shortcutDimension6Code", "shortcutDimension7Code", "shortcutDimension8Code",
    "lastDimCorrectionEntryNo", "lastDimCorrectionNode", "dimensionChangesCount",
    "prodOrderNo", "faEntryType", "faEntryNo", "comment", "accountId", "lastModifiedDateTime",
    "documentLineNo3PL", "wmsDocumentType", "wmsDocumentNo", "wmsDocumentLineNo",
    "tmsDocumentType", "tmsDocumentNo", "tmsDocumentSequenceNo", "tmsDocumentLineNo", "ultimo","Entity"
]


def bulk_insert_staging(connection, data, staging_table, company_name):
    """Bulk inserts data into the staging table."""
    if not data:
        return
    
    columns = list(data[0].keys()) + ["Entity"]
    placeholders = ", ".join(["?"] * len(columns))
    columns_sql = ", ".join([f"[{col}]" for col in columns])
    insert_sql = f"INSERT INTO {staging_table} ({columns_sql}) VALUES ({placeholders})"
    
    values = [[item[col] for col in data[0].keys()] + [company_name] for item in data]
    cursor = connection.cursor()
    cursor.executemany(insert_sql, values)
    connection.commit()


def merge_staging_to_target(connection, staging_table, target_table, columns):
    """Merges data from staging into the target table."""
    update_columns = [col for col in columns if col not in ("id", "Entity")]
    
    merge_sql = f"""
    MERGE {target_table} AS Target
    USING {staging_table} AS Source
    ON Target.[id] = Source.[id] AND Target.[Entity] = Source.[Entity]
    WHEN MATCHED THEN
        UPDATE SET {", ".join([f"Target.[{col}] = Source.[{col}]" for col in update_columns])}
    WHEN NOT MATCHED THEN
        INSERT ({", ".join([f"[{col}]" for col in columns])})
        VALUES ({", ".join([f"Source.[{col}]" for col in columns])});
    """
    cursor = connection.cursor()
    cursor.execute(merge_sql)
    connection.commit()


def truncate_staging(connection, staging_table):
    """Truncates the staging table."""
    cursor = connection.cursor()
    cursor.execute(f"TRUNCATE TABLE {staging_table}")
    connection.commit()


def get_max_entry_no_per_entity(connection):
    """Fetches the maximum entryNo per entity from the target table."""
    query = f"SELECT Entity, MAX({entryno}) as MaxEntryNo FROM {target_table} GROUP BY Entity"
    cursor = connection.cursor()
    cursor.execute(query)
    return {row[0]: row[1] for row in cursor.fetchall()}


if __name__ == "__main__":
    print("Bulk loading BC_GLentries into staging and merging to target table...")
    connection = pyodbc.connect(connection_string)
    start_time = _DEF.datetime.now()
    overall_status = "Success"
    total_rows = 0

    try:
        truncate_staging(connection, staging_table)
        
        company_names = _DEF.get_company_names(connection)
        max_entry_nos = get_max_entry_no_per_entity(connection)
        first_data = True
        columns = []
        
        for company_name in company_names:
            max_entry_no = max_entry_nos.get(company_name, 0)
            api = f"{api_full}{company_name}&$filter=entryNo gt {max_entry_no}"
            print(api)
            
            api_data_generator = _DEF.make_api_request(api, _AUTH.client_id, _AUTH.client_secret, _AUTH.token_url)
            data_to_insert = list(api_data_generator)
            row_count = len(data_to_insert)
            
            if row_count > 0:
                total_rows += row_count
                if first_data and data_to_insert:
                    columns = list(data_to_insert[0].keys()) + ["Entity"]
                    first_data = False
                bulk_insert_staging(connection, data_to_insert, staging_table, company_name)
        
        if total_rows > 0:
            merge_staging_to_target(connection, staging_table, target_table, columns)
            truncate_staging(connection, staging_table)
        else:
            print("No data found to process.")
        
    except Exception as e:
        overall_status = "Error"
        error_details = str(e)
        print(f"An error occurred: {e}")
        _DEF.log_status(connection, "Error", script_cat, script_name, start_time, _DEF.datetime.now(), 0, error_details, "None", company_name,"N/A")
        _DEF.send_email_mfa(f"ErrorLog -> {script_name} / {script_cat}", error_details, _AUTH.email_sender, _AUTH.email_recipient, _AUTH.guid_blink, _AUTH.email_client_id, _AUTH.email_client_secret)
    
    finally:
        if overall_status == "Success":
            _DEF.log_status(connection, "Success", script_cat, script_name, start_time, _DEF.datetime.now(),int((_DEF.datetime.now() - start_time).total_seconds() / 60),total_rows, f"Total rows processed: {total_rows}.", "All", "N/A")
            #_DEF.log_status(connection, "Success", script_cat, script_name, start_time, _DEF.datetime.now(), int((_DEF.datetime.now() - start_time).total_seconds() / 60), total_inserted_rows, success_message, "All", "N/A")
        connection.close()