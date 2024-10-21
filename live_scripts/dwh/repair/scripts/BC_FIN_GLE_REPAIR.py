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

script_name_check = "Check GLentries"
script_name_insert = "BC_GLentries_repair"
script_cat = "DWH_repair_manual"

# SQL Server connection settings
connection_string = f"DRIVER=ODBC Driver 17 for SQL Server;SERVER={_AUTH.server};DATABASE={_AUTH.database};UID={_AUTH.username};PWD={_AUTH.password}"

sql_table = "dbo.BC_GLentries"
entryno = "entryNo"

# API endpoint URL 
api_url = _AUTH.end_REST_BOLTRICS_BC
api_table = "generalLedgerEntries"
api_full = api_url + "/" + api_table + "/$count?company="
api_full_insert = api_url + "/" + api_table + "?company="

columns_insert = ["[@odata.etag]", "id", "systemCreatedAt", "systemCreatedBy", "systemModifiedAt", "systemModifiedBy",
                  "entryNo", "gLAccountNo", "postingDate", "documentType", "documentNo", "description", "balAccountNo", 
                  "amount", "globalDimension1Code", "globalDimension2Code", "userID", "sourceCode", "systemCreatedEntry",
                  "priorYearEntry", "jobNo", "quantity", "vatAmount", "businessUnitCode", "journalBatchName", "reasonCode",
                  "genPostingType", "genBusPostingGroup", "genProdPostingGroup", "balAccountType", "transactionNo",
                  "debitAmount", "creditAmount", "documentDate", "externalDocumentNo", "sourceType", "sourceNo", "noSeries",
                  "taxAreaCode", "taxLiable", "taxGroupCode", "useTax", "vatBusPostingGroup", "vatProdPostingGroup",
                  "additionalCurrencyAmount", "addCurrencyDebitAmount", "addCurrencyCreditAmount", "closeIncomeStatementDimID",
                  "icPartnerCode", "reversed", "reversedByEntryNo", "reversedEntryNo", "gLAccountName", "journalTemplName",
                  "dimensionSetID", "shortcutDimension3Code", "shortcutDimension4Code", "shortcutDimension5Code",
                  "shortcutDimension6Code", "shortcutDimension7Code", "shortcutDimension8Code", "lastDimCorrectionEntryNo",
                  "lastDimCorrectionNode", "dimensionChangesCount", "prodOrderNo", "faEntryType", "faEntryNo", "comment",
                  "accountId", "lastModifiedDateTime", "documentLineNo3PL", "wmsDocumentType", "wmsDocumentNo",
                  "wmsDocumentLineNo", "tmsDocumentType", "tmsDocumentNo", "tmsDocumentSequenceNo", "tmsDocumentLineNo", 
                  "ultimo", "Entity"]

# Function to count data rows from API
def count_api_rows(data):
    return int(data)

# Function to count rows in SQL table for a specific company
def count_sql_rows(connection, sql_table, company_name):
    cursor = connection.cursor()
    sql_query = f"SELECT COUNT(*) FROM {sql_table} WHERE [Entity] = ?"
    cursor.execute(sql_query, (company_name,))
    result = cursor.fetchone()
    return result[0] if result else 0

# Function to get the maximum entry number for each entity
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

# Main function for checking and inserting mismatches
if __name__ == "__main__":
    print("Comparing and repairing BC_GLentries row counts between API and SQL/Staging...")
    connection = pyodbc.connect(connection_string)
    overall_status = "Success"
    total_mismatches = 0
    total_inserted_rows = 0

    start_time = _DEF.datetime.now()
    company_names = _DEF.get_company_names(connection)
    max_entry_nos = get_max_entry_no_per_entity(connection)

    for company_name in company_names:
        try:
            # Check mismatch in row counts
            api = f"{api_full}{company_name}"
            api_response = _DEF.make_api_request_count(api, _AUTH.client_id, _AUTH.client_secret, _AUTH.token_url)
            api_row_count = count_api_rows(api_response)
            sql_row_count = count_sql_rows(connection, sql_table, company_name)

            if api_row_count != sql_row_count:
                # Log the mismatch
                overall_status = "Error"
                total_mismatches += 1
                error_details = f"Mismatch in row count for {company_name}: API rows {api_row_count}, SQL rows {sql_row_count}"
                _DEF.log_status(connection, "Error", script_cat, script_name_check, start_time, _DEF.datetime.now(), int((_DEF.datetime.now() - start_time).total_seconds() / 60), abs(api_row_count - sql_row_count), error_details, company_name, "N/A")
                _DEF.send_email_mfa(f"ErrorLog -> {script_name_check} / {script_cat}", error_details, _AUTH.email_sender, _AUTH.email_recipient, _AUTH.guid_blink, _AUTH.email_client_id, _AUTH.email_client_secret)

                # Repair the mismatch by inserting missing entries
                max_entry_no = max_entry_nos.get(company_name, 0)
                api_insert = f"{api_full_insert}{company_name}&$filter=entryNo gt {max_entry_no}"
                api_data_generator = _DEF.make_api_request(api_insert, _AUTH.client_id, _AUTH.client_secret, _AUTH.token_url)
                data_to_insert = list(api_data_generator)
                row_count = len(data_to_insert)

                if row_count > 0:
                    _DEF.insert_data_into_sql(connection, data_to_insert, sql_table, company_name, columns_insert)
                    inserted_rows = _DEF.count_rows(data_to_insert)
                    total_inserted_rows += inserted_rows

                    if inserted_rows != row_count:
                        overall_status = "Error"
                        error_details = f"Expected to insert {row_count} rows, but only {inserted_rows} were inserted."
                        _DEF.log_status(connection, "Error", script_cat, script_name_insert, start_time, _DEF.datetime.now(), int((_DEF.datetime.now() - start_time).total_seconds() / 60), row_count - inserted_rows, error_details, company_name, api_insert)
                        _DEF.send_email_mfa(f"ErrorLog -> {script_name_insert} / {script_cat}", error_details, _AUTH.email_sender, _AUTH.email_recipient, _AUTH.guid_blink, _AUTH.email_client_id, _AUTH.email_client_secret)
            else:
                print(f"No mismatch found for {company_name}.")
        except Exception as e:
            overall_status = "Error"
            total_mismatches += 1
            error_details = f"An error occurred for {company_name}: {str(e)}"
            print(error_details)
            _DEF.log_status(connection, "Error", script_cat, script_name_check, start_time, _DEF.datetime.now(), int((_DEF.datetime.now() - start_time).total_seconds() / 60), 0, error_details, company_name, "N/A")
            _DEF.send_email_mfa(f"ErrorLog -> {script_name_check} / {script_cat}", error_details, _AUTH.email_sender, _AUTH.email_recipient, _AUTH.guid_blink, _AUTH.email_client_id, _AUTH.email_client_secret)

    # Final logging
    if overall_status == "Success":
        success_message = f"Total rows inserted: {total_inserted_rows}."
        _DEF.log_status(connection, "Success", script_cat, script_name_check, start_time, _DEF.datetime.now(), int((_DEF.datetime.now() - start_time).total_seconds() / 60), total_inserted_rows, success_message, "All", "N/A")
    else:
        error_summary = f"Total companies with mismatches or errors: {total_mismatches}."
        _DEF.log_status(connection, "Error", script_cat, script_name_check, start_time, _DEF.datetime.now(), int((_DEF.datetime.now() - start_time).total_seconds() / 60), "N/A", error_summary, "Multiple", "N/A")
