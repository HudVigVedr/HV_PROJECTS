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

script_name = "BC_PurchaseInvoiceHeader_Bulk"
script_cat = "DWH_extract"
script_type = "Copying"

# SQL Server connection settings
connection_string = (
    f"DRIVER=ODBC Driver 17 for SQL Server;"
    f"SERVER={_AUTH.server};DATABASE={_AUTH.database};UID={_AUTH.username};PWD={_AUTH.password}"
)
target_table = "BC_PurchaseInvoiceHeader"
staging_table = "_staging_" + target_table # Staging table name

# API endpoint URL
api_url = _AUTH.end_REST_BOLTRICS_BC
api_table = "purchaseInvoiceHeaders"
api_full = (
    api_url
    + "/"
    + api_table
    + "?"
    + "$select=id,no,payToVendorNo,postingDate,preAssignedNo,userID&$filter=systemModifiedAt gt "
    + _DEF.yesterday_date
    + "T00:00:00Z&company="
)

def truncate_table(connection, table_name):
    """Clear out the specified table."""
    cursor = connection.cursor()
    cursor.execute(f"TRUNCATE TABLE {table_name}")
    connection.commit()

def bulk_insert_staging(connection, data, staging_table, company_name):
    """
    Bulk insert a list of records (dictionaries) into the staging table.
    For BC_PurchaseInvoiceHeader, we use the following columns:
    [OdataEtag], [id], [no], [payToVendorNo], [postingDate], [preAssignedNo], [userID], [Entity]
    """
    if not data:
        return

    # Define the target column order explicitly.
    columns = ["OdataEtag", "id", "no", "payToVendorNo", "postingDate", "preAssignedNo", "userID", "Entity"]
    placeholders = ", ".join(["?"] * len(columns))
    columns_sql = ", ".join([f"[{col}]" for col in columns])
    insert_sql = f"INSERT INTO {staging_table} ({columns_sql}) VALUES ({placeholders})"
    
    # Build the values for each record.
    # Note: If the API data doesn't include 'OdataEtag', we set it to None.
    values = []
    for item in data:
        row = [
            item.get("OdataEtag", None),
            item.get("id", None),
            item.get("no", None),
            item.get("payToVendorNo", None),
            item.get("postingDate", None),
            item.get("preAssignedNo", None),
            item.get("userID", None),
            company_name
        ]
        values.append(row)
    
    cursor = connection.cursor()
    cursor.executemany(insert_sql, values)
    connection.commit()

def delete_existing_rows(connection, staging_table, target_table):
    """
    Delete rows from the target table where a matching record exists in the staging table.
    Matching is based on [no] and [Entity].
    """
    sql_delete = f"""
    DELETE T
    FROM {target_table} T
    INNER JOIN {staging_table} S
        ON T.[no] = S.[no]
        AND T.[Entity] = S.[Entity]
    """
    cursor = connection.cursor()
    cursor.execute(sql_delete)
    connection.commit()

def insert_staging_to_target(connection, staging_table, target_table):
    """
    Insert all rows from the staging table into the target table.
    """
    sql_insert = f"""
    INSERT INTO {target_table} ([OdataEtag], [id], [no], [payToVendorNo], [postingDate], [preAssignedNo], [userID], [Entity])
    SELECT [OdataEtag], [id], [no], [payToVendorNo], [postingDate], [preAssignedNo], [userID], [Entity]
    FROM {staging_table}
    """
    cursor = connection.cursor()
    cursor.execute(sql_insert)
    connection.commit()

if __name__ == "__main__":
    print("Bulk loading BC_PurchaseInvoiceHeader data into staging and merging to target table...")
    connection = pyodbc.connect(connection_string)
    threshold = 0
    start_time = _DEF.datetime.now()
    overall_status = "Success"
    total_rows = 0

    try:
        # Clear the staging table at the beginning.
        truncate_table(connection, staging_table)
        
        # Get company names from your helper function.
        company_names = _DEF.get_company_names(connection)
        
        for company_name in company_names:
            api = f"{api_full}{company_name}"
            api_data_generator = _DEF.make_api_request(api, _AUTH.client_id, _AUTH.client_secret, _AUTH.token_url)
            data_to_insert = list(api_data_generator)
            row_count = len(data_to_insert)
            
            if row_count > threshold:
                total_rows += row_count
                bulk_insert_staging(connection, data_to_insert, staging_table, company_name)
                print(f"Bulk inserted {row_count} rows for company {company_name} into staging.")
        
        if total_rows > 0:
            # Delete matching rows in the target table based on staging data.
            delete_existing_rows(connection, staging_table, target_table)
            # Insert new records from staging into the target table.
            insert_staging_to_target(connection, staging_table, target_table)
            # Optionally, clear the staging table after processing.
            truncate_table(connection, staging_table)
            print(f"Processed a total of {total_rows} rows.")
        else:
            print("No data found to process.")
    
    except Exception as e:
        overall_status = "Error"
        error_details = str(e)
        print(f"An error occurred: {e}")
        _DEF.log_status(
            connection,
            "Error",
            script_cat,
            script_name,
            start_time,
            _DEF.datetime.now(),
            int((_DEF.datetime.now() - start_time).total_seconds() / 60),
            0,
            error_details,
            "None",
            "N/A"
        )
        _DEF.send_email_mfa(
            f"ErrorLog -> {script_name} / {script_cat}",
            error_details,
            _AUTH.email_sender,
            _AUTH.email_recipient,
            _AUTH.guid_blink,
            _AUTH.email_client_id,
            _AUTH.email_client_secret
        )
    
    finally:
        if overall_status == "Success":
            success_message = f"Total rows inserted/updated: {total_rows}."
            _DEF.log_status(
                connection,
                "Success",
                script_cat,
                script_name,
                start_time,
                _DEF.datetime.now(),
                int((_DEF.datetime.now() - start_time).total_seconds() / 60),
                total_rows,
                success_message,
                "All",
                "N/A"
            )
        connection.close()
