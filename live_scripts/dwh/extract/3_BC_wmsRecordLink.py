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

script_name = "BC_wmsRecordLink_Bulk"
script_cat = "DWH_extract"
script_type = "Copying"

# SQL Server connection settings
connection_string = (
    f"DRIVER=ODBC Driver 17 for SQL Server;"
    f"SERVER={_AUTH.server};DATABASE={_AUTH.database};UID={_AUTH.username};PWD={_AUTH.password}"
)
target_table = "BC_wmsRecordLink"
staging_table = target_table + "_staging_"  # staging table

# API endpoint URL
api_url = _AUTH.end_REST_BOLTRICS_BC
api_table = "wmsRecordLinks"
api_full = (
    api_url
    + "/"
    + api_table
    + "?"
    + "$filter=((tableNo eq 122) or (tableNo eq 0) or (tableNo eq 112))and (created gt "
    + _DEF.yesterday_date
    + "T00:00:00Z)&company="
)


def bulk_insert_staging(connection, data, staging_table, company_name):
    """Bulk insert a list of dictionaries into the staging table.
       Assumes each dictionary has the same keys and that the staging table
       has the same columns as the target table plus an 'Entity' column."""
    if not data:
        return
    # Determine columns from the first record and add the 'Entity' column.
    columns = list(data[0].keys()) + ["Entity"]
    placeholders = ", ".join(["?"] * len(columns))
    columns_sql = ", ".join([f"[{col}]" for col in columns])
    insert_sql = f"INSERT INTO {staging_table} ({columns_sql}) VALUES ({placeholders})"
    
    # Prepare the values for executemany
    values = []
    for item in data:
        row = [item[key] for key in data[0].keys()] + [company_name]
        values.append(row)
    
    cursor = connection.cursor()
    cursor.executemany(insert_sql, values)
    connection.commit()

def merge_staging_to_target(connection, staging_table, target_table, columns):
    """Perform a single MERGE operation to update/insert rows from the staging table to the target table.
       The join is made on [id] and [Entity]."""
    # Exclude the key columns from the UPDATE set.
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
    """Clear out the staging table."""
    cursor = connection.cursor()
    cursor.execute(f"TRUNCATE TABLE {staging_table}")
    connection.commit()

if __name__ == "__main__":
    print("Bulk loading BC_wmsRecordLink data into staging and merging to target table...")
    connection = pyodbc.connect(connection_string)
    threshold = 0

    start_time = _DEF.datetime.now()
    overall_status = "Success"
    total_rows = 0

    try:
        # Optional: Truncate staging table at the start to ensure it's empty.
        truncate_staging(connection, staging_table)
        
        company_names = _DEF.get_company_names2(connection)
        first_data = True
        columns = []  # Will hold the list of columns (including 'Entity') used in the staging table.
        
        # Loop through each company and bulk insert the API data into the staging table.
        for company_name in company_names:
            api = f"{api_full}{company_name}"
            api_data_generator = _DEF.make_api_request(api, _AUTH.client_id, _AUTH.client_secret, _AUTH.token_url)
            print(api)
            data_to_insert = list(api_data_generator)
            row_count = len(data_to_insert)
            
            if row_count > threshold:
                total_rows += row_count
                # Determine the column list once based on the first non-empty dataset.
                if first_data and data_to_insert:
                    columns = list(data_to_insert[0].keys()) + ["Entity"]
                    first_data = False
                bulk_insert_staging(connection, data_to_insert, staging_table, company_name)
        
        if total_rows > 0:
            # Perform a single MERGE from the staging table into the target table.
            merge_staging_to_target(connection, staging_table, target_table, columns)
            # Optionally, clear the staging table after merging.
            truncate_staging(connection, staging_table)
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
            success_message = f"Total rows processed: {total_rows}."
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
