## -> Step 1: adjust paths ##
# Local path
path_script = "C:/Python/HV_PROJECTS"
#server_path = "C:/Python/ft_projects"

## -> No changes needed for these imports ##
import pyodbc
from email.mime.text import MIMEText
import datetime
import sys
sys.path.append(path_script)
import _AUTH
import _DEF

## -> Step 2: Adjust script variables ##
# Variables for logging
script_name = "BC_GLbudget_Bulk"
script_cat = "DWH_extract"

# Variables for the destination table and columns
sql_table = "BC_GLbudget"
staging_table = "_staging_" + sql_table 
columns_insert = [
    "[odata.etag]"
      ,"Entry_No"
      ,"Budget_Name"
      ,"[Date]"
      ,"G_L_Account_No"
      ,"Description"
      ,"Global_Dimension_1_Code"
      ,"Global_Dimension_2_Code"
      ,"Budget_Dimension_1_Code"
      ,"Budget_Dimension_2_Code"
      ,"Budget_Dimension_3_Code"
      ,"Budget_Dimension_4_Code"
      ,"Business_Unit_Code"
      ,"Amount"
      ,"Dimension_Set_ID"
      ,"Entity"
]

# Variables for API request

api_table = "Grootboekbudgetposten"
api_full = _AUTH.end_Odata_BC + "/Company('"
api_full2 = "')/" 

###############################################################################
# Helper functions for staging/bulk operations
###############################################################################

def truncate_table(connection, table_name):
    """Clears out all records from the specified table."""
    cursor = connection.cursor()
    cursor.execute(f"TRUNCATE TABLE {table_name}")
    connection.commit()

def bulk_insert_staging(connection, data, staging_table, company_name, columns):
    if not data:
        return
    placeholders = ", ".join(["?"] * len(columns))
    columns_sql = ', '.join(columns)
    insert_sql = f"INSERT INTO {staging_table} ({columns_sql}) VALUES ({placeholders})"

    values = []
    for item in data:
        row = []
        for col in columns:
            if col == "Entity":
                row.append(company_name)
            elif col == "[Date]" or col == "Date":
                date_value = item.get("Date", None)
                if date_value:
                    try:
                        row.append(datetime.datetime.strptime(date_value, "%Y-%m-%d").date())
                    except Exception:
                        row.append(None)
                else:
                    row.append(None)
            else:
                row.append(item.get(col, None))
        values.append(row)
    
    cursor = connection.cursor()
    cursor.executemany(insert_sql, values)
    connection.commit()

def insert_staging_to_target(connection, staging_table, target_table, columns):
    """
    Performs a set-based INSERT from the staging table into the target table,
    selecting all columns in the defined order.
    """
    columns_sql = ", ".join(columns)

    sql_insert = f"INSERT INTO {target_table} ({columns_sql}) SELECT {columns_sql} FROM {staging_table}"
    cursor = connection.cursor()
    cursor.execute(sql_insert)
    connection.commit()

###############################################################################
# Main Script
###############################################################################
if __name__ == "__main__":
    print(f"Bulk copying {script_name} to SQL/Staging ...")
    connection = pyodbc.connect(_AUTH.connection_string)
    threshold = 0  # Minimum row count threshold
    start_time = _DEF.datetime.now()
    overall_status = "Success"
    total_inserted_rows = 0

    try:
        # Truncate staging and target tables for a full refresh.
        truncate_table(connection, staging_table)
        truncate_table(connection, sql_table)
        
        # Get the list of companies from your helper function.
        company_names = _DEF.get_company_names(connection)
        for company_name in company_names:
            api = f"{api_full}{company_name}{api_full2}{api_table}"


            api_data_generator = _DEF.make_api_request(api, _AUTH.client_id, _AUTH.client_secret, _AUTH.token_url)
            data_to_insert = list(api_data_generator)
            row_count = len(data_to_insert)
            
            if row_count > threshold:

                bulk_insert_staging(connection, data_to_insert, staging_table, company_name, columns_insert)
                total_inserted_rows += row_count
                print(f"Inserted {row_count} rows for company {company_name} into staging.")
        
        # Move all data from staging into the target table in one set-based operation.
        insert_staging_to_target(connection, staging_table, sql_table, columns_insert)
    
    except Exception as e:
        overall_status = "Error"
        error_details = str(e)
        print(f"An error occurred: {e}")
        _DEF.log_status(connection, "Error", script_cat, script_name, start_time, _DEF.datetime.now(),
                        int((_DEF.datetime.now() - start_time).total_seconds() / 60), 0, error_details, "None", "N/A")
        _DEF.send_email_mfa(f"ErrorLog -> {script_name} / {script_cat}",
                            error_details, _AUTH.email_sender, _AUTH.email_recipient,
                            _AUTH.guid_blink, _AUTH.email_client_id, _AUTH.email_client_secret)
    
    finally:
        if overall_status == "Success":
            success_message = f"Total rows inserted: {total_inserted_rows}."
            _DEF.log_status(connection, "Success", script_cat, script_name, start_time, _DEF.datetime.now(),
                            int((_DEF.datetime.now() - start_time).total_seconds() / 60), total_inserted_rows, success_message, "All", "N/A")
        connection.close()
