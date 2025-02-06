## -> Step 1: adjust paths ##

#Local path
path_script = "C:/Python/HV_PROJECTS"

#server_path
#path_script = "C:/Python/ft_projects"

#No changes needed
import pyodbc
from email.mime.text import MIMEText
import sys
sys.path.append(path_script)
import _AUTH
import _DEF 


## -> Step 2: Adjust script variables ##

# Variables for logging
script_name = "BC_DetCustomerLedger"
script_cat = "DWH_extract"


# Variables for the destination table
sql_table = "dbo.BC_DetCustomerLedger"
entryno = "Entry_No"

columns_insert = [
    "[@odata.etag]" 
    , "Entry_No"      
      ,"Posting_Date"
      ,"Entry_Type"
      ,"Document_Type"
      ,"Document_No"
      ,"Customer_No"
      ,"Initial_Entry_Global_Dim_1"
      ,"Initial_Entry_Global_Dim_2"
      ,"Posting_Group"
      ,"Currency_Code"
      ,"Amount"
      ,"Amount_LCY"
      ,"Debit_Amount"
      ,"Debit_Amount_LCY"
      ,"Credit_Amount"
      ,"Credit_Amount_LCY"
      ,"Initial_Entry_Due_Date"
      ,"User_ID"
      ,"Source_Code"
      ,"Reason_Code"
      ,"Unapplied"
      ,"Unapplied_by_Entry_No"
      ,"Cust_Ledger_Entry_No"
      ,"Entity"

]

# Variables for API request
api_table = "')/GedetKlantPosten"
api_full = _AUTH.end_Odata_BC + "/" + "Company('"

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
 
# No changes needed 
if __name__ == "__main__":
    print(f"Copying {script_name} to SQL/Staging ...")
    connection = pyodbc.connect(_AUTH.connection_string)
    threshold = 0

    start_time = _DEF.datetime.now()
    overall_status = "Success"
    total_inserted_rows = 0

    try:
        company_names = _DEF.get_company_names(connection)
        max_entry_nos = get_max_entry_no_per_entity(connection)
    
        for company_name in company_names:
            # Get the maximum Entry_No for the current company (entity)
            max_entry_no = max_entry_nos.get(company_name, 0)  # Default to 0 if not found

            # Filter API data to only retrieve records with Entry_No greater than the max_entry_no
            api = f"{api_full}{company_name}{api_table}?$filter={entryno} gt {max_entry_no}"
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