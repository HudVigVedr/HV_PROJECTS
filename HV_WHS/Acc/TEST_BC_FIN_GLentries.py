import requests
import pyodbc
import json
import _AUTH
import _DEF as _DEF
import smtplib
from email.mime.text import MIMEText
import time

# SQL Server connection settings
connection_string = f"DRIVER=ODBC Driver 17 for SQL Server;SERVER={_AUTH.server};DATABASE={_AUTH.database};UID={_AUTH.username};PWD={_AUTH.password}"
#connection_string2 = f"DRIVER=ODBC Driver 17 for SQL Server;SERVER=HV-db;DATABASE=Staging;UID=hheij;PWD=ByMus&060R6f"
sql_table = "dbo.BC_GLE_TEST3"

# API endpoint URL (same as before) -> aanvullen
api_url = _AUTH.end_REST_BC
api_table = "generalLedgerEntries"
api_full = api_url + "/" + api_table + "?company="

# delete function SQL table
def delete_sql_table(connection):
    cursor = connection.cursor()
    cursor.execute(f"DELETE FROM {sql_table}")
    connection.commit()

def validate_numeric_value(value, max_size):
    try:
        if abs(value) > max_size:
            # Log an error or handle the large value as needed
            print(f"Value {value} is too large. Maximum allowed is {max_size}.")
            # You can truncate the value, set it to a default value, or skip the record
            value = max_size  # Truncate to the maximum allowed value
    except TypeError:
        # Handle non-numeric values as needed
        pass
    return value

# Function to insert data into SQL Server
def insert_data_into_sql(connection, data, sql_table, company_name):
    cursor = connection.cursor()

    sql_insert = f"""
        INSERT INTO {sql_table} (
            [odata_etag],
            [id],
            [system_created_at],
            [system_created_by],
            [system_modified_at],
            [system_modified_by],
            [entry_no],
            [gl_account_no],
            [posting_date],
            [document_type],
            [document_no],
            [description],
            [bal_account_no],
            [amount],
            [global_dimension1_code],
            [global_dimension2_code],
            [user_id],
            [source_code],
            [system_created_entry],
            [prior_year_entry],
            [job_no],
            [quantity],
            [vat_amount],
            [business_unit_code],
            [journal_batch_name],
            [reason_code],
            [gen_posting_type],
            [gen_bus_posting_group],
            [gen_prod_posting_group],
            [bal_account_type],
            [transaction_no],
            [debit_amount],
            [credit_amount],
            [document_date],
            [external_document_no],
            [source_type],
            [source_no],
            [no_series],
            [tax_area_code],
            [tax_liable],
            [tax_group_code],
            [use_tax],
            [vat_bus_posting_group],
            [vat_prod_posting_group],
            [additional_currency_amount],
            [add_currency_debit_amount],
            [add_currency_credit_amount],
            [close_income_statement_dim_id],
            [ic_partner_code],
            [reversed],
            [reversed_by_entry_no],
            [reversed_entry_no],
            [gl_account_name],
            [journal_templ_name],
            [dimension_set_id],
            [shortcut_dimension3_code],
            [shortcut_dimension4_code],
            [shortcut_dimension5_code],
            [shortcut_dimension6_code],
            [shortcut_dimension7_code],
            [shortcut_dimension8_code],
            [last_dim_correction_entry_no],
            [last_dim_correction_node],
            [dimension_changes_count],
            [prod_order_no],
            [fa_entry_type],
            [fa_entry_no],
            [comment],
            [account_id],
            [last_modified_date_time],
            [document_line_no3pl],
            [wms_document_type],
            [wms_document_no],
            [wms_document_line_no],
            [tms_document_type],
            [tms_document_no],
            [tms_document_sequence_no],
            [tms_document_line_no],
            [ultimo],
            [Entity]  
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    sql_check_exists = f"""
        SELECT 1 FROM {sql_table}
        WHERE [id] = ? AND [Entity] = ?
    """

    for item in data:
        # Validate numeric values before inserting
        numeric_column_names = ['amount', 'debit_amount', 'credit_amount', 'additional_currency_amount', 'add_currency_debit_amount', 'add_currency_credit_amount']  # List of numeric columns to validate
        max_numeric_size = 1e10  # Maximum allowed value for numeric columns
        for col in numeric_column_names:
            if col in item:
                item[col] = validate_numeric_value(item[col], max_numeric_size)
        
        values = list(item.values())
        entity_id = item.get('id')
        if entity_id is not None:
            cursor.execute(sql_check_exists, (entity_id, company_name))
            if cursor.fetchone() is None:
                values.append(company_name)  # add company name to the list of values
                cursor.execute(sql_insert, tuple(values))

    connection.commit()

if __name__ == "__main__":

    start_time = time.time()  # Record start time
    rows_inserted = 0  # Initialize counter for rows inserted
    successes = []  # List to hold successful company names
    failures = []  # List to hold failed company names
    rows_inserted_per_iteration = {}  # Dictionary to hold rows inserted per iteration

    try:
        # Establish the SQL Server connection
        #connection1 = pyodbc.connect(connection_string2)
        connection = pyodbc.connect(connection_string)

        # Get a list of company names from SQL Server
        company_names = _DEF.get_company_names(connection)

        #delete_sql_table(connection)

        for company_name in company_names:
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
        
        connection.close()
        end_time = time.time()  # Record end time
        duration = (end_time - start_time )/60 # Calculate duration

        # Print results
        print(f"Total rows inserted: {rows_inserted}")
        print(f"Total time taken: {duration} minutes")

        
        # Prepare email content
        email_body = f"The script completed in {duration} seconds with {rows_inserted} total rows inserted.\n\n"
        if successes:
            email_body += "Successes:\n" + "\n".join(successes) + "\n\n"
        if failures:
            email_body += "Failures:\n" + "\n".join(f"{company}: {error}" for company, error in failures)

        # Include rows inserted per iteration in email
        for company, rows in rows_inserted_per_iteration.items():
            email_body += f"Rows inserted for {company}: {rows}\n"

        # Send email
        _DEF.send_email(
            'Script Summary',
            email_body,
            _AUTH.email_recipient,
            _AUTH.email_sender,
            _AUTH.smtp_server,
            _AUTH.smtp_port,
            _AUTH.email_username,
            _AUTH.email_password
        )
