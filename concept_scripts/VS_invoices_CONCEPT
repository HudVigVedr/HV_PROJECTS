import pandas as pd
import pyodbc
import xml.etree.ElementTree as ET  # For XML creation
import datetime
import requests
from io import StringIO
from decimal import Decimal
from azure.servicebus import ServiceBusClient, ServiceBusMessage

import sys
sys.path.append('C:/Python/HV_PROJECTS')
import _AUTH
import _DEF 

script_name = "Import invoices from VS to BC"
script_cat = "TEST"


api_base = _AUTH.end_REST_MS_BC + "/"
table_pur = "purchaseInvoices"
table_purl = "purchaseInvoiceLines"
table_sls = "salesInvoices"
table_slsl = "salesInvoiceLines"

api_company = "?company="
api_filter = "&$"

# SQL Server connection settings
sql_connection_string = f"DRIVER=ODBC Driver 17 for SQL Server;SERVER={_AUTH.server};DATABASE={_AUTH.database};UID={_AUTH.username};PWD={_AUTH.password}"

#servicebus_var
connection_str = "YOUR_CONNECTION_STRING"
queue_name = "YOUR_QUEUE_NAME"



def receive_xml_messages(connection_str, queue_name):
    # Create a ServiceBusClient
    servicebus_client = ServiceBusClient.from_connection_string(connection_str)

    # Create a receiver for the queue
    receiver = servicebus_client.get_queue_receiver(queue_name=queue_name)

    try:
        # Start receiving messages
        with receiver:
            for message in receiver:
                try:
                    # Assuming the message body is XML
                    xml_message = message.body.decode('utf-8')
                    print("Received XML message:")
                    print(xml_message)
                    
                    # Process the XML message here as needed
                    
                    # Complete the message to remove it from the queue
                    receiver.complete_message(message)
                    
                except Exception as e:
                    print("Error processing message:", str(e))
                    # If there was an error processing the message, you can handle it here
                    # For example, you could log the error or move the message to a dead-letter queue
                    receiver.abandon_message(message)
    except Exception as e:
        print("Error receiving messages:", str(e))
    finally:
        # Close the ServiceBusClient
        servicebus_client.close()




# Main execution
if __name__ == "__main__":
    print("Checking Veson invoice(s) status in BC...")
    connection = pyodbc.connect(sql_connection_string)

    start_time = _DEF.datetime.now()
    overall_status = "Success"
    processed_files_count = 0  # Initialize the counter


    try:
        # Get the list of invoices
        invoices_csv = get_invoices(open_invoices)
        if invoices_csv:
            invoices_df = pd.read_csv(StringIO(invoices_csv))  # Convert CSV text to DataFrame

            for _, invoice in invoices_df.iterrows():
                exact_trans_no = invoice['ExactTransNo']
                invoice_no = str(invoice['Invoice No'])
                currency = invoice['Curr']
                org_amount = invoice['Amount Curr']
                
                # Determine the endpoint type for status check
                status_endpoint_company = "Hartel" if "VLIE" in exact_trans_no else "Chartering"
                invoice_endpoint_table = "salesInvoiceHeaders" if invoice_no.startswith("1") else "purchaseInvoiceHeaders"
            

                company_code = "VLIET" if "VLIE" in exact_trans_no else "HNV BV"
                
                if currency == 'EUR':
                        ledger_code = 19200
                elif currency == 'USD':
                        ledger_code = 19400
                elif currency == 'DKK':
                        ledger_code = 19500
                elif currency == 'NOK':
                        ledger_code = 19600
                elif currency == 'SEK':
                        ledger_code = 19700
                elif currency == 'GBP':
                        ledger_code = 19300   
                else:
                        ledger_code = None # or some default value


                # Fetch invoice details if open
                api_url_fetch = fetch_base + invoice_endpoint_table + fetch_company + status_endpoint_company + fetch_filter + invoice_no + fetch_select

                invoice_details = fetch_invoice_details_if_open(api_url_fetch, _AUTH.client_id, _AUTH.client_secret, _AUTH.token_url)

          
                if invoice_details:
                    # Increase the count of processed files
                    processed_files_count += 1

                    if currency == 'EUR':
                        currency_factor = 1
                    else:
                        currency_factor = invoice_details['currencyFactor']

                    # New conditional statement to determine the amount
                    amount_including_vat = invoice_details['amountIncludingVAT']
                    difference = abs(org_amount - amount_including_vat)
                    
                    if -0.01999 <= difference <= 0.019999:
                        selected_amount = org_amount
                    else:
                        selected_amount = amount_including_vat

                    if currency == 'EUR':
                        currency_amount = selected_amount
                    else:
                        currency_amount = round(selected_amount / currency_factor, 2)

                    selected_amount_str = "{:.2f}".format(selected_amount)
                    currency_amount_str = "{:.2f}".format(currency_amount)

                    act_date = invoice_details['systemModifiedAt']
                    formatted_date = act_date.split('T')[0] if act_date else None

                    if "VLIE" in exact_trans_no:
                         company_code_xml = "VLIET"
                    elif "HNV" in exact_trans_no:
                         company_code_xml = "HNV BV"  
                    elif "COA" in exact_trans_no:
                         company_code_xml = "COASTA"
                    else:
                        company_code_xml = None 

                    # Prepare your XML data here
                    xml_data = create_xml_data(
                        invoiceTransNo=invoice_details['postingDescription'],
                        entryDate=invoice_details['postingDate'],
                        actDate=formatted_date,
                        externalRefId=invoice_details['id'],
                        payMode="WT",
                        currencyAmount=selected_amount_str,
                        currency=currency,
                        baseCurrencyAmount=currency_amount_str, 
                        bankXCRate=str(currency_factor),  
                        bankCode=str(ledger_code),
                        companyCode=company_code_xml
                    )
                    #print(xml_data)
                    send_xml_data(endpoint_xml, xml_data)
                    #print(invoice_no)
                    #xml_file_path = "test_xml_data.txt"  
                    #write_xml_to_file(xml_data, xml_file_path)

    except Exception as e:
        overall_status = "Error"
        error_details = str(e)
        print(f"An error occurred: {e}")
        _DEF.log_status(connection, "Error", script_cat, script_name, start_time, _DEF.datetime.now(), int((_DEF.datetime.now() - start_time).total_seconds() / 60), processed_files_count, error_details, "None", "N/A")

    finally:
        if overall_status == "Success":
            success_message = f"Total invoice(s) processed: {processed_files_count}."
            _DEF.log_status(connection, "Success", script_cat, script_name, start_time, _DEF.datetime.now(), int((_DEF.datetime.now() - start_time).total_seconds() / 60), processed_files_count, success_message, "All", "N/A")

        connection.close()