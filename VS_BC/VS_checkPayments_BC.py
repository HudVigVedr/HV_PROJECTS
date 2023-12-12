import pandas as pd
import pyodbc
import xml.etree.ElementTree as ET  # For XML creation
import datetime
import requests
from io import StringIO
from decimal import Decimal

import sys
sys.path.append('C:/Python/HV_PROJECTS')
import _AUTH
import _DEF 

script_name = "Check invoice status in BC"
script_cat = "VS_INT"

open_table = "Interface-OpenInvoices-new"
open_invoices = _AUTH.end_veson + open_table + _AUTH.vs_token


fetch_base = _AUTH.end_REST_BOLTRICS_BC + "/"
fetch_company = "?company="
fetch_filter = "&$filter=postingDescription eq '*"
fetch_select = "*'&$select=id,no,systemModifiedAt,postingDate,postingDescription,currencyCode,currencyFactor,closed,amountIncludingVAT"

endpoint_xml = _AUTH.vs_quee + _AUTH.vs_token

# SQL Server connection settings
sql_connection_string = f"DRIVER=ODBC Driver 17 for SQL Server;SERVER={_AUTH.server};DATABASE={_AUTH.database};UID={_AUTH.username};PWD={_AUTH.password}"


def get_invoices(api_url):
    try:
        response = requests.get(api_url)
        response.raise_for_status()  # This will raise an exception for HTTP errors
        return response.text  # Assuming the API returns CSV as plain text
    except requests.RequestException as e:
        print(f"Error fetching invoices: {e}")
        return None


def create_xml_data(invoiceTransNo, entryDate, actDate, externalRefId, payMode, currencyAmount, currency, bankCode, companyCode, baseCurrencyAmount, bankXCRate):
    root = ET.Element("simplePayment")

    ET.SubElement(root, "invoiceTransNo").text = invoiceTransNo
    ET.SubElement(root, "entryDate").text = entryDate
    ET.SubElement(root, "actDate").text = actDate
    ET.SubElement(root, "externalRefId").text = externalRefId
    ET.SubElement(root, "payMode").text = payMode
    ET.SubElement(root, "currencyAmount").text = str(currencyAmount)
    ET.SubElement(root, "currency").text = currency
    ET.SubElement(root, "baseCurrencyAmount").text = baseCurrencyAmount
    ET.SubElement(root, "bankXCRate").text = bankXCRate # Assuming empty for now
    ET.SubElement(root, "bankCode").text = bankCode
    ET.SubElement(root, "companyCode").text = companyCode

    xml_data = ET.tostring(root, encoding='utf-8', method='xml').decode('utf-8')
    return xml_data


def fetch_invoice_details_if_open(api_full, client_id, client_secret, token_url):
    try:
        # Fetch invoice details
        invoice_details = next(_DEF.make_api_request(api_full, client_id, client_secret, token_url), None)

        # Check if invoice details are available and if the invoice is open
        if not invoice_details.get('closed', True):
            return None  # Invoice is either not found or closed

        # Extract posting description if available
        posting_description = None
        if 'postingDescription' in invoice_details:
            full_description = invoice_details['postingDescription']
            if full_description:
                parts = full_description.split('-')
                posting_description = parts[-1] 

        # Combine all required details
        invoice_info = {
            "id": invoice_details.get('id'),
            "systemModifiedAt": invoice_details.get('systemModifiedAt'),
            "no": invoice_details.get('no'),
            "postingDate": invoice_details.get('postingDate'),
            "currencyCode": invoice_details.get('currencyCode'),
            "currencyFactor": invoice_details.get('currencyFactor'),
            "amountIncludingVAT": invoice_details.get('amountIncludingVAT'),
            "postingDescription": posting_description
        }


        return invoice_info
    except Exception as e:
        print(f"Error fetching details for invoice {invoice_no}: {e}")
        return None

def write_xml_to_file(xml_data, file_path):
    try:
        with open(file_path, 'a') as file:  # 'a' is for append mode
            file.write(xml_data + "\n\n")  # Write XML data with a newline for separation
    except Exception as e:
        print(f"Error writing XML to file: {e}")


def send_xml_data(api_endpoint, xml_data):
    headers = {'Content-Type': 'application/xml'}
    try:
        response = requests.post(api_endpoint, data=xml_data, headers=headers)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        print(f"Error sending XML data: {e}")
        return None


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