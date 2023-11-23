import pandas as pd
import pyodbc
import xml.etree.ElementTree as ET  # For XML creation
import datetime

import sys
sys.path.append('C:/Python/HV_PROJECTS')
import _AUTH
import _DEF 

script_name = "GS files to BC"
script_cat = "GS_INT"

# SQL Server connection settings
sql_connection_string = f"DRIVER=ODBC Driver 17 for SQL Server;SERVER={_AUTH.server};DATABASE={_AUTH.database};UID={_AUTH.username};PWD={_AUTH.password}"

#csv_file_path = "C:/Users/ThomLems/OneDrive - Blink Solutions BV/Back-ups/Bureaublad/ovet_portcalls.csv" #test LC
csv_file_path = "J:/Edi/ovet_portcalls.csv" #server

#insert data
URL1 = _AUTH.BC_base
URL2 = "PROD-123/WS/"
URL3 = "/Codeunit/DIPost"

#check data
api_url = _AUTH.end_REST_BOLTRICS_BC 
api_table = "/wmsDocumentHeaders?"
api_filter = "&$filter=attribute06 eq '"
api_File = ""
api_company = "Company="


target_companies = ["Maripro BV", "Maripro Belgium BV"]

def determine_endpoint(company_name):
    if company_name == "Maripro BV":
        return URL1 + URL2 + "Maripro" + URL3
    elif company_name == "Maripro Belgium BV":
        return URL1 + URL2 + "Maripro%20Belgium" + URL3
    elif company_name in ["FSA!"]:
        return URL1 + URL2 + ["Fairway"] + URL3
    elif company_name in ["OVT!" ]:
        return URL1 + URL2 + "OVET" + URL3
    elif company_name in ["BA!"]:
        return URL1 + URL2 + "BMA" + URL3
    else:
        return None
    
def determine_endpoint_API(company_name):
    if company_name == "Maripro BV":
        return api_url + api_table + api_company + "Maripro" + api_filter
    elif company_name == "Maripro Belgium BV":
        return api_url + api_table +  api_company + "Maripro%20Belgium" + api_filter 
    elif company_name in ["FSA"]:
        return api_url + api_table + api_company + "Fairway" + api_filter 
    elif company_name in ["OVT" ]:
        return api_url + api_table + api_company + "OVET" + api_filter 
    elif company_name in ["BA"]:
        return api_url + api_table +  api_company + "BMA" + api_filter
    else:
        return None

# "Fairway", "Fairway Shipping Agencies Amsterdam", "Fairway Shipping Agencies Rotterdam", "Fairway Shipping Agencies Belgium"
# "OVET", "Ovet Shipping BV"
#  "BMA", "Bulk Maritime Agencies Amsterdam"

# Function to read CSV
def read_csv(file_path):
    try:
        df = pd.read_csv(file_path, sep=';')
        return df
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        return None

# Function to create XML data
def create_xml_data(row_data):
    Timestamp = datetime.datetime.now()
    DateStamp = Timestamp.strftime("%Y-%m-%d")
    Agency = row_data["Agency"]
    Portcall = row_data["PortCallNumber"]
    Vessel = row_data["VesselName"]
    Voy = row_data["VoyageNumber"]

    ETAstring = str(row_data["ETA"]) if pd.notna(row_data["ETA"]) else None
    try:
        ETA = datetime.datetime.strptime(ETAstring, "%Y-%m-%d %H:%M") if ETAstring else None
    except ValueError:
        ETA = None  # Or handle the exception as needed
    ETAform = ETA.strftime("%Y-%m-%d") if ETA else ""

    ETDstring = str(row_data["ETD"]) if pd.notna(row_data["ETD"]) else None
    try:
        ETD = datetime.datetime.strptime(ETDstring, "%Y-%m-%d %H:%M") if ETDstring else None
    except ValueError:
        ETD = None  # Or handle the exception as needed
    ETDform = ETD.strftime("%Y-%m-%d") if ETD else ""

    PortFrom = row_data["LastPort"]
    PortTo = row_data["NextPort"]
    Created = row_data["Created"]

    CdataContent = f"""
<ns0:Message xmlns:ns0="www.boltrics.nl/receiveoceanfreightexport:v1.00">
    <ns0:Header>
        <ns0:MessageID>{Portcall}</ns0:MessageID>
        <ns0:CreationDateTime>{DateStamp}</ns0:CreationDateTime>
        <ns0:ProcesAction>INSERT</ns0:ProcesAction>
        <ns0:FromTradingPartner>KMA00038</ns0:FromTradingPartner>
        <ns0:ToTradingPartner>{Agency}</ns0:ToTradingPartner>
    </ns0:Header>
    <ns0:Documents>
        <ns0:Document>
            <ns0:StatusCode>15-EDI</ns0:StatusCode>
            <ns0:Customer>
                <ns0:No>KMA00038</ns0:No>
            </ns0:Customer>
            <ns0:PostingDate>{ETDform}</ns0:PostingDate>
            <ns0:ExternalDocumentNo>{Portcall}</ns0:ExternalDocumentNo>
            <ns0:DeliveryDate>{ETDform}</ns0:DeliveryDate>
            <ns0:EstimatedDepartureDate>{ETDform}</ns0:EstimatedDepartureDate>
            <ns0:OrderTypeCode>AGENCIES</ns0:OrderTypeCode>
            <ns0:PortFrom>{PortFrom}</ns0:PortFrom>
            <ns0:PortTo>{PortTo}</ns0:PortTo>
            <ns0:VesselNo>{Vessel}</ns0:VesselNo>
            <ns0:VoyageNo>{Voy}</ns0:VoyageNo>
            <ns0:Attribute06>{Portcall}</ns0:Attribute06>
        </ns0:Document>
    </ns0:Documents>
</ns0:Message>"""
    return CdataContent

# Function to send XML data
def send_xml_data(api_endpoint, xml_data):
    headers = {'Content-Type': 'application/xml', 'SOAPaction': 'urn:microsoft-dynamics-schemas/codeunit/DIPost'}  # Set appropriate headers
    return  _DEF.make_api_request_XML(api_endpoint, _AUTH.client_id, _AUTH.client_secret, _AUTH.token_url, xml_data, headers)


# Main execution
if __name__ == "__main__":
    print("Sending new files from Gatship to Dynamics...")
    connection = pyodbc.connect(sql_connection_string)

    start_time = _DEF.datetime.now()
    overall_status = "Success"
    processed_files_count = 0  # Initialize the counter

    try:
        df = read_csv(csv_file_path)
        if df is not None:

            df_filtered = df[df['Agency'].isin(target_companies)]

            for _, row in df_filtered.iterrows():
                company_name = row['Agency']
                endpoint = determine_endpoint_API(company_name)
                file_name = row['PortCallNumber']
                api_check_url = f"{endpoint}{file_name}'"
                response_generator = _DEF.make_api_request(api_check_url, _AUTH.client_id, _AUTH.client_secret, _AUTH.token_url)

                file_exists = False  # Default assumption
                for response_entry in response_generator:
                    if response_entry and response_entry.get('value') != []:
                        file_exists = True
                        

                if not file_exists:
                    xml_data = create_xml_data(row)
                    soap_message = _DEF.create_soap_message(xml_data)
                    api_endpoint = determine_endpoint(company_name)
                    send_xml_data(api_endpoint, soap_message)
                    processed_files_count += 1  # Increment the counter
                    print(f"XML sent for {file_name}")

        # Log the number of processed files
        print(f"Total files processed: {processed_files_count}")

    except Exception as e:
        overall_status = "Error"
        error_details = str(e)
        print(f"An error occurred: {e}")
        _DEF.log_status(connection, "Error", script_cat, script_name, start_time, _DEF.datetime.now(), int((_DEF.datetime.now() - start_time).total_seconds() / 60), processed_files_count, error_details, "None", "N/A")

    finally:
        if overall_status == "Success":
            success_message = f"Script executed successfully. Total files processed: {processed_files_count}."
            _DEF.log_status(connection, "Success", script_cat, script_name, start_time, _DEF.datetime.now(), int((_DEF.datetime.now() - start_time).total_seconds() / 60), processed_files_count, success_message, "All", "N/A")

        connection.close()