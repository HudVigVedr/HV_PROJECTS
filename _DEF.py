import requests
import pyodbc
import json
import smtplib
from email.mime.text import MIMEText
from datetime import datetime, timedelta
import csv
import io
import re


## Data functions ##

def delete_sql_table(connection, sql_table):
    cursor = connection.cursor()
    cursor.execute(f"DELETE FROM {sql_table}")
    connection.commit()


def generate_insert_sql(table_name, columns):
    placeholders = ', '.join(['?'] * len(columns))
    column_names = ', '.join(columns)
    sql_insert = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"
    return sql_insert


def insert_data_into_sql(connection, data, sql_table, company_name, columns):
    cursor = connection.cursor()

    sql_insert = generate_insert_sql(sql_table, columns)

    for item in data:
        values = list(item.values())
        values.append(company_name)  # add company name to the list of values
        cursor.execute(sql_insert, tuple(values))

    connection.commit()


def get_company_names(connection):
    cursor = connection.cursor()
    cursor.execute("SELECT name FROM dbo.companies")
    companies = cursor.fetchall()
    
    # Extract the 'name2' values and convert them to strings
    company_names = [str(row[0]) for row in companies]
    
    return company_names


## Email functions ##
def send_email(subject, body, to_address, from_address, smtp_server, smtp_port, smtp_username, smtp_password):
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = from_address
    msg['To'] = to_address

    try:
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()  # Start TLS
            server.login(smtp_username, smtp_password)
            server.sendmail(from_address, to_address, msg.as_string())
            print("Email sent successfully")
    except Exception as e:
        print(f"Failed to send email: {e}")


def send_email_mfa(subject, body, from_address, to_address, tenant_id, client_id, client_secret):
    token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
    token_data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
        "resource": "https://graph.microsoft.com",
    }
    token_response = requests.post(token_url, data=token_data)
    access_token = token_response.json().get("access_token")

    # Send an email using Microsoft Graph API
    email_url = f"https://graph.microsoft.com/v1.0/users/{from_address}/sendMail"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    email_data = {
        "message": {
            "subject": subject,
            "body": {
                "contentType": "Text",
                "content": body,
            },
            "toRecipients": [
                {
                    "emailAddress": {
                        "address": email,
                    }
                }
                for email in to_address
            ],
        },
        "saveToSentItems": "true",
    }
    response = requests.post(email_url, headers=headers, data=json.dumps(email_data))

    if response.status_code == 202:
        print("Email sent successfully!")
    else:
        print(f"Failed to send email. Status code: {response.status_code}, Response: {response.text}")


## API functions ##

# Function to get access token
def get_access_token(client_id, client_secret, token_url):
    try:
        data = {
            'grant_type': 'client_credentials',
            'client_id': client_id,
            'client_secret': client_secret,
            'scope': 'https://api.businesscentral.dynamics.com/.default'
        }
        response = requests.post(token_url, data=data)
        response.raise_for_status()  # Check for HTTP error status
        token_data = response.json()
        return token_data['access_token']
    except Exception as e:
        print(f"Error getting access token: {e}")
        return None

def make_api_request(api_base, client_id, client_secret, token_url, params=None):
    access_token = get_access_token(client_id, client_secret, token_url)
    if params is None:
        params = {}
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    next_link = api_base
    while next_link:
        response = requests.get(next_link, headers=headers, params=params)
        try:
            response.raise_for_status()  # Check for HTTP error status
            data = response.json()
            next_link = data.get('@odata.nextLink')
            entries = data.get('value', [])
            if entries:
                for entry in entries:
                    #print(f"Type of entry: {type(entry)}")  # Debugging print statement
                    yield entry  # Use yield to return entries one by one
            else:
                break  # No more entries, break the loop
        except requests.exceptions.HTTPError as e:
            if response.status_code == 401:
                print("Token expired. Requesting new token...")
                access_token = get_access_token(client_id, client_secret, token_url)
                headers['Authorization'] = f'Bearer {access_token}'
            else:
                print(f"HTTP error making API request for {next_link}: {e}")
                next_link = None
        except Exception as e:
            print(f"Error making API request for {next_link}: {e}")
            next_link = None


def make_api_request_count(api_base, client_id, client_secret, token_url, params=None):
    access_token = get_access_token(client_id, client_secret, token_url)
    if params is None:
        params = {}
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    response = requests.get(api_base, headers=headers, params=params)
    try:
        response.raise_for_status()
        # Decode response text assuming UTF-8 encoding and remove BOM if present
        response_text = response.content.decode('utf-8-sig').strip()
        # Use regular expression to extract digits
        count_str = re.search(r'\d+', response_text).group()
        return int(count_str)
    except requests.exceptions.HTTPError as e:
        if response.status_code == 401:
            print("Token expired. Requesting new token...")
            # Handle token expiration and retry logic if necessary
        else:
            print(f"HTTP error making API request: {e}")
            return None
    except Exception as e:
        print(f"Error making API request: {e}")
        return None



def make_api_request_XML(api_base, client_id, client_secret, token_url, xml_data, headers):
    access_token = get_access_token(client_id, client_secret, token_url)
    headers['Authorization'] = f'Bearer {access_token}'
    
    try:
        response = requests.post(api_base, headers=headers, data=xml_data)
        response.raise_for_status()
        return response  # Consider processing the response before returning
    except requests.exceptions.HTTPError as e:
        if response.status_code == 401:
            print("Token expired. Requesting new token...")
            access_token = get_access_token(client_id, client_secret, token_url)
            if not access_token:
                print("Failed to refresh token.")
                return None
            headers['Authorization'] = f'Bearer {access_token}'
            return requests.post(api_base, headers=headers, data=xml_data)
        else:
            print(f"HTTP error making API request: {e}")
            return None
    except Exception as e:
        print(f"Error making API request: {e}")
        return None


def make_api_request_vs(url):
    response = requests.get(url)
    response.raise_for_status()  # Will raise an error for bad status codes
    return response.text

def get_yesterday_date():
    yesterday = datetime.now() - timedelta(days=1)
    return yesterday.strftime('%Y-%m-%d')
yesterday_date = get_yesterday_date()

def create_soap_message(xml_data):
    # Define the SOAP envelope template with CDATA placeholder
    soap_template = """<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/"><s:Body><PostXml xmlns="urn:microsoft-dynamics-schemas/codeunit/DIPost"><xml><![CDATA[{}
    ]]></xml></PostXml></s:Body></s:Envelope>"""

    # Insert the XML data into the CDATA section of the SOAP template
    return soap_template.format(xml_data)



# Function to count data rows from API
def count_api_rows(data):
    return int(data)

#Logging functions ##
def count_rows(api_data_generator):
    """Count the number of rows in the API data generator"""
    return sum(1 for _ in api_data_generator)

def log_status(connection, status, Categorie, Name, start_time, end_time, time_run, records_inserted, error_details, company_name, URi):
    """Log the status (success or error) into the dbo.Log table"""
    cursor = connection.cursor()
    sql = "INSERT INTO dbo.Log (Status,  Categorie, Name, StartDateTime, EndDateTime, TimeRunInMinutes, RecordsInserted, error_details, company_name, URi) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    cursor.execute(sql, status, Categorie, Name, start_time, end_time, time_run, records_inserted, error_details, company_name, URi)
    connection.commit()

