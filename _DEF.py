import requests
import pyodbc
import json
import smtplib
from email.mime.text import MIMEText
from datetime import datetime, timedelta
import csv
import io


#send mail
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



# Function to get a list of companies from SQL Server
def get_company_names(connection):
    cursor = connection.cursor()
    cursor.execute("SELECT name FROM dbo.companies")
    companies = cursor.fetchall()
    
    # Extract the 'name2' values and convert them to strings
    company_names = [str(row[0]) for row in companies]
    
    return company_names

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


def make_api_request_vs(api_base, params=None):
    if params is None:
        params = {}
    headers = {
        'Content-Type':'text/csv'
    }
    response = requests.get(api_base, headers=headers, params=params)
    try:
        response.raise_for_status()  # Check for HTTP error status
        reader = csv.DictReader(io.StringIO(response.text))
        data = list(reader)
        
        return data
    except requests.exceptions.HTTPError as e:
        print(f"HTTP error making API request: {e}")
    except Exception as e:
        print(f"Error making API request: {e}")



def get_yesterday_date():
    yesterday = datetime.now() - timedelta(days=1)
    return yesterday.strftime('%Y-%m-%d')

yesterday_date = get_yesterday_date()


def insert_data_into_sql(connection, data, table):
    cursor = connection.cursor()

    # Prepare the SQL insert statement
    column_names = data[0].keys()
    placeholders = ', '.join(['?'] * len(column_names))
    sql_insert = f"INSERT INTO {table} ({', '.join(column_names)}) VALUES ({placeholders})"