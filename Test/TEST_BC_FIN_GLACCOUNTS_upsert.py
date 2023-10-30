import requests
import pyodbc
import json
import AUTH

# SQL Server connection settings
connection_string = f"DRIVER=ODBC Driver 17 for SQL Server;SERVER={AUTH.server};DATABASE={AUTH.database};UID={AUTH.username};PWD={AUTH.password}"
connection_string2 = f"DRIVER=ODBC Driver 17 for SQL Server;SERVER=HV-db;DATABASE=Staging;UID=hheij;PWD=ByMus&060R6f"
sql_table = "dbo.test"

# API endpoint URL (same as before) -> aanvullen
api_url = AUTH.end_REST_BC
api_table = "generalLedgerAccounts"
api_full = api_url + "/" + api_table + "?$select=id,systemCreatedAt,systemModifiedBy,no&company="

# Function to get a list of companies from SQL Server
def get_company_names(connection):
    cursor = connection.cursor()
    cursor.execute("SELECT name2 FROM dbo.companies")
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

# Function to make API request with OAuth2 token
def make_api_request(api_full, access_token):
    try:
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Content-Type': 'application/json'
        }
        api_full = api_full
        response = requests.get(api_full, headers=headers)
        response.raise_for_status()  # Check for HTTP error status
        return response.json()
    except Exception as e:
        print(f"Error making API request for {api_full}: {e}")
        return None


# Function to insert data into SQL Server
def insert_data_into_sql(connection, data):
        cursor = connection.cursor()

        # SQL INSERT statement with placeholders
        cursor.execute(f"SELECT * FROM {sql_table} WHERE no = ? AND id = ?", (item['no'], item['id']))
        
        # SQL UPDATE statement with placeholders
        sql_update = f"UPDATE {sql_table} SET id = ?, systemCreatedAt = ?, systemModifiedBy = ? WHERE no = ?"

        for item in data:

            # Check if row exists
            cursor.execute(f"SELECT * FROM {sql_table} WHERE no = ?", (item['no'],))
            row = cursor.fetchone()

            if row is None:
                    cursor.execute(sql_insert, (
                    item['id'], 
                    item['systemCreatedAt'], 
                    item['systemModifiedBy'],
                    item['no']
                ))
            else:
                cursor.execute(sql_update, (
                    item['id'], 
                    item['systemCreatedAt'], 
                    item['systemModifiedBy'],
                    item['no']
                ))

        connection.commit()

if __name__ == "__main__":
    try:
        # Establish the SQL Server connection
        connection1 = pyodbc.connect(connection_string2)
        connection = pyodbc.connect(connection_string)

        # Get a list of company names from SQL Server
        company_names = get_company_names(connection1)

        for company_name in company_names:
            api = f"{api_full}{company_name}"  # Construct the API URL
            access_token = get_access_token(AUTH.client_id, AUTH.client_secret, AUTH.token_url)  # Get access token

            if access_token:
                api_data = make_api_request(api, access_token)  # Make the API request

                if api_data:
                    insert_data_into_sql(connection, api_data['value'])  # Insert API data into SQL Server
    except Exception as e:
        print(f"An error occurred for {company_name}: {e}")
    finally:
        connection.close()


