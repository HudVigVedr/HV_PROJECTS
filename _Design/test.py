from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
from office365.sharepoint.files.file import File
import pandas as pd
import pyodbc
import io

# Importing custom authentication module
import sys
sys.path.append('C:/Python/HV_PROJECTS')
import _AUTH

# Local Excel file path
file_path = r"C:\Users\ThomLems\Hudig & Veder\Rapportage - Documents\HV_WHS\Bronbestanden\Grootboekschema.xlsx"  # replace with your actual file name
#file_path = r""  

# SQL database details
sql_connection_string = f"DRIVER=ODBC Driver 17 for SQL Server;SERVER={_AUTH.server};DATABASE={_AUTH.database};UID={_AUTH.username};PWD={_AUTH.password}"
table_name = 'BC_GLaccounts_mapping'

def read_excel(file_path):
    try:
        df = pd.read_excel(file_path, engine='openpyxl')
        return df
    except Exception as e:
        print(f"An error occurred while reading the Excel file: {e}")
        return None

# Function to import the data into SQL
def import_to_sql(df, sql_connection_string, table_name):
    try:
        # Establish SQL connection

        conn = pyodbc.connect(sql_connection_string)
        cursor = conn.cursor()

        # Insert data into SQL table
        for index, row in df.iterrows():
            cursor.execute(f"""
                INSERT INTO {table_name} ([Ledgernummer]) 
                values(?)""", 
                row['Ledgernummer']
            )

        conn.commit()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"An error occurred while inserting into SQL: {e}")

# Main execution
df = read_excel(file_path)
if df is not None:
    # Proceed with importing data to SQL
    import_to_sql(df, sql_connection_string, table_name)
else:
    print("Failed to read data from the Excel file")