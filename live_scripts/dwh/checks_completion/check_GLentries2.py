import requests
import pandas as pd
import concurrent.futures
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import sys
import os
import base64
import json

sys.path.append('C:/Python/HV_PROJECTS')
import _AUTH
import _DEF

script_name = "Check Mutations"
script_cat = "DWH_checks"

# SQLAlchemy Engine Setup
engine = create_engine(
    f"mssql+pyodbc://{_AUTH.username}:{_AUTH.password}@{_AUTH.server}/{_AUTH.database}?driver=ODBC+Driver+17+for+SQL+Server",
    fast_executemany=True
)
engine_datamart = create_engine(
    f"mssql+pyodbc://{_AUTH.username}:{_AUTH.password}@{_AUTH.server}/Datamart?driver=ODBC+Driver+17+for+SQL+Server",
    fast_executemany=True
)

# API endpoint URL
api_url = _AUTH.end_REST_BOLTRICS_BC
api_table = "generalLedgerEntries"
api_full = f"{api_url}/{api_table}?$select=gLAccountNo,amount&company="


def truncate_check_mutations():
    """Truncate the check_mutations table before starting the comparison."""
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE dbo.check_mutations"))


def get_all_gl_accounts():
    """Retrieve all distinct GL accounts per company from BC_GLaccounts."""
    query = """
    SELECT DISTINCT no AS GLAccountNo, Entity 
    FROM BC_GLaccounts
    """
    return pd.read_sql(query, engine)


def get_staging_data():
    """Retrieve summed amounts from Staging.BC_GLentries."""
    query = """
    SELECT GA.no AS GLAccountNo, GA.Entity, 
           COALESCE(SUM(SG.amount), 0) AS amount
    FROM BC_GLaccounts AS GA
    LEFT JOIN BC_GLentries AS SG 
        ON GA.no = SG.gLAccountNo
        AND GA.Entity = SG.Entity
    GROUP BY GA.no, GA.Entity
    """
    return pd.read_sql(query, engine)


def get_datamart_data():
    """Retrieve summed amounts from Datamart ensuring all GL accounts are included."""
    query = """
    SELECT GA.no AS GLAccountNo, GA.Entity AS Dk_Dimcompany, 
           COALESCE(SUM(FG.Bedrag), 0) AS amount
    FROM Staging.dbo.BC_GLaccounts AS GA
    LEFT JOIN BC_FactGrootboek AS FG
        ON GA.no = LEFT(FG.DK_DimGrootboek, CHARINDEX('|', FG.DK_DimGrootboek) - 1)
        AND GA.Entity = FG.DK_DimCompany
    GROUP BY GA.no, GA.Entity
    """
    return pd.read_sql(query, engine_datamart)


def get_api_data(company_name):
    """Retrieve summed amounts from the API asynchronously."""
    api = f"{api_full}{company_name}"
    api_response = _DEF.make_api_request_list(api, _AUTH.client_id, _AUTH.client_secret, _AUTH.token_url)

    if not api_response:
        return {}

    # Aggregate API results into a dictionary
    api_data = {}
    for entry in api_response:
        key = (company_name, entry['gLAccountNo'])
        api_data[key] = api_data.get(key, 0) + entry['amount']

    return api_data


def insert_mismatches(mismatches):
    """Insert detected mismatches into the check_mutations table."""
    if not mismatches:
        return

    df_mismatches = pd.DataFrame(
        mismatches,
        columns=["GL_Account", "Entity", "API_Amount", "Staging_Amount", "Datamart_Amount", "Deviation"]
    )

    # Convert to correct data types
    df_mismatches = df_mismatches.astype({
        "GL_Account": str,
        "Entity": str,
        "API_Amount": float,
        "Staging_Amount": float,
        "Datamart_Amount": float,
        "Deviation": float
    })

    with engine.begin() as conn:
        df_mismatches.to_sql("check_mutations", con=conn, if_exists="append", index=False, schema="dbo", chunksize=100)


def fetch_mismatches():
    """Retrieve all deviations (Deviation <> 0) from check_mutations."""
    query = """
    SELECT * FROM dbo.check_mutations
    WHERE Deviation <> 0
    """
    return pd.read_sql(query, engine)


def export_to_excel(df):
    """Export DataFrame to Excel and return file path."""
    file_path = "check_mutations_report.xlsx"
    df.to_excel(file_path, index=False)
    return file_path


def send_mismatch_report():
    """Check for mismatches and send an email report if there are deviations."""
    df_mismatches = fetch_mismatches()

    if not df_mismatches.empty:
        # Export to Excel
        file_path = export_to_excel(df_mismatches)

        # Email details
        subject = "🔍 Check Mutations Report – Deviations Found"
        body = "Dear team,\n\nAttached is the report containing all detected deviations.\n\nBest regards,\nYour Automation Script"

        # Email details from _AUTH
        from_address = _AUTH.email_sender
        to_address = ["thom@blinksolutions.nl"]  # Replace with the recipient's email(s)
        tenant_id = _AUTH.guid_blink
        client_id = _AUTH.client_id
        client_secret = _AUTH.client_secret

        # Send email using _DEF function
        _DEF.send_email_mfa_attachment(subject, body, from_address, to_address, tenant_id, client_id, client_secret, file_path)

        # Remove the file after sending
        os.remove(file_path)
    else:
        print("✅ No deviations found. No email sent.")


def compare_and_store():
    """Compare API, Staging, and Datamart data for all GL accounts per company."""

    print("Starting mutation check between API, Staging, and Datamart...")

    # Step 1: Truncate check_mutations table
    truncate_check_mutations()

    # Step 2: Get all unique GL accounts per company
    df_all_gl_accounts = get_all_gl_accounts()

    # Step 3: Fetch API, Staging, and Datamart data
    df_staging = get_staging_data()
    df_datamart = get_datamart_data()

    # Step 4: Convert SQL DataFrames to dictionaries **(ensure GL_Account is str)**
    staging_data = {(row.Entity, str(row.GLAccountNo)): row.amount for row in df_staging.itertuples(index=False)}
    datamart_data = {(row.Dk_Dimcompany, str(row.GLAccountNo)): row.amount for row in df_datamart.itertuples(index=False)}

    # Step 5: Fetch API data using ThreadPoolExecutor
    company_names = df_all_gl_accounts["Entity"].unique()
    api_data = {}
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = executor.map(get_api_data, company_names)

    for result in results:
        api_data.update({(k[0], str(k[1])): v for k, v in result.items()})  # Ensure GL_Account is str

    # Step 6: Ensure every GL account per company is present in all datasets
    gl_account_set = {(row.Entity, str(row.GLAccountNo)) for row in df_all_gl_accounts.itertuples(index=False)}

    for key in gl_account_set:
        api_data.setdefault(key, 0)
        staging_data.setdefault(key, 0)
        datamart_data.setdefault(key, 0)

    # Step 7: Compare values and collect all GL accounts (even when Deviation = 0)
    mismatches = []
    for entity, gl_account in gl_account_set:
        api_amount = api_data.get((entity, gl_account), 0)
        staging_amount = staging_data.get((entity, gl_account), 0)
        datamart_amount = datamart_data.get((entity, gl_account), 0)

        deviation = abs(api_amount - staging_amount) + abs(staging_amount - datamart_amount)

        # ✅ Insert all GL accounts, even if Deviation = 0
        mismatches.append((gl_account, entity, api_amount, staging_amount, datamart_amount, deviation))

    # Insert all accounts into the database
    insert_mismatches(mismatches)

    print("Mutation check completed.")

    #send_mismatch_report()


if __name__ == "__main__":
    compare_and_store()
