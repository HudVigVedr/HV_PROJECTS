import requests
import pandas as pd
import concurrent.futures
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import sys
import os

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
api_table = "generalLedgerAccounts"
api_full = f"{api_url}/{api_table}?$select=no,balance&$filter=apiAccountType eq 'Posting'&company="


def truncate_check_mutations():
    """Truncate the check_mutations table before starting the comparison."""
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE dbo.check_mutations"))


def get_all_gl_accounts():
    """Retrieve all distinct GL accounts per company from BC_GLaccounts."""
    query = """
    SELECT DISTINCT no AS gLAccountNo, Entity
    FROM BC_GLaccounts
    """
    return pd.read_sql(query, engine)


def get_staging_data():
    """Retrieve summed amounts from Staging.BC_GLentries."""
    query = """
    SELECT gLAccountNo AS gLAccountNo, Entity, SUM(amount) AS Amount
    FROM BC_GLentries
    GROUP BY gLAccountNo, Entity
    """
    return pd.read_sql(query, engine)


def get_datamart_data():
    """Retrieve summed amounts from Datamart."""
    query = """
    SELECT LEFT(DK_DimGrootboek, CHARINDEX('|', DK_DimGrootboek) - 1) as gLAccountNo, AC.[Naam database] as Entity, SUM(bedrag) as Amount
    FROM BC_FactGrootboek as FG
    LEFT JOIN Algemeen_Company as AC
    on FG.DK_DimCompany = AC.Company
    GROUP BY
    DK_DimGrootboek, AC.[Naam database]
    """
    return pd.read_sql(query, engine_datamart)


def get_api_data(company_name):
    """Retrieve summed amounts from the API asynchronously."""
    api = f"{api_full}{company_name}"
    api_response = _DEF.make_api_request_list(api, _AUTH.client_id, _AUTH.client_secret, _AUTH.token_url)

    if not api_response:
        return {}

    api_data = {}
    for entry in api_response:
        key = (company_name, str(entry['no']))
        api_data[key] = api_data.get(key, 0) + entry['balance']
    
    return api_data


def insert_mismatches(mismatches):
    """Insert detected mismatches into the check_mutations table."""
    if not mismatches:
        return

    df_mismatches = pd.DataFrame(
        mismatches,
        columns=["GL_Account", "Entity", "API_Amount", "Staging_Amount", "Datamart_Amount", "Deviation"]
    )

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
        subject = "🔍 Check Mutations Report – afwijkingen gevonden"
        body = "Goedemorgen,\n\nZie bijgaand een rapport met alle afwijkingen van de mutatie check tussen BC, DWH-Staging & DWH-Datamart.\n\nMvg,\nThom"

        # Email details from _AUTH
        from_address = _AUTH.email_sender
        to_address = ["thom@blinksolutions.nl", "p.schijff@hudigveder.nl"]  # Replace with the recipient's email(s)
        tenant_id = _AUTH.guid_blink
        client_id = _AUTH.email_client_id 
        client_secret = _AUTH.email_client_secret

        # Send email using _DEF function
        _DEF.send_email_mfa_attachment(subject, body, from_address, to_address, tenant_id, client_id, client_secret, file_path)

        # Remove the file after sending
        os.remove(file_path)
    else:
        print("✅ No deviations found. No email sent.")


def compare_and_store():
    """Compare API, Staging, and Datamart data for all GL accounts per company."""
    print("Starting mutation check...")

    truncate_check_mutations()
    
    df_all_gl_accounts = get_all_gl_accounts()
    df_staging = get_staging_data()
    df_datamart = get_datamart_data()

    # Convert DataFrames to dictionaries
    staging_data = {(row.Entity, str(row.gLAccountNo)): row.Amount for row in df_staging.itertuples(index=False)}
    datamart_data = {(row.Entity, str(row.gLAccountNo)): row.Amount for row in df_datamart.itertuples(index=False)}

    company_names = df_all_gl_accounts["Entity"].unique()
    api_data = {}
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = executor.map(get_api_data, company_names)
    
    for result in results:
        api_data.update({(k[0], str(k[1])): v for k, v in result.items()})

    gl_account_set = {(row.Entity, str(row.gLAccountNo)) for row in df_all_gl_accounts.itertuples(index=False)}

    for key in gl_account_set:
        api_data.setdefault(key, 0)
        staging_data.setdefault(key, 0)
        datamart_data.setdefault(key, 0)

    mismatches = []
    for entity, gl_account in gl_account_set:
        api_amount = api_data.get((entity, gl_account), 0)
        staging_amount = staging_data.get((entity, gl_account), 0)
        datamart_amount = datamart_data.get((entity, gl_account), 0)
        deviation = abs(api_amount - staging_amount) + abs(staging_amount - datamart_amount)
        mismatches.append((gl_account, entity, api_amount, staging_amount, datamart_amount, deviation))
    
    insert_mismatches(mismatches)
    send_mismatch_report()
    print("Mutation check completed.")



if __name__ == "__main__":
    compare_and_store()
