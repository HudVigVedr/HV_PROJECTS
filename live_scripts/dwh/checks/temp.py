import pyodbc
import sys
sys.path.append('C:/Python/HV_PROJECTS')
import _AUTH
import _DEF

script_name = "BC_GLentries (S)"
script_cat = "DWH"

# SQL Server connection settings
#connection_string = f"DRIVER=ODBC Driver 17 for SQL Server;SERVER={_AUTH.server};DATABASE={_AUTH.database};UID={_AUTH.username};PWD={_AUTH.password}"
connection_string = f"DRIVER=ODBC Driver 17 for SQL Server;SERVER={_AUTH.server};DATABASE=Datamart;UID={_AUTH.username};PWD={_AUTH.password}"

#sql_table = "dbo.BC_GLentries"
sql_table = "dbo.BC_FactGrootboek"
#entryno = "entryNo"
entryno = "Boekstuknr"

def get_max_entry_no_per_entity(connection):
    query = f"""
        SELECT DK_DimCompany, MAX({entryno}) as MaxEntryNo
        FROM {sql_table}
        GROUP BY DK_DimCompany
    """
    cursor = connection.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    
    # Convert results to a dictionary for easy lookup
    return {row[0]: row[1] for row in results}

if __name__ == "__main__":
    print("Fetching max entryNo per Entity...")

    try:
        connection = pyodbc.connect(connection_string)
        max_entry_nos = get_max_entry_no_per_entity(connection)

        # Print the max entryNo for each entity
        for entity, max_entry_no in max_entry_nos.items():
            print(f"Entity: {entity}, Max EntryNo: {max_entry_no}")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        connection.close()
