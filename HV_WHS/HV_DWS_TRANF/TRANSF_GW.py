import pyodbc
import time

import sys
sys.path.append('C:/Python/HV_PROJECTS')
import _AUTH
import _DEF 

script_name = "Transform_Staging_GW"
script_cat = "DWH"

job_name = 'Update Staging Gatship'

# Create a connection to the SQL Server
conn = pyodbc.connect(f"DRIVER=ODBC Driver 17 for SQL Server;SERVER={_AUTH.server};DATABASE={_AUTH.database};UID={_AUTH.username};PWD={_AUTH.password}")

# Create a cursor object to execute SQL commands
cursor = conn.cursor()

# Define the T-SQL command to start the SQL Server Agent job
start_job_sql = f"EXEC msdb.dbo.sp_start_job N'{job_name}'"

try:
    # Execute the T-SQL command to start the job
    cursor.execute(start_job_sql)
    conn.commit()
    print(f"Job '{job_name}' has been started successfully.")
except Exception as e:
    print(f"Error starting job '{job_name}': {str(e)}")

# Polling interval (in seconds)
poll_interval = 10

# Check the job status periodically
while True:
    # Execute sp_help_job to get job status
    cursor.execute(f"EXEC msdb.dbo.sp_help_job @job_name = '{job_name}'")
    row = cursor.fetchone()
    
    if row is not None:
        job_status = row[10]  # The job status is in the 10th column of the result set
        
        if job_status == 1:  # 1 represents job executing
            print(f"Job '{job_name}' is still executing.")
        elif job_status == 4:  # 4 represents job completed
            print(f"Job '{job_name}' has completed.")
            break
        elif job_status == 5:  # 5 represents job failed
            print(f"Job '{job_name}' has failed.")
            break

    # Check the run_status in sysjobhistory
    cursor.execute(f"SELECT TOP 1 run_status FROM msdb.dbo.sysjobhistory WHERE job_id = (SELECT job_id FROM msdb.dbo.sysjobs WHERE name = '{job_name}') ORDER BY run_date DESC, run_duration DESC")
    row = cursor.fetchone()
    
    if row is not None:
        run_status = row[0]
        
        if run_status == 1:  # 1 represents job still running
            print(f"Job '{job_name}' is still running.")
        elif run_status == 3:  # 3 represents job succeeded
            print(f"Job '{job_name}' has succeeded.")
            break
        elif run_status == 0:  # 0 represents job failed
            print(f"Job '{job_name}' has failed.")
            break
    
    time.sleep(poll_interval)

# Close the cursor and connection
cursor.close()
conn.close()

