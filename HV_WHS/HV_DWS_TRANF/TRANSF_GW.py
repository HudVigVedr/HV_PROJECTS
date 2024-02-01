import pyodbc
import time
import logging

import sys
sys.path.append('C:/Python/HV_PROJECTS')
import _AUTH
import _DEF 

script_name = "Transform_Staging_GW"
script_cat = "DWH"


# Define your constants
DATABASE_SERVER = _AUTH.server
DATABASE_NAME = _AUTH.database
DATABASE_USERNAME = _AUTH.username
DATABASE_PASSWORD = _AUTH.password
JOB_NAME = 'Update Staging Gatship'
POLL_INTERVAL = 5

connection= f"DRIVER=ODBC Driver 17 for SQL Server;SERVER={_AUTH.server};DATABASE={_AUTH.database};UID={_AUTH.username};PWD={_AUTH.password}"

# Configure logging
logging.basicConfig(filename='job_monitor.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to create a database connection
def create_connection():
    try:
        conn = pyodbc.connect(f"DRIVER=ODBC Driver 17 for SQL Server;SERVER={DATABASE_SERVER};DATABASE={DATABASE_NAME};UID={DATABASE_USERNAME};PWD={DATABASE_PASSWORD}")
        return conn
    except pyodbc.Error as e:
        logging.error(f"Error creating database connection: {str(e)}")
        return None

# Function to start the SQL Server job
def start_job(connection, job_name):
    cursor = connection.cursor()
    try:
        cursor.execute(f"EXEC msdb.dbo.sp_start_job N'{job_name}'")
        connection.commit()
        logging.info(f"Job '{job_name}' has been started successfully.")
    except pyodbc.Error as e:
        logging.error(f"Error starting job '{job_name}': {str(e)}")
    finally:
        cursor.close()

# Function to monitor job status
def monitor_job(connection, job_name):
    cursor = connection.cursor()
    while True:
        try:
            cursor.execute(f"EXEC msdb.dbo.sp_help_job @job_name = '{job_name}'")
            row = cursor.fetchone()

            if row is not None:
                job_status = row[25]  # The job status is in the 10th column of the result set

                if job_status == 4:
                    logging.info(f"Job '{job_name}' is still executing.")
                elif job_status == 1:
                    logging.info(f"Job '{job_name}' has completed.")
                    return "Success"  # Return a success status
                elif job_status == 0:
                    logging.error(f"Job '{job_name}' has failed.")
                    return "Failed"  # Return a failed status

            cursor.execute(f"SELECT TOP 1 run_status FROM msdb.dbo.sysjobhistory WHERE job_id = (SELECT job_id FROM msdb.dbo.sysjobs WHERE name = '{job_name}') ORDER BY run_date DESC, run_duration DESC")
            row = cursor.fetchone()

            if row is not None:
                run_status = row[0]

                if run_status == 1:
                    logging.info(f"Job '{job_name}' is still running.")
                elif run_status == 3:
                    logging.info(f"Job '{job_name}' has succeeded.")
                    return "Success"  # Return a success status
                elif run_status == 0:
                    logging.error(f"Job '{job_name}' has failed.")
                    return "Failed"  # Return a failed status

            time.sleep(POLL_INTERVAL)
        except pyodbc.Error as e:
            logging.error(f"Error monitoring job '{job_name}': {str(e)}")


# Main function
def main():

    start_time = _DEF.datetime.now()  # Get the start time

    connection = create_connection()
    if connection:
        start_job(connection, JOB_NAME)
        result = monitor_job(connection, JOB_NAME)
    
        if result == "Success":
            success_message = "Job has completed successfully."
            _DEF.log_status(connection, "Success", script_cat, script_name, start_time, _DEF.datetime.now(), int((_DEF.datetime.now() - start_time).total_seconds() / 60), 0, success_message, "N/A", "N/A")
        else:
            error_details = f"Job '{JOB_NAME}' has failed."
            _DEF.log_status(connection, "Error", script_cat, script_name, start_time, _DEF.datetime.now(), int((_DEF.datetime.now() - start_time).total_seconds() / 60), 0, error_details, "N/A", "N/A")

            # Send an email for the error
            _DEF.send_email_mfa(f"ErrorLog -> {script_name} / {script_cat}", error_details,  _AUTH.email_sender,  _AUTH.email_recipient, _AUTH.guid_blink, _AUTH.email_client_id, _AUTH.email_client_secret)
    connection.close()

if __name__ == '__main__':
    main()