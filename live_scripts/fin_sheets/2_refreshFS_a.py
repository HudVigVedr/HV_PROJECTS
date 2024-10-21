import os
import datetime
import logging
import shutil
from contextlib import contextmanager
import win32com.client as win32
import pythoncom
import pyodbc
import sys
import time

import sys
sys.path.append('C:/Python/HV_PROJECTS')
import _AUTH
import _DEF

script_name = "Refresh finance excels"
script_cat = "FIN_excel"

#local
#folder_path = r'C:\Users\ThomLems\Hudig & Veder\Rapportage - Documents\Original\Afdelingen\Finance\FS sheet\FS - uitgerold\A'
#destination_folder = r'C:\Users\ThomLems\Hudig & Veder\Rapportage - Documents\Original\Afdelingen\Finance\FS sheet\FS - uitgerold\Refreshed Fin sheets\A'

#server
folder_path = r'C:\Users\beheerder\Hudig & Veder\Rapportage - temp\A'
destination_folder = r'C:\Users\beheerder\Hudig & Veder\Rapportage - TestAutomation\A'

connection_string = f"DRIVER=ODBC Driver 17 for SQL Server;SERVER={_AUTH.server};DATABASE={_AUTH.database};UID={_AUTH.username};PWD={_AUTH.password}"

def delete_old_files(destination_folder):
    current_date = datetime.datetime.now()
    retention_period = datetime.timedelta(days=31)
    cutoff_date = current_date - retention_period  # Files older than this will be deleted

    for filename in os.listdir(destination_folder):
        file_path = os.path.join(destination_folder, filename)
        # Get the file's creation time
        creation_time = os.path.getctime(file_path)
        creation_date = datetime.datetime.fromtimestamp(creation_time)
        if creation_date < cutoff_date:
            try:
                os.remove(file_path)
                print(f"Deleted old file: {file_path}")
            except Exception as e:
                print(f"Error occurred while deleting old file: {file_path}")


def refresh_and_copy_files(folder_path, destination_folder):

    # Refresh data connections and copy files
    successful_files = []
    error_files = []
    refreshed_files = []

    #Create an instance of the Excel application
    excel = win32.gencache.EnsureDispatch('Excel.Application')

    for file_name in os.listdir(folder_path):
        if file_name.endswith('.xlsm'):
            try:
                current_date = datetime.datetime.now().strftime('%d%m%Y')

                # Append the current date to the original file name, preserving the extension
                base_file_name, extension = os.path.splitext(file_name)
                dated_file_name = f"{base_file_name}_{current_date}{extension}"

                file_path = os.path.join(folder_path, file_name)
                destination_path = os.path.join(destination_folder, dated_file_name)

                # Open the workbook
                wb = excel.Workbooks.Open(file_path)
                #logger.info(f"Opened workbook: {file_path}")

                # Refresh data connections
                #ws = wb.Worksheets(1)
                #ws.Range('S5').Value = datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S')  # Adding a timestamp

                excel.Application.Run("Module1.DataRefresh")
                time.sleep(5)  # Wait for 1 second

                wb.Save()
                wb.Close()

                # Append the file path to refreshed_files
                refreshed_files.append((file_path, destination_path))

            except Exception as e:
                error_files.append((file_name, str(e)))
                #logger.exception(f"Error occurred while processing file: {file_path}")

    # Quit the Excel application
    os.system("taskkill /f /im EXCEL.EXE")


    # After all files have been refreshed, start copying them one by one
    for file_path, destination_path in refreshed_files:
        try:
            shutil.copy(file_path, destination_path)
            successful_files.append(file_path)
            #logger.info(f"Copied and protected file: {file_path}")
        except Exception as e:
            error_files.append((file_path, str(e)))
            #logger.exception(f"Error occurred while copying file: {file_path}")

    return successful_files, error_files


if __name__ == "__main__":

    _DEF.quit_all_excel_instances()
    # Configure logging
    #logging.basicConfig(filename='log.log', level=logging.DEBUG)
    #logger = logging.getLogger(__name__)

    connection = pyodbc.connect(connection_string)

    # Capture the start time
    start_time = datetime.datetime.now()

    print("FS files processing...")

    # Initialize variables
    overall_status = "Success"
    full_uri = "N/A"  

    try:
        # Define folder paths for processing
        folders = {
            "folder": (folder_path, destination_folder),
        }

        for folder_name, (folder_path, destination_folder) in folders.items():
            current_time = datetime.datetime.now()
            elapsed_time = (current_time - start_time).total_seconds()
            if elapsed_time > 600: 
                raise TimeoutError("Script execution exceeded 10 minutes.")

            os.makedirs(destination_folder, exist_ok=True)
            delete_old_files(destination_folder)


            successful_files, error_files = refresh_and_copy_files(folder_path, destination_folder)


            if error_files:
                    overall_status = "Error"
                    for file_name, error in error_files:
                        error_details = f"Error in file {file_name}: {error}"
                        _DEF.log_status(connection, "Error", script_cat, script_name, start_time, 
                                        datetime.datetime.now(), 
                                        int((datetime.datetime.now() - start_time).total_seconds() / 60), 
                                        1, error_details, folder_name, full_uri)

                        _DEF.send_email_mfa(f"ErrorLog -> {script_name} / {script_cat}", error_details,  _AUTH.email_sender,  _AUTH.email_recipient, _AUTH.guid_blink, _AUTH.email_client_id, _AUTH.email_client_secret)


        if overall_status == "Success":
            # Log a success entry if no errors were found in any folder
            _DEF.log_status(connection, "Success", script_cat, script_name, start_time, 
                            datetime.datetime.now(), 
                            int((datetime.datetime.now() - start_time).total_seconds() / 60), 
                            0, "No errors found in any folder", "All", full_uri)

    except TimeoutError as te:
        overall_status = "Error"
        error_details = str(te)
        # Log the timeout error
        _DEF.log_status(connection, "Error", script_cat, script_name, start_time, 
                        datetime.datetime.now(), 
                        int((datetime.datetime.now() - start_time).total_seconds() / 60), 
                        0, error_details, "N/A", full_uri)
        _DEF.send_email_mfa(f"ErrorLog -> {script_name} / {script_cat}", error_details,  _AUTH.email_sender,  _AUTH.email_recipient, _AUTH.guid_blink, _AUTH.email_client_id, _AUTH.email_client_secret)

    
    except Exception as e:
        # Catch-all for any unexpected errors in the script
        overall_status = "Error"
        error_details = str(e)
        _DEF.log_status(connection, "Error", script_cat, script_name, start_time, 
                        datetime.datetime.now(), 
                        int((datetime.datetime.now() - start_time).total_seconds() / 60), 
                        0, error_details, "N/A", full_uri)

        _DEF.send_email_mfa(f"ErrorLog -> {script_name} / {script_cat}", error_details,  _AUTH.email_sender,  _AUTH.email_recipient, _AUTH.guid_blink, _AUTH.email_client_id, _AUTH.email_client_secret)