import os
import datetime
import shutil
import win32com.client as win32
import pyodbc
import sys
import time
import logging

sys.path.append('C:/Python/HV_PROJECTS')
import _AUTH
import _DEF

script_name = "Refresh finance excels"
script_cat = "FIN_excel"

# Define source/destination folders with labels



folder_pairs = [
    ('A', r'C:\Users\beheerder\Hudig & Veder\Rapportage - FS - uitgerold\A', r'C:\Users\beheerder\Hudig & Veder\Rapportage - FS - uitgerold\Refreshed Fin sheets\A'),
    ('B', r'C:\Users\beheerder\Hudig & Veder\Rapportage - FS - uitgerold\B', r'C:\Users\beheerder\Hudig & Veder\Rapportage - FS - uitgerold\Refreshed Fin sheets\B'),
    ('C', r'C:\Users\beheerder\Hudig & Veder\Rapportage - temp\C', r'C:\Users\beheerder\Hudig & Veder\Rapportage - FS - uitgerold\Refreshed Fin sheets\C'),
]

# Set up logging
now_str = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
log_folder = r"C:\Users\beheerder\Hudig & Veder\Rapportage - FS - uitgerold\Refreshed Fin sheets\Logging"
os.makedirs(log_folder, exist_ok=True)
log_path = os.path.join(log_folder, f"refresh_log_{now_str}.txt")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(log_path, encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# Database connection string
connection_string = f"DRIVER=ODBC Driver 17 for SQL Server;SERVER={_AUTH.server};DATABASE={_AUTH.database};UID={_AUTH.username};PWD={_AUTH.password}"

def safe_log_fallback(message):
    with open("fallback_log.txt", "a", encoding="utf-8") as f:
        f.write(f"{datetime.datetime.now()} | {message}\n")

def delete_old_files(destination_folder):
    cutoff = datetime.datetime.now() - datetime.timedelta(days=31)
    logging.info(f"Deleting files older than {cutoff.strftime('%Y-%m-%d')} in: {destination_folder}")
    for filename in os.listdir(destination_folder):
        file_path = os.path.join(destination_folder, filename)
        try:
            if os.path.getctime(file_path) < cutoff.timestamp():
                os.remove(file_path)
                logging.info(f"Deleted: {file_path}")
        except Exception as e:
            logging.error(f"Error deleting {file_path}: {e}")

import os  # Make sure os is imported at the top

def is_excel_file_valid(file_path):
    try:
        test_excel = win32.gencache.EnsureDispatch('Excel.Application')
        test_excel.DisplayAlerts = False
        wb = test_excel.Workbooks.Open(file_path, CorruptLoad=2)
        wb.Close(False)
        test_excel.Quit()
        return True
    except Exception as e:
        logging.error(f"Invalid Excel file: {file_path} -> {e}")
        return False

def refresh_and_copy_files(folder_label, folder_path, destination_folder):
    refreshed_files = []
    error_files = []

    for file_name in os.listdir(folder_path):
        if not file_name.endswith(".xlsm"):
            continue

        file_path = os.path.join(folder_path, file_name)
        logging.info(f"[{folder_label}] Processing: {file_name}")

        if not is_excel_file_valid(file_path):
            error_files.append((folder_label, file_name, "Corrupted or unreadable Excel file"))
            continue

        excel = None
        wb = None
        try:
            # Start Excel per file
            excel = win32.gencache.EnsureDispatch('Excel.Application')
            excel.DisplayAlerts = False
            excel.Visible = False
            excel.AskToUpdateLinks = False
            excel.AlertBeforeOverwriting = False
            excel.AutomationSecurity = 1  # msoAutomationSecurityLow

            # Open the workbook
            wb = excel.Workbooks.Open(file_path)

            # Run macro
            try:
                excel.Application.Run("Module1.DataRefresh")
                time.sleep(3)  # Give macro time to finish
                wb.Save()
                logging.info(f"[{folder_label}] Refreshed successfully: {file_name}")
            except Exception as macro_error:
                error_msg = f"Macro run failed: {macro_error}"
                error_files.append((folder_label, file_name, error_msg))
                logging.error(f"[{folder_label}] {error_msg}")

        except Exception as excel_error:
            error_msg = f"Excel init/open failed: {excel_error}"
            error_files.append((folder_label, file_name, error_msg))
            logging.error(f"[{folder_label}] {error_msg}")

        finally:
            # Close workbook and quit Excel
            try:
                if wb:
                    wb.Close(SaveChanges=True)
            except Exception as close_error:
                logging.warning(f"[{folder_label}] Failed to close workbook: {file_name} -> {close_error}")

            try:
                if excel:
                    excel.Quit()
                    logging.info(f"[{folder_label}] Excel instance closed for: {file_name}")
            except Exception as quit_error:
                logging.warning(f"[{folder_label}] Failed to quit Excel for {file_name}: {quit_error}")

            # Kill any leftover Excel processes
            try:
                _DEF.quit_all_excel_instances()
                logging.info(f"[{folder_label}] _DEF Excel cleanup done for {file_name}")
            except Exception as def_kill_error:
                logging.warning(f"[{folder_label}] _DEF Excel kill failed: {def_kill_error}")
            try:
                os.system("taskkill /f /im excel.exe >nul 2>&1")
            except Exception as sys_kill_error:
                logging.warning(f"[{folder_label}] taskkill failed for {file_name}: {sys_kill_error}")

            del excel

        # Copy file with date suffix
        try:
            current_date = datetime.datetime.now().strftime('%d%m%Y')
            base_name, ext = os.path.splitext(file_name)
            dated_file = f"{base_name}_{current_date}{ext}"
            dest_path = os.path.join(destination_folder, dated_file)

            shutil.copy(file_path, dest_path)
            refreshed_files.append(dest_path)
            logging.info(f"[{folder_label}] Copied to: {dest_path}")
        except Exception as copy_error:
            error_msg = f"Copy failed: {copy_error}"
            error_files.append((folder_label, file_name, error_msg))
            logging.error(f"[{folder_label}] {error_msg}")

    return refreshed_files, error_files



if __name__ == "__main__":
    _DEF.quit_all_excel_instances()
    logging.info("Script started: Refresh finance excels")

    try:
        conn = pyodbc.connect(connection_string)
        logging.info("Database connection successful.")
    except Exception as e:
        safe_log_fallback(f"Database connection error: {e}")
        logging.critical(f"Could not connect to database: {e}")
        sys.exit(1)

    start = datetime.datetime.now()
    full_uri = "N/A"
    overall_status = "Success"
    total_refreshed = []
    total_errors = []

    try:
        for folder_label, folder_path, destination_folder in folder_pairs:
            logging.info(f"--- Start processing folder {folder_label} ---")

            os.makedirs(destination_folder, exist_ok=True)
            delete_old_files(destination_folder)

            logging.info(f"Refreshing Excel files in: {folder_path}")
            refreshed, errors = refresh_and_copy_files(folder_label, folder_path, destination_folder)

            total_refreshed.extend(refreshed)
            total_errors.extend(errors)

            logging.info(f"[{folder_label}] Refreshed: {len(refreshed)}, Errors: {len(errors)}")

        logging.info(f"TOTAL refreshed: {len(total_refreshed)} file(s), TOTAL errors: {len(total_errors)}")

        for folder_label, fname, err in total_errors:
            msg = f"[{folder_label}] Error in file {fname}: {err}"
            _DEF.log_status(conn, "Error", script_cat, script_name, start, datetime.datetime.now(), 0, msg, "Script", "N/A", full_uri)
            logging.error(f"{msg} (email skipped)")
            _DEF.send_email_mfa(
            subject=f"❌ {script_name} - Crash tijdens uitvoering",
            body=msg,
            from_address=_AUTH.email_sender,
            to_address=["thom@blinksolutions.nl"],
            tenant_id=_AUTH.guid_blink,
            client_id=_AUTH.email_client_id,
            client_secret=_AUTH.email_client_secret
        )

        if overall_status == "Success":
            msg = f"All files processed successfully. Count: {len(total_refreshed)}"
            _DEF.log_status(conn, "Success", script_cat, script_name, start, datetime.datetime.now(), 0, msg, "Script", "N/A", full_uri)
            logging.info(msg)

    except Exception as e:
        msg = f"Script crash: {e}"
        logging.critical(msg)
        safe_log_fallback(msg)
        _DEF.log_status(conn, "Error", script_cat, script_name, start, datetime.datetime.now(), 0, msg, "Script", "N/A", full_uri)
        logging.error(f"{msg} (email skipped)")
        _DEF.send_email_mfa(
            subject=f"❌ {script_name} - Crash tijdens uitvoering",
            body=msg,
            from_address=_AUTH.email_sender,
            to_address=["thom@blinksolutions.nl"],
            tenant_id=_AUTH.guid_blink,
            client_id=_AUTH.email_client_id,
            client_secret=_AUTH.email_client_secret
        )


    logging.info("Script finished.")
