import os
import datetime
import shutil
import win32com.client as win32
import pyodbc
import sys
import time

sys.path.append('C:/Python/HV_PROJECTS')
import _AUTH
import _DEF

script_name = "Refresh finance excels"
script_cat = "FIN_excel"

#folder_path = r'C:\Users\beheerder\Hudig & Veder\Rapportage - temp\A'
#destination_folder = r'C:\Users\beheerder\Hudig & Veder\Rapportage - TestAutomation\A'

#local
folder_path = r'C:\Users\ThomLemsBlinkSolutio\Hudig & Veder\Rapportage - Documenten\Original\Afdelingen\Finance\FS sheet\FS - uitgerold\A'
destination_folder = r'C:\Users\ThomLemsBlinkSolutio\Hudig & Veder\Rapportage - Documenten\Original\Afdelingen\Finance\FS sheet\FS - uitgerold\Refreshed Fin sheets\A'

connection_string = f"DRIVER=ODBC Driver 17 for SQL Server;SERVER={_AUTH.server};DATABASE={_AUTH.database};UID={_AUTH.username};PWD={_AUTH.password}"

def safe_log_fallback(message):
    with open("fallback_log.txt", "a", encoding="utf-8") as f:
        f.write(f"{datetime.datetime.now()} | {message}\n")

def delete_old_files(destination_folder):
    cutoff = datetime.datetime.now() - datetime.timedelta(days=31)
    for filename in os.listdir(destination_folder):
        file_path = os.path.join(destination_folder, filename)
        if os.path.getctime(file_path) < cutoff.timestamp():
            try:
                os.remove(file_path)
                print(f"Deleted old file: {file_path}")
            except Exception as e:
                print(f"Error deleting: {file_path} -> {e}")

def is_excel_file_valid(file_path):
    try:
        test_excel = win32.gencache.EnsureDispatch('Excel.Application')
        test_excel.DisplayAlerts = False
        wb = test_excel.Workbooks.Open(file_path, CorruptLoad=2)
        wb.Close(False)
        test_excel.Quit()
        return True
    except Exception:
        return False

def refresh_and_copy_files(folder_path, destination_folder):
    refreshed_files = []
    error_files = []

    excel = win32.gencache.EnsureDispatch('Excel.Application')
    excel.DisplayAlerts = False
    excel.Visible = False
    excel.AskToUpdateLinks = False
    excel.AlertBeforeOverwriting = False

    try:
        for file_name in os.listdir(folder_path):
            if file_name.endswith(".xlsm"):
                file_path = os.path.join(folder_path, file_name)

                if not is_excel_file_valid(file_path):
                    error_files.append((file_name, "Corrupted or unreadable Excel file"))
                    continue

                try:
                    current_date = datetime.datetime.now().strftime('%d%m%Y')
                    base_name, ext = os.path.splitext(file_name)
                    dated_file = f"{base_name}_{current_date}{ext}"
                    dest_path = os.path.join(destination_folder, dated_file)

                    wb = excel.Workbooks.Open(file_path)
                    excel.Application.Run("Module1.DataRefresh")
                    time.sleep(3)
                    wb.Save()
                    wb.Close()
                    shutil.copy(file_path, dest_path)
                    refreshed_files.append(dest_path)
                except Exception as e:
                    error_files.append((file_name, str(e)))
    finally:
        excel.Quit()
        del excel

    return refreshed_files, error_files

if __name__ == "__main__":
    _DEF.quit_all_excel_instances()
    try:
        conn = pyodbc.connect(connection_string)
    except Exception as e:
        safe_log_fallback(f"Database connectie fout: {e}")
        sys.exit(1)

    start = datetime.datetime.now()
    overall_status = "Success"
    full_uri = "N/A"

    try:
        os.makedirs(destination_folder, exist_ok=True)
        delete_old_files(destination_folder)

        refreshed, errors = refresh_and_copy_files(folder_path, destination_folder)

        if errors:
            overall_status = "Error"
            for fname, err in errors:
                msg = f"Error in file {fname}: {err}"
                _DEF.log_status(conn, "Error", script_cat, script_name, start, datetime.datetime.now(), 0, msg, "folder", full_uri)
                _DEF.send_email_mfa(f"ErrorLog -> {script_name}", msg, _AUTH.email_sender, _AUTH.email_recipient, _AUTH.guid_blink, _AUTH.email_client_id, _AUTH.email_client_secret)

        if overall_status == "Success":
            _DEF.log_status(conn, "Success", script_cat, script_name, start, datetime.datetime.now(), 0, "All files processed successfully", "All", full_uri)

    except Exception as e:
        msg = f"Script crash: {e}"
        safe_log_fallback(msg)
        #_DEF.log_status(conn, "Error", script_cat, script_name, start, datetime.datetime.now(), 0, msg, "Script", full_uri)
        _DEF.send_email_mfa(f"Script Crash -> {script_name}", msg, _AUTH.email_sender, _AUTH.email_recipient, _AUTH.guid_blink, _AUTH.email_client_id, _AUTH.email_client_secret)
