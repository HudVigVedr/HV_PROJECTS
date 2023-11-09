import os
import datetime
import time 
import shutil
import smtplib
import logging
import win32com.client as win32
from email.mime.text import MIMEText
import pythoncom

import sys
sys.path.append('C:/Python/HV_PROJECTS')
import _AUTH

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
                logger.info(f"Deleted old file: {file_path}")
            except Exception as e:
                logger.exception(f"Error occurred while deleting old file: {file_path}")


def refresh_and_copy_filesC(folder_path, destination_folder):
    pythoncom.CoInitialize()

    # Refresh data connections and copy files
    successful_files = []
    error_files = []
    refreshed_files = []

    # Create an instance of the Excel application
    excel = win32.gencache.EnsureDispatch('Excel.Application')
    excel.Visible = True

    for file_name in os.listdir(folder_pathC):
        if file_name.endswith('.xlsm'):
            try:
                current_date = datetime.datetime.now().strftime('%d%m%Y')

                # Append the current date to the original file name, preserving the extension
                base_file_name, extension = os.path.splitext(file_name)
                dated_file_name = f"{base_file_name}_{current_date}{extension}"

                file_path = os.path.join(folder_pathC, file_name)
                destination_path = os.path.join(destination_folderC, dated_file_name)

                # Open the workbook
                wb = excel.Workbooks.Open(file_path)
                logger.info(f"Opened workbook: {file_path}")

                # Refresh data connections
                excel.Application.Run("Module1.DataRefresh")
                time.sleep(1)  # Wait for 1 second

                ws = wb.Worksheets(1)
                ws.Range('S5').Value = datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S')  # Adding a timestamp
                wb.Save()
                wb.Close()

                # Append the file path to refreshed_files
                refreshed_files.append((file_path, destination_path))

            except Exception as e:
                error_files.append((file_name, str(e)))
                logger.exception(f"Error occurred while processing file: {file_path}")

    # Quit the Excel application
    os.system("taskkill /f /im EXCEL.EXE")
    #win32.pythoncom.CoUninitialize()

    # After all files have been refreshed, start copying them one by one
    for file_path, destination_path in refreshed_files:
        try:
            shutil.copy(file_path, destination_path)
            successful_files.append(file_path)
            logger.info(f"Copied and protected file: {file_path}")
        except Exception as e:
            error_files.append((file_path, str(e)))
            logger.exception(f"Error occurred while copying file: {file_path}")

    return successful_files, error_files

def refresh_and_copy_files(folder_path, destination_folder):
    # Refresh data connections and copy files

    pythoncom.CoInitialize()

    successful_files = []
    error_files = []
    refreshed_files = []

    # Create an instance of the Excel application
    excel = win32.gencache.EnsureDispatch('Excel.Application')

    excel.Visible = True

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
                logger.info(f"Opened workbook: {file_path}")

                # Refresh data connections
                excel.Application.Run("Module1.DataRefresh")
                time.sleep(10)  # Wait for 1 second

                ws = wb.Worksheets(5)
                ws.Range('C6').Value = datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S')  # Adding a timestamp
                wb.Save()
                wb.Close()

                # Append the file path to refreshed_files
                refreshed_files.append((file_path, destination_path))

            except Exception as e:
                error_files.append((file_name, str(e)))
                logger.exception(f"Error occurred while processing file: {file_path}")

    # Quit the Excel application
    time.sleep(5) 
    excel.Quit()
    #os.system("taskkill /f /im EXCEL.EXE")
    #win32.pythoncom.CoUninitialize()

    # After all files have been refreshed, start copying them one by one
    for file_path, destination_path in refreshed_files:
        try:
            shutil.copy(file_path, destination_path)
            successful_files.append(file_path)
            logger.info(f"Copied and protected file: {file_path}")
        except Exception as e:
            error_files.append((file_path, str(e)))
            logger.exception(f"Error occurred while copying file: {file_path}")

    return successful_files, error_files


def send_email(successful_files, error_files, start_time, end_time, execution_time, sender_email, receiver_email,
               smtp_server, smtp_port, smtp_username, smtp_password):
    # Send email notification for successful execution
    if successful_files:
        subject = "FS - success"
        message = "The script has successfully executed. Copied and protected files:\n\n"
        message += "\n".join(successful_files)
        message += f"\n\nScript started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}"  # add start time
        message += f"\nScript ended at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}"  # add end time
        message += f"\nScript execution time: {execution_time}"  # add execution time
        msg = MIMEText(message)
        msg['Subject'] = subject
        msg['From'] = sender_email
        msg['To'] = receiver_email

        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(smtp_username, smtp_password)
            server.sendmail(sender_email, receiver_email, msg.as_string())

    # Send email notification for error cases
    if error_files:
        subject = "FS - error"
        message = "An error occurred while processing the following files:\n\n"
        for file_name, error in error_files:
            message += f"File: {file_name}\nError: {error}\n\n"
        message += f"\n\nScript started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}"  # add start time
        message += f"\nScript ended at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}"  # add end time
        message += f"\nScript execution time: {execution_time}"  # add execution time
        msg = MIMEText(message)
        msg['Subject'] = subject
        msg['From'] = sender_email
        msg['To'] = receiver_email

        with smtplib.SMTP(smtp_server, smtp_port) as server:
            server.starttls()
            server.login(smtp_username, smtp_password)
            server.sendmail(sender_email, receiver_email, msg.as_string())


if __name__ == "__main__":

    # Configure logging
    logging.basicConfig(filename='logB.log', level=logging.DEBUG)
    logger = logging.getLogger(__name__)

    # Capture the start time
    start_time = datetime.datetime.now()

    # Determine the start and end dates for file retention
    #retention_start_date, retention_end_date = calculate_retention_dates()

    # Email configuration
    sender_email = _AUTH.email_sender
    receiver_email = _AUTH.email_recipient
    smtp_server = "smtp.office365.com"
    smtp_port = 587
    smtp_username = _AUTH.email_username
    smtp_password = _AUTH.email_password

    #Folder A
    #folder_pathA = r'C:\Users\ThomLems\Hudig & Veder\Rapportage - Documents\Original\Afdelingen\Finance\FS sheet\Live\A'
    #destination_folderA = r'C:\Users\ThomLems\Hudig & Veder\Rapportage - Documents\Original\Afdelingen\Finance\FS sheet\FS - uitgerold\TestAutomation\A'
    folder_pathA = r'C:\Users\beheerder\Hudig & Veder\Rapportage - Live\A'
    destination_folderA = r'C:\Users\beheerder\Hudig & Veder\Rapportage - TestAutomation\A'

    #Folder B
    #folder_pathB = r'C:\Users\ThomLems\Hudig & Veder\Rapportage - Documents\Original\Afdelingen\Finance\FS sheet\Live\B'
    #destination_folderB = r'C:\Users\ThomLems\Hudig & Veder\Rapportage - Documents\Original\Afdelingen\Finance\FS sheet\FS - uitgerold\TestAutomation\B'
    folder_pathB = r'C:\Users\beheerder\Hudig & Veder\Rapportage - Live\B'
    destination_folderB = r'C:\Users\beheerder\Hudig & Veder\Rapportage - TestAutomation\B'

    #Folder C
    #folder_pathC = r'C:\Users\ThomLems\Hudig & Veder\Rapportage - Documents\Original\Afdelingen\Finance\FS sheet\Live\C'
    #destination_folderC = r'C:\Users\ThomLems\Hudig & Veder\Rapportage - Documents\Original\Afdelingen\Finance\FS sheet\FS - uitgerold\TestAutomation\C'
    folder_pathC = r'C:\Users\beheerder\Hudig & Veder\Rapportage - Live\C'
    destination_folderC = r'C:\Users\beheerder\Hudig & Veder\Rapportage - TestAutomation\C'
    
    #RunA
    os.makedirs(destination_folderA, exist_ok=True)
    delete_old_files(destination_folderA)
    successful_files, error_files = refresh_and_copy_files(folder_pathA, destination_folderA)

    # Capture the end time
    end_time = datetime.datetime.now()

    # Calculate the script execution time
    execution_time = str(end_time - start_time)

    # Send email notifications
    send_email(successful_files, error_files, start_time, end_time, execution_time, sender_email, receiver_email,
               smtp_server, smtp_port, smtp_username, smtp_password)

    #RunB
    os.makedirs(destination_folderB, exist_ok=True)
    delete_old_files(destination_folderB)
    successful_files, error_files = refresh_and_copy_files(folder_pathB, destination_folderB)

    # Capture the end time
    end_time = datetime.datetime.now()

    # Calculate the script execution time
    execution_time = str(end_time - start_time)

    # Send email notifications
    send_email(successful_files, error_files, start_time, end_time, execution_time, sender_email, receiver_email,
               smtp_server, smtp_port, smtp_username, smtp_password)

    #RunC
    os.makedirs(destination_folderC, exist_ok=True)
    delete_old_files(destination_folderC)
    successful_files, error_files = refresh_and_copy_filesC(folder_pathC, destination_folderC)


    # Capture the end time
    end_time = datetime.datetime.now()

    # Calculate the script execution time
    execution_time = str(end_time - start_time)

    # Send email notifications
    send_email(successful_files, error_files, start_time, end_time, execution_time, sender_email, receiver_email,
               smtp_server, smtp_port, smtp_username, smtp_password)
