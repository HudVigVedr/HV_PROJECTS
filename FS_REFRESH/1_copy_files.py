import shutil
import os

def copy_files(source_dir, dest_dir):
    # Check if source directory exists
    if not os.path.exists(source_dir):
        print(f"Source directory '{source_dir}' does not exist.")
        return
    
    # Check if destination directory exists, if not, create it
    if not os.path.exists(dest_dir):
        os.makedirs(dest_dir)

    # Get list of files in source directory
    files = os.listdir(source_dir)

    # Iterate over each file and copy it to destination directory
    for file_name in files:
        source_file_path = os.path.join(source_dir, file_name)
        dest_file_path = os.path.join(dest_dir, file_name)
        shutil.copy2(source_file_path, dest_file_path)  # Overwrite existing files
        print(f"Copied '{source_file_path}' to '{dest_file_path}'")

if __name__ == "__main__":
    # Local
    #source_directory = r"C:\Users\ThomLems\Hudig & Veder\Rapportage - Documents\Original\Afdelingen\Finance\FS sheet\Live"
    #destination_directory = r"C:\Users\ThomLems\Hudig & Veder\Rapportage - Documents\Original\Afdelingen\Finance\FS sheet\FS - uitgerold"


    # Server
    source_directory = r"C:\Users\beheerder\Hudig & Veder\Rapportage - Live"
    destination_directory = r"C:\Users\beheerder\Hudig & Veder\Rapportage - temp"

    copy_files(source_directory, destination_directory)
