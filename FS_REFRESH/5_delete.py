import os

def delete_specific_folders(directory, folders_to_delete):
    # Iterate over all items (files and directories) in the given directory
    for item in os.listdir(directory):
        # Form the full path of the item
        item_path = os.path.join(directory, item)
        # Check if the item is a directory
        if os.path.isdir(item_path):
            # Check if the directory name is in the list of folders to delete
            if item in folders_to_delete:
                # Delete the directory
                delete_specific_folders(item_path, folders_to_delete)
                os.rmdir(item_path)
                print(f"Deleted directory: {item_path}")
            else:
                # If the directory is not in the list, continue to the next item
                continue

if __name__ == "__main__":
    # Specify the directory path from which you want to delete folders
    directory_path = r"C:\Users\beheerder\Hudig & Veder\Rapportage - temp"
    # Specify the list of folders you want to delete
    folders_to_delete = ["A", "B", "C"]
    # Call the function to delete specific folders
    delete_specific_folders(directory_path, folders_to_delete)