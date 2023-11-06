import os
import subprocess

# Get the directory of the runner.py script
script_directory = os.path.dirname(os.path.abspath(__file__))
print("Script is running from:", script_directory)

# Change the current working directory to the script directory
os.chdir(script_directory)

# List all files in the current directory
files = os.listdir('.')

# Filter out Python files, excluding this script
python_files = [f for f in files if f.endswith('.py') and f != 'runner.py']

# Run each Python file
for file in python_files:
    try:
        # Using subprocess.run instead of os.system for better error handling
        result = subprocess.run(['python', file], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print(f"Successfully ran {file}: {result.stdout}")
    except subprocess.CalledProcessError as e:
        print(f"Error running {file}: {e.stderr}")
