import os
import subprocess
import time
import csv

# Directory containing the scripts
script_dir = os.path.dirname(os.path.realpath(__file__))

# CSV file to store the results
csv_file = os.path.join(script_dir, "script_runtimes.csv")

# Function to run a script and measure its execution time
def run_script(script_path):
    start_time = time.time()
    subprocess.run(["python", script_path], check=True)
    end_time = time.time()
    return end_time - start_time

# Main function
def main():
    script_files = [f for f in os.listdir(script_dir) if f.endswith('.py') and f != os.path.basename(__file__)]

    with open(csv_file, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(['Script Name', 'Execution Time (minutes)'])

        for script in script_files:
            script_path = os.path.join(script_dir, script)
            runtime = run_script(script_path)
            writer.writerow([script, runtime])

if __name__ == "__main__":
    main()
