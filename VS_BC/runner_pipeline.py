import os
import subprocess

# Directory containing the scripts
script_dir = os.path.dirname(os.path.realpath(__file__))

# Function to run a script
def run_script(script_path):
    subprocess.run(["python", script_path], check=True)

# Main function
def main():
    script_files = [f for f in os.listdir(script_dir) if f.endswith('.py') and f != os.path.basename(__file__)]

    for script in script_files:
        script_path = os.path.join(script_dir, script)
        run_script(script_path)

if __name__ == "__main__":
    main()
