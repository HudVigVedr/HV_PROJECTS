import subprocess

print("Pipeline BC started...")
subprocess.run(["python", "BC_FIN_GLE_U.py"])
subprocess.run(["python", "BC_CUSTOMERS.py"])
subprocess.run(["python", "BC_VENDORS.py"])
subprocess.run(["python", "BC_GLaccounts.py"])
