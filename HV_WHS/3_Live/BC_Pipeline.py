import subprocess

print("Pipeline BC started...")
subprocess.run(["python", "BC_FIN_GLE_S.py"])
subprocess.run(["python", "BC_CUSTOMERS.py"])
subprocess.run(["python", "BC_VENDORS.py"])
subprocess.run(["python", "BC_GLaccounts.py"])
subprocess.run(["python", "BC_Services.py"])
subprocess.run(["python", "wmsDH_U.py"])
subprocess.run(["python", "wmsDH_L.py"])

