import requests
import csv
from io import StringIO

def make_api_request_vs(url):
    response = requests.get(url)
    response.raise_for_status()  # Will raise an error for bad status codes
    return response.text

def fetch_and_print_csv_data():
    # Define the API endpoint and the API token
    url = "https://api.veslink.com/v1/imos/reports/Fixtures_WHS_HV_LC?apiToken=3c1ac6db0a97436e41f8e1e7e443f0e025ed1e0edfde2df2e6c1fce557e4d7ce"

    try:
        # Use the make_api_request_vs function to make the API request
        csv_content = make_api_request_vs(url)

        # Use StringIO to read the CSV data
        csv_data = StringIO(csv_content)
        csv_reader = csv.reader(csv_data)

        # Print each row in the CSV data
        for row in csv_reader:
            print(row)

    except requests.HTTPError as e:
        print(f"HTTP error occurred: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

# Call the function
fetch_and_print_csv_data()
