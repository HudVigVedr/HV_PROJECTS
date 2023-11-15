import requests

# OAuth2 credentials and endpoint
tenant_id = "cc91eaa9-c853-432b-a07d-291b2935204b"
client_id = "d631a172-e9d9-4e5d-bc65-47145f20cb50"
client_secret = "osZ8Q~Y53MPwMrusguQwMclownNvRRbXG8KAgcsX"
resource = "https://hudigvederbv.sharepoint.com~/" 
token_url = "https://login.microsoftonline.com/cc91eaa9-c853-432b-a07d-291b2935204b/oauth2/v2.0/token"

# SharePoint file URL
sharepoint_url = "https://hudigvederbv.sharepoint.com/sites/Rapportages/Gedeelde%%20documenten/HV_WHS/Bronbestanden/Grootboekschema.xlsx"

# Get OAuth2 token
token_data = {
    "grant_type": "client_credentials",
    "client_id": client_id,
    "client_secret": client_secret,
    "scope": resource
}
token_r = requests.post(token_url, data=token_data)
token = token_r.json().get("access_token")

# Send a GET request to the SharePoint URL with the OAuth2 token
headers = {"Authorization": f"Bearer {token}"}
response = requests.get(sharepoint_url, headers=headers)

# Print the status code and response content
print("Status Code:", response.status_code)
print("Response Content:", response.text[:500])  # Print first 500 characters for brevity