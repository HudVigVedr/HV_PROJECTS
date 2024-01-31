#Local path
path_script = "C:/Python/HV_PROJECTS"

#server_path
#path_script = "C:/Python/HV_PROJECTS"



import configparser
config = configparser.ConfigParser()
config.read(f"{path_script}/config.ini")


# OAuth2 credentials (same as before)
bc_guid = config['API_CRED']['bc_guid']
client_id = config['API_CRED']['client_id']
client_secret = config['API_CRED']['client_secret']
token_url = f"https://login.microsoftonline.com/{bc_guid}/oauth2/v2.0/token"
vs_token = config['API_CRED']['vs_token']

# Base endpoints
bc_custno = config['API_CRED']['bc_custno']
end_REST_BOLTRICS_BC = f"https://api.businesscentral.dynamics.com/v2.0/{bc_guid}/{bc_custno}/api/boltrics/boltrics/v1.0"
end_REST_MS_BC = f"https://api.businesscentral.dynamics.com/v2.0/{bc_guid}/{bc_custno}/api/v2.0"
end_Odata_BC = f"https://api.businesscentral.dynamics.com/v2.0/{bc_guid}/{bc_custno}/ODataV4"
end_veson =  "https://api.veslink.com/v1/imos/reports/"
BC_URi = f"https://businesscentral.dynamics.com/{bc_guid}/{bc_custno}?company="
BC_base = f"https://api.businesscentral.dynamics.com/v2.0/{bc_guid}/"
vs_quee = "https://api.veslink.com/v1/imosmessaging/queue"


# Server credentials
server = "HV-db"
database = "Staging"
username = config['SERVER_CRED']['username']
password = config['SERVER_CRED']['password']

connection_string = f"DRIVER=ODBC Driver 17 for SQL Server;SERVER={server};DATABASE={database};UID={username};PWD={password}"

#mail credentials
email_username = config['EMAIL_CRED']['email_username']
email_recipient = "thom@blinksolutions.nl"
email_sender = "thom@blinksolutions.nl"
smtp_server = "smtp.office365.com"
#smtp_server = "hudigveder-nl.mail.protection.outlook.com"
smtp_port = 587
email_password = config['EMAIL_CRED']['email_password']
email_client_id = config['EMAIL_CRED']['email_client_id']
email_tenant_id = config['EMAIL_CRED']['email_tenant_id']
email_client_secret = config['EMAIL_CRED']['email_client_secret']
guid_blink = config['EMAIL_CRED']['guid_blink']
token_url_mail = f"https://login.microsoftonline.com/{guid_blink}/oauth2/v2.0/token"

#Sharepoint
sharepoint_url = "https://hudigvederbv.sharepoint.com/"
sharepoint_username = ""
sharepoint_password = ""
