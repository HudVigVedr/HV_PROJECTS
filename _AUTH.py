import configparser

config = configparser.ConfigParser()
config.read('config.ini')

# OAuth2 credentials (same as before)
client_id = config['API']['client_id']
client_secret = config['API']['client_secret']
token_url = "https://login.microsoftonline.com/cc91eaa9-c853-432b-a07d-291b2935204b/oauth2/v2.0/token"


# Base endpoints
end_REST_BOLTRICS_BC = "https://api.businesscentral.dynamics.com/v2.0/cc91eaa9-c853-432b-a07d-291b2935204b/PROD-123/api/boltrics/boltrics/v1.0"
end_REST_MS_BC = "https://api.businesscentral.dynamics.com/v2.0/cc91eaa9-c853-432b-a07d-291b2935204b/PROD-123/api/v2.0"
end_Odata_BC = "https://api.businesscentral.dynamics.com/v2.0/cc91eaa9-c853-432b-a07d-291b2935204b/PROD-123/ODataV4"
end_veson =  ""

# Server credentials
server = "HV-db"
database = "Staging"
username = config['SERVER']['username']
password = config['SERVER']['password']

#connection_string2 = f"DRIVER=ODBC Driver 17 for SQL Server;SERVER=HV-db;DATABASE=Staging;UID=hheij;PWD=ByMus&060R6f"

#mail credentials
email_username = config['EMAIL']['email_username']
email_recipient = "tlems@hudigveder.com"
email_sender = "tlems@hudigveder.com"
smtp_server = "smtp.office365.com"
smtp_port = 587
email_password = config['EMAIL']['email_password']

