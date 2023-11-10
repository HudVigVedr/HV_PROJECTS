
import configparser
config = configparser.ConfigParser()
config.read('C:/Python/HV_PROJECTS/config.ini')


# OAuth2 credentials (same as before)
client_id = config['API_CRED']['client_id']
client_secret = config['API_CRED']['client_secret']
token_url = "https://login.microsoftonline.com/cc91eaa9-c853-432b-a07d-291b2935204b/oauth2/v2.0/token"
vs_token = config['API_CRED']['vs_token']

# Base endpoints
end_REST_BOLTRICS_BC = "https://api.businesscentral.dynamics.com/v2.0/cc91eaa9-c853-432b-a07d-291b2935204b/PROD-123/api/boltrics/boltrics/v1.0"
end_REST_MS_BC = "https://api.businesscentral.dynamics.com/v2.0/cc91eaa9-c853-432b-a07d-291b2935204b/PROD-123/api/v2.0"
end_Odata_BC = "https://api.businesscentral.dynamics.com/v2.0/cc91eaa9-c853-432b-a07d-291b2935204b/PROD-123/ODataV4"
end_veson =  "https://api.veslink.com/v1/imos/reports/"
BC_URi = "https://businesscentral.dynamics.com/cc91eaa9-c853-432b-a07d-291b2935204b/PROD-123?company="

# Server credentials
server = "HV-db"
database = "Staging"
username = config['SERVER_CRED']['username']
password = config['SERVER_CRED']['password']


#mail credentials
email_username = config['EMAIL_CRED']['email_username']
email_recipient = "tlems@hudigveder.com"
email_sender = "tlems@hudigveder.com"
smtp_server = "smtp.office365.com"
smtp_port = 587
email_password = config['EMAIL_CRED']['email_password']

