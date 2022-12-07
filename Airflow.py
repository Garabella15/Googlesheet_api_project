import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from airflow.contrib.hooks.gcp_api_base_hook import GoogleCloudBaseHook

scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']

def get_cred_dict(conn_id='my_google_connection'):
    gcp_hook = GoogleCloudBaseHook(gcp_conn_id=conn_id)
    return json.loads(gcp_hook._get_field('keyfile_dict'))

def get_client(conn_id='my_google_connection'):
    cred_dict = get_cred_dict(conn_id)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(cred_dict, scope)
    client = gspread.authorize(creds)
    return client

def get_sheet(doc_name, sheet_name):
    client = get_client()
    sheet = client.open(doc_name).worksheet(sheet_name)
    return sheet