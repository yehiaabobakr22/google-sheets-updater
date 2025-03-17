import gspread
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine
from google.oauth2.service_account import Credentials
import requests
import json

# Function to download the JSON file from Google Drive
def download_json_from_drive(file_id):
    url = f"https://drive.google.com/uc?id={file_id}"
    response = requests.get(url)
    if response.status_code == 200:
        return json.loads(response.text)
    else:
        st.error("Failed to download credentials file.")
        return None

# Replace with your actual Google Drive JSON file ID
DRIVE_FILE_ID = "1f4JXoVoU2JOu8Jsea2mZSTu_kqpAjFQy"

# Load credentials
SERVICE_ACCOUNT_CREDS = download_json_from_drive(DRIVE_FILE_ID)
if SERVICE_ACCOUNT_CREDS:
    credentials = Credentials.from_service_account_info(SERVICE_ACCOUNT_CREDS, scopes=[
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ])
    client = gspread.authorize(credentials)
    sh = client.open_by_key('14AnAINMcJwJO67yjKG8QwPA-R57UtP3263r0PrHEM3I')

    # Database connection
    ops = create_engine("postgresql://analysis_team:ZvVU9ajncL@ops-management-db.statsbomb.com:5432/ops_management")

    st.title("Google Sheets Updater")

    if st.button("Update Sheets"):
        try:
            # Update 'Current Manpower'
            sheet = sh.worksheet('Current Manpower')
            query = """SELECT u.hr_code AS "HR Code", CONCAT(u.first_name, ' ', u.last_name) AS "Name" FROM users u"""
            df = pd.read_sql(query, ops)
            sheet.batch_clear(["A:B"])
            sheet.update([df.columns.values.tolist()] + df.values.tolist())

            # Update 'First Day'
            sheet2 = sh.worksheet('First Day')
            query2 = """SELECT u.hr_code, TO_CHAR(MIN(ss."date"), 'YYYY-MM-DD') AS "First Day" FROM squads_shifts_members ssm"""
            df2 = pd.read_sql(query2, ops)
            sheet2.batch_clear(["A:B"])
            sheet2.update([df2.columns.values.tolist()] + df2.values.tolist())

            st.success("Sheets updated successfully!")
        except Exception as e:
            st.error(f"An error occurred: {e}")
else:
    st.error("Could not authenticate with Google Sheets.")
