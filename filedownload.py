# filedownload.py
import json
import gdown
from googleapiclient.discovery import build
import os

API_KEY = os.environ.get("GOOGLE_API_KEY")
folder_id = os.environ.get("GDRIVE_FOLDER_ID")

# Authenticate with Google Drive API
def authenticate_drive():
    service = build('drive', 'v3', developerKey=API_KEY)
    return service

# Get the file ID of the latest uploaded file in the folder
def get_file_id(folder_id):
    service = authenticate_drive()

    # Query to list files in the folder
    results = service.files().list(q=f"'{folder_id}' in parents", fields="files(id, name)").execute()
    items = results.get('files', [])

    if not items:
        print('No files found in the folder.')
        return None
    else:
        file_id = items[0]['id']
        file_name = items[0]['name']
        print(f"File name: {file_name}, File ID: {file_id}")
        return file_id

# Function to download and load job data from Google Drive
def download_and_load_job_data():
    file_id = get_file_id(folder_id)
    if file_id:
        output_file = "job_data.json"

        # Download the file using gdown
        gdown.download(f"https://drive.google.com/uc?id={file_id}", output_file, quiet=False)

        # Load JSON data
        try:
            with open(output_file, "r", encoding="utf-8") as f:
                raw_data = json.load(f)
            print("Job data loaded successfully!")
            return raw_data
        except json.JSONDecodeError:
            print("Error: The file is not in JSON format or is corrupted.")
            return None