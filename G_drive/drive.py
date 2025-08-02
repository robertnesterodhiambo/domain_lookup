import os
import io
import re
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# === CONFIGURATION ===
SERVICE_ACCOUNT_FILE = 'service_account.json'
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
ROOT_FOLDER_ID = '14upfyvbabH1hJs7TtBoM17eXsSjf_cyu'  # Replace with your folder ID
DOWNLOAD_ROOT = 'downloads'

# Mapping for Google Docs export types
EXPORT_MIME_MAP = {
    'application/vnd.google-apps.document': ('application/pdf', '.pdf'),
    'application/vnd.google-apps.spreadsheet': ('application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', '.xlsx'),
    'application/vnd.google-apps.presentation': ('application/pdf', '.pdf'),
}

def sanitize_filename(name):
    """Remove illegal characters from file/folder names."""
    return re.sub(r'[<>:"/\\|?*]', '_', name)

def authenticate():
    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    return build('drive', 'v3', credentials=credentials)

def list_folder_contents(service, folder_id):
    query = f"'{folder_id}' in parents and trashed = false"
    results = service.files().list(
        q=query,
        fields="files(id, name, mimeType)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True
    ).execute()
    return results.get('files', [])

def download_file(service, file_id, file_name, mime_type, local_path):
    dir_path = os.path.dirname(local_path)
    os.makedirs(dir_path, exist_ok=True)

    if mime_type in EXPORT_MIME_MAP:
        export_mime, ext = EXPORT_MIME_MAP[mime_type]
        if not file_name.endswith(ext):
            file_name += ext
        local_path = os.path.join(dir_path, file_name)
        request = service.files().export_media(fileId=file_id, mimeType=export_mime)
    else:
        local_path = os.path.join(dir_path, file_name)
        request = service.files().get_media(fileId=file_id)

    with io.FileIO(local_path, 'wb') as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            if status:
                print(f"Downloading {local_path}: {int(status.progress() * 100)}%")

    print(f"✅ Downloaded: {local_path}")

def download_folder_recursive(service, folder_id, local_dir):
    items = list_folder_contents(service, folder_id)
    for item in items:
        item_name = sanitize_filename(item['name'])
        item_id = item['id']
        item_type = item['mimeType']

        item_path = os.path.join(local_dir, item_name)

        if item_type == 'application/vnd.google-apps.folder':
            print(f"📁 Entering folder: {item_path}")
            download_folder_recursive(service, item_id, item_path)
        else:
            download_file(service, item_id, item_name, item_type, item_path)

def main():
    service = authenticate()
    os.makedirs(DOWNLOAD_ROOT, exist_ok=True)
    download_folder_recursive(service, ROOT_FOLDER_ID, DOWNLOAD_ROOT)

if __name__ == '__main__':
    main()
