import json
import os
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

SCOPES = ["https://www.googleapis.com/auth/drive"]
token_path = "token.json"

# Load creds
creds = Credentials.from_authorized_user_file(token_path, SCOPES)
if creds.expired and creds.refresh_token:
    creds.refresh(Request())

service = build("drive", "v3", credentials=creds, cache_discovery=False)

print("\n========== LOGGED IN AS ==========")
about = service.about().get(fields="user").execute()
print(f"Email: {about['user']['emailAddress']}")
print(f"Name:  {about['user']['displayName']}")

print("\n========== FOLDERS SHARED WITH YOU ==========")
results = service.files().list(
    q="mimeType='application/vnd.google-apps.folder' and sharedWithMe=true",
    fields="files(id, name, owners)",
    corpora="user",          # ← change this
    supportsAllDrives=True
    # ← remove includeItemsFromAllDrives
).execute()
folders = results.get("files", [])
if not folders:
    print("No folders shared with you!")
else:
    for f in folders:
        owner = f.get("owners", [{}])[0].get("emailAddress", "unknown")
        print(f"Name: {f['name']}  |  ID: {f['id']}  |  Owner: {owner}")

print("\n========== YOUR OWN FOLDERS ==========")
results2 = service.files().list(
    q="mimeType='application/vnd.google-apps.folder' and 'me' in owners",
    fields="files(id, name)",
    supportsAllDrives=True
).execute()
folders2 = results2.get("files", [])
if not folders2:
    print("No folders found!")
else:
    for f in folders2:
        print(f"Name: {f['name']}  |  ID: {f['id']}")

print("\n========== CHECK SPECIFIC FOLDER ID ==========")
folder_id = "1iolyds2YyRF7ylr2o0hA5S50xuX967sM"  # your current ID
try:
    result = service.files().get(
        fileId=folder_id,
        fields="id, name, owners",
        supportsAllDrives=True
    ).execute()
    print(f"✅ Folder accessible! Name: {result['name']}")
except Exception as e:
    print(f"❌ Folder NOT accessible: {e}")