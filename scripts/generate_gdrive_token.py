import os.path
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2.credentials import Credentials

SCOPES = ['https://www.googleapis.com/auth/drive']

def main():
    # Delete old token.json first to force fresh login
    if os.path.exists('token.json'):
        os.remove('token.json')
        print("Deleted old token.json")

    print("Starting new OAuth2 flow...")
    flow = InstalledAppFlow.from_client_secrets_file(
        'credentials.json', SCOPES
    )
    creds = flow.run_local_server(
        port=8000,
        access_type="offline",  # ← ensures refresh token is included
        prompt="consent"        # ← forces Google to give refresh token every time
    )

    with open('token.json', 'w') as token:
        token.write(creds.to_json())

    # Verify refresh token is present
    import json
    data = json.loads(creds.to_json())
    if data.get("refresh_token"):
        print(f"✅ Success! Refresh token received.")
        print(f"Refresh token: {data['refresh_token'][:20]}...")
    else:
        print("❌ No refresh token received! Try again.")

if __name__ == '__main__':
    main()