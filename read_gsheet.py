import gspread
from google.auth import exceptions
from google.oauth2 import service_account

# Load credentials
credentials = service_account.Credentials.from_service_account_file("gsheetpostgres-4ba412f0c6f3.json", scopes=["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"])
client = gspread.Client(auth=credentials)

# Access Google Sheets data
sheet = client.open("University Partnership").sheet1
data = sheet.get_all_records()

# Print the retrieved data
print(data)

