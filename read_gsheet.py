import gspread
from google.auth import exceptions
from google.oauth2 import service_account
import time

# Record the start time
start_time = time.time()

# Load credentials
credentials = service_account.Credentials.from_service_account_file("gsheetpostgres-4ba412f0c6f3.json", scopes=["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"])
client = gspread.Client(auth=credentials)

# Access Google Sheets data
sheet = client.open("University Partnership").worksheet("All Study Program Recap")
data = sheet.get_all_records()

# Record the end time
end_time = time.time()

# Calculate the execution time
execution_time = end_time - start_time

# Print the retrieved data
print("Execution time:", execution_time, "seconds")
print(data)
