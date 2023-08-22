import gspread
from google.auth import exceptions
from google.oauth2 import service_account
import psycopg2

# Load Google Sheets API credentials
credentials = service_account.Credentials.from_service_account_file("gsheetpostgres-4ba412f0c6f3.json", scopes=["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"])
client = gspread.Client(auth=credentials)

# Access Google Sheets data
sheet = client.open("Daftar Lamaran Kerja").sheet1
data = sheet.get_all_records()

# Connect to PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="Javierpedro2"
)
cursor = conn.cursor()

# Insert data into PostgreSQL
for record in data:
    cursor.execute(
        "INSERT INTO job_opportunities (company, company_description, job_description, qualification, link, cv_used, status) VALUES (%s, %s, %s, %s, %s, %s, %s)",
        (record["Company"], record["Company Description "], record["Job Description"], record["Qualification"], record["Link"], record["CV_used"], record["Status"])
    )

conn.commit()
cursor.close()
conn.close()
