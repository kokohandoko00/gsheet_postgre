## Kode ini untuk memindahkan seluruh row data pada google sheet ke PostgreSQL --> Success

import gspread
from google.auth import exceptions
from google.oauth2 import service_account
import psycopg2
from psycopg2 import extras
import time

# Fungsi untuk mengonversi data yang kosong menjadi berisi None
def convert_empty_to_none(data):
    converted_data = []
    for record in data:
        converted_record = {}
        for key, value in record.items():
            if value == "":
                converted_record[key] = None
            else:
                converted_record[key] = value
        converted_data.append(converted_record)
    return converted_data

# Record the start time
start_time = time.time()

# Menghubungkan service ke Google Sheet API
credentials = service_account.Credentials.from_service_account_file("gsheetpostgres-4ba412f0c6f3.json", scopes=["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"])
client = gspread.Client(auth=credentials)

# Mengakses data pada Google Sheet
sheet = client.open("University Partnership").worksheet("All Study Program Recap")
data = sheet.get_all_records()

# Mengonversi data yang kosong menjadi None
converted_data = convert_empty_to_none(data)

# Menghubungkan Koneksi ke Postgresql
conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="Javierpedro2"
)
cursor = conn.cursor()

# Prepare the data for batch insertion
batch_data = [
    (
        record["No"],
        record["Country"],
        record["Partner University"],
        record["Faculty/Subject"],
        record["Program Type (Exchange/DD/Other)"],
        record["Study Program"],
        record["Start Month"],
        record["Start Year"],
        record["Finish Month"],
        record["Finish Year"],
        record["Status (active/inactive/expired)"],
        record["Document Type"],
        record["File"],
        record["Level"],
        record["Notes"]
    )
    for record in converted_data
]

# Define the SQL query for batch insertion
insert_query = """
    INSERT INTO uni_partner (
        No, country, partner_university, faculty_subject, program_type,
        study_program, start_month, start_year, finish_month,
        finish_year, status, document_type, file_link, level, notes
    )
    VALUES %s
"""

# Set the page_size to match the number of rows you have (82)
page_size = 82

# Execute the batch insertion
extras.execute_values(cursor, insert_query, batch_data, template=None, page_size=page_size)


# Record the end time
end_time = time.time()

# Calculate the execution time
execution_time = end_time - start_time

conn.commit()
cursor.close()
conn.close()