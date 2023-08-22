import gspread
from google.auth import exceptions
from google.oauth2 import service_account
import psycopg2
import time

# # Record the start time
# start_time = time.time()


# # Load Google Sheets API credentials
# credentials = service_account.Credentials.from_service_account_file("gsheetpostgres-4ba412f0c6f3.json", scopes=["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"])
# client = gspread.Client(auth=credentials)

# # Access Google Sheets data
# sheet = client.open("University Partnership").worksheet("All Study Program Recap")
# data = sheet.get_all_records()


# # Connect to PostgreSQL
# conn = psycopg2.connect(
#     host="localhost",
#     database="postgres",
#     user="postgres",
#     password="Javierpedro2"
# )
# cursor = conn.cursor()

# # Insert data into PostgreSQL
# for record in data:
#     cursor.execute(
#         "INSERT INTO uni_partner (country, partner_university, faculty_subject, program_type, study_program, start_month, start_year, finish_month, finish_year, status, document_type, file_link, level, notes) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
#         (
#             record["Country"],
#             record["Partner University"],
#             record["Faculty/Subject"],
#             record["Program Type (Exchange/DD/Other)"],
#             record["Study Program"],
#             record["Start Month"],
#             record["Start Year"],
#             record["Finish Month"],
#             record["Finish Year"],
#             record["Status (active/inactive/expired)"],
#             record["Document Type"],
#             record["File"],
#             record["Level"],
#             record["Notes"]
#         )
#     )

# # Record the end time
# end_time = time.time()

# # Calculate the execution time
# execution_time = end_time - start_time

# print("Execution time:", execution_time, "seconds")

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

# Load Google Sheets API credentials
credentials = service_account.Credentials.from_service_account_file("gsheetpostgres-4ba412f0c6f3.json", scopes=["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"])
client = gspread.Client(auth=credentials)

# Access Google Sheets data
sheet = client.open("University Partnership").worksheet("All Study Program Recap")
data = sheet.get_all_records()

# Convert empty strings to None
converted_data = convert_empty_to_none(data)

# Connect to PostgreSQL
conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="Javierpedro2"
)
cursor = conn.cursor()

# Insert data into PostgreSQL
for record in converted_data:
    cursor.execute(
        "INSERT INTO uni_partner (country, partner_university, faculty_subject, program_type, study_program, start_month, start_year, finish_month, finish_year, status, document_type, file_link, level, notes) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (
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
    )

conn.commit()
cursor.close()
conn.close()