import gspread
from google.oauth2 import service_account
import psycopg2

def connect():
    return psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="Javierpedro2"
    )

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

def get_last_synced_row_id():
    conn = connect()
    cursor = conn.cursor()

    # Retrieve the last synchronized row ID from the sync_status table
    cursor.execute("SELECT last_synced_row_id FROM sync_status ORDER BY id DESC LIMIT 1;")
    row = cursor.fetchone()  # Assuming you have only one row
    print("Retrieved row:", row)  # Add this line to debug

    if row is None:
        last_synced_row_id = None
    else:
        last_synced_row_id = row[0]

    cursor.close()
    conn.close()

    return last_synced_row_id

 

def main():
    # Load Google Sheets API credentials
    credentials = service_account.Credentials.from_service_account_file("gsheetpostgres-4ba412f0c6f3.json", scopes=["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"])
    client = gspread.Client(auth=credentials)

    # Access Google Sheets data
    sheet = client.open("University Partnership").worksheet("All Study Program Recap")
    data = sheet.get_all_records()

    # Convert empty strings to None
    converted_data = convert_empty_to_none(data)
    last_synced_row_id = get_last_synced_row_id()

    # Filter new data
    new_data = []
    for record in converted_data:
        if last_synced_row_id is None or record["No"] > last_synced_row_id:
            new_data.append(record)

    # Connect to PostgreSQL
    conn = connect()
    cursor = conn.cursor()

    # Insert new data into PostgreSQL
    for record in new_data:  # Use new_data here, not converted_data
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
        update_last_synced_row_id(record["No"])  # Update after inserting
        conn.commit()

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
