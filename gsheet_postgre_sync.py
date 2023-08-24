# Kode ini untuk memindahkan data pada google sheet ke PostgreSQL
# Lalu jika terdapat row terbaru maka yang row terakhir yang hanya dipindahkan ke Postgresql 
# Harapannya agar tidak terdapat duplikasi data yang dimasukkan ke Postgresql --> On Process  

import gspread
from google.oauth2 import service_account
import psycopg2

# Membuat fungsi untuk menghubungkan service ke PostgreSQL
def connect():
    return psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="Javierpedro2"
    )

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

# Fungsi untuk mendapatkan id row terakhir yang dimasukkan ke Postgresql
def get_last_synced_row_id():
    conn = connect()
    cursor = conn.cursor()

    # Mendapatkan row ID terakhir dari tabel sync_status
    cursor.execute("SELECT last_synced_row_id FROM sync_status ORDER BY id DESC LIMIT 1;")
    row = cursor.fetchone()
    print("Retrieved row:", row) 

    if row is None:
        last_synced_row_id = None
    else:
        last_synced_row_id = row[0]

    cursor.close()
    conn.close()

    return last_synced_row_id

# Fungsi untuk update tabel sync_status sehingga data row id terakhir bisa dimasukkan
def update_last_synced_row_id(row_id):
    conn = connect()
    cursor = conn.cursor()

    cursor.execute("UPDATE sync_status SET last_synced_row_id = %s;", (row_id,))
    conn.commit()  # Commit the changes

    cursor.close()
    conn.close()

# Fungsi utama
def main():
    # Menghubungkan service ke Google Sheet API
    credentials = service_account.Credentials.from_service_account_file("gsheetpostgres-4ba412f0c6f3.json", scopes=["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"])
    client = gspread.Client(auth=credentials)

    # Mengakses data pada Google Sheet
    sheet = client.open("University Partnership").worksheet("All Study Program Recap")
    data = sheet.get_all_records()

    # Mengonversi data yang kosong menjadi None
    converted_data = convert_empty_to_none(data)
    last_synced_row_id = get_last_synced_row_id()

    # Filter new data
    new_data = []
    for record in converted_data:
        print("Record['No']:", record["No"])
        if last_synced_row_id is None or record["No"] > last_synced_row_id:
            new_data.append(record)

    # Menghubungkan Koneksi ke Postgresql
    conn = connect()
    cursor = conn.cursor()

   # Memasukkan data ke Postgresql
    for record in new_data:
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
        update_last_synced_row_id(record["No"])  # Melakukan update tabel setelah inserting
        conn.commit()

    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
