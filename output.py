import psycopg2
from pprint import PrettyPrinter

conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="Javierpedro2"
)
cursor = conn.cursor()

# Execute a SELECT query
cursor.execute("SELECT company, job_description, qualification FROM job_opportunities")
rows = cursor.fetchall()

# Use PrettyPrinter to format the output
pp = PrettyPrinter(width=100)
pp.pprint(rows)

cursor.close()
conn.close()
