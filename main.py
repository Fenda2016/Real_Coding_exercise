import csv
import os
import psycopg2
from datetime import datetime

# Connect to the database 
conn = psycopg2.connect(
    port=int(os.environ.get("DB_PORT", 5432)),
    host=os.environ.get("DB_HOST"),
    database=os.environ.get("DB_NAME"),
    user=os.environ.get("DB_USER"),
    password=os.environ.get("DB_PASSWORD"),
)
directory = os.environ.get("DIRECTORY")
cursor = conn.cursor()
"""OPEN THE FILES UNDER THE DIRECTORY"""
import concurrent.futures

# Get the list of files in the directory
files = [os.path.join(directory, file) for file in os.listdir(directory) if file.endswith('.csv')]

# Function to process a CSV file
def process_csv(file):
    file_path = os.path.join(directory, file)
    with open(file_path, 'r') as file:
        try:
            reader = csv.reader(file)
            next(reader)  # Skip the header row
            for row in reader:
                try:
                    # Check if row[0] is in ISO 8601 format
                    datetime.fromisoformat(row[0])
                    # Check if row[2] is numeric
                    if not row[2].isnumeric(): 
                        raise ValueError(f"Non-numeric value found: {row[2]}")
                    # Insert the data into the database
                    cursor.execute('INSERT INTO newtable (timestamp, sensorname, value) VALUES (%s, %s, %s) ON CONFLICT(timestamp,sensorname) DO UPDATE SET value=excluded.value', (row[0], row[1], row[2]))
                except ValueError as ve:
                    print("Value Error occured: ", ve)
        except Exception as e:
            print(f"Error reading CSV file: {e}")

# Process the CSV files in parallel
with concurrent.futures.ThreadPoolExecutor() as executor:
    executor.map(process_csv, files)
# Commit the changes and close the connection
conn.commit()
conn.close()