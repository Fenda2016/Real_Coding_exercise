import csv
import sqlite3
import os
import psycopg2

"""we have a DB table for three colums"""
"""read the CSV files and insert the data into the Database"""
# Connect to the database
conn = psycopg2.connect(
    host="your_host",
    database="your_database",
    user="your_user",
    password="your_password"
)

cursor = conn.cursor()
with open('data.csv', 'r') as file:
    reader = csv.reader(file)
    next(reader)  # Skip the header row
    for row in reader:
        # Insert the data into the database
        cursor.execute('INSERT INTO table_name (column1, column2, column3) VALUES (?, ?, ?)', row)

# Commit the changes and close the connection
conn.commit()
conn.close()