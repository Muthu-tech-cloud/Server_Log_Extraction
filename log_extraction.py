import re
from datetime import datetime
from pymongo import MongoClient
import sqlite3
from constants import *  # External config for MongoDB URI and DB/Collection names

# ------------------------------
# STEP 1: Connect to MongoDB
# ------------------------------
try:
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    print("Connected to MongoDB successfully.")
except Exception as e:
    print(f"MongoDB Connection Error: {e}")
    exit()

# ------------------------------
# STEP 2: Read and Parse Email Log File
# ------------------------------
try:
    with open(file_path, "r", encoding="utf-8") as f:
        text = f.read()
except FileNotFoundError:
    print("Log file not found. Please check the path.")
    exit()

# Email and Timestamp regex
email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
timestamp_pattern = r'\b[A-Za-z]{3} \d{1,2} \d{2}:\d{2}:\d{2} \d{4}\b'

# Extract matches from log
matches = re.findall(rf'({email_pattern}).*?({timestamp_pattern})', text)

email_timestamp_data = []
for match in matches:
    email, timestamp = match
    try:
        formatted_timestamp = datetime.strptime(timestamp, "%b %d %H:%M:%S %Y").strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        formatted_timestamp = timestamp
    email_timestamp_data.append({"email": email, "timestamp": formatted_timestamp})

# ------------------------------
# STEP 3: Insert Cleaned Data into MongoDB
# ------------------------------
if email_timestamp_data:
    collection.insert_many(email_timestamp_data)
    print(f"{len(email_timestamp_data)} records inserted into MongoDB.")
else:
    print("No valid data found to insert.")
    exit()

# ------------------------------
# STEP 4: Transfer Data to SQLite
# ------------------------------
documents = list(collection.find({}, {"_id": 0}))  # Exclude MongoDB _id field

sqlite_conn = sqlite3.connect("user_logs.db")
cursor = sqlite_conn.cursor()

# Create table
cursor.execute('''
    CREATE TABLE IF NOT EXISTS user_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT NOT NULL,
        date TEXT NOT NULL
    );
''')

# Insert into SQLite
for doc in documents:
    try:
        email = doc['email']
        date = doc['timestamp']  # match with table schema
        cursor.execute("INSERT INTO user_history (email, date) VALUES (?, ?)", (email, date))
    except Exception as e:
        print(f"Error inserting record {doc}: {e}")

sqlite_conn.commit()
sqlite_conn.close()
client.close()
print("Data successfully transferred to SQLite.")

# ------------------------------
# STEP 5: Perform Analysis on SQLite Data
# ------------------------------
sqlite_conn = sqlite3.connect("user_logs.db")
cursor = sqlite_conn.cursor()

print("\n--- Unique Email Addresses ---")
cursor.execute("SELECT DISTINCT email FROM user_history;")
for row in cursor.fetchall():
    print(row[0])

print("\n--- Email Count Per Day ---")
cursor.execute("""
    SELECT DATE(date) AS email_date, COUNT(*) AS email_count
    FROM user_history
    GROUP BY email_date
    ORDER BY email_date;
""")
for row in cursor.fetchall():
    print(f"{row[0]}: {row[1]} emails")

print("\n--- First and Last Email Date per Address ---")
cursor.execute("""
    SELECT email, MIN(date) AS first_email, MAX(date) AS last_email
    FROM user_history
    GROUP BY email;
""")
for row in cursor.fetchall():
    print(f"{row[0]} -> First: {row[1]}, Last: {row[2]}")

print("\n--- Email Count by Domain ---")
cursor.execute("""
    SELECT SUBSTR(email, INSTR(email, '@') + 1) AS domain, COUNT(*) AS total_emails
    FROM user_history
    GROUP BY domain
    ORDER BY total_emails DESC;
""")
for row in cursor.fetchall():
    print(f"{row[0]}: {row[1]} emails")

# Cleanup
sqlite_conn.close()
