# Server_Log_Extraction

This script extracts email addresses and timestamps from a log file, stores them in MongoDB, then transfers and analyzes them in SQLite.

## Steps:
1. Extract emails & timestamps from `mbox.txt`.
2. Store raw data in MongoDB.
3. Migrate to SQLite (`user_logs.db`).
4. Analyze using SQL queries.

## Notes:
- Create your own `constants.py` with MongoDB credentials.
- `constants.py` is **not included** for security.
- MongoDB URI format: `MONGO_URI = "your_connection_string"`

## Run:
```bash
python log_extraction.py
