#!/usr/bin/env python
"""Test PostgreSQL connection."""
import os
import sys
from dotenv import load_dotenv

load_dotenv()

import psycopg2

host = os.getenv("POSTGRES_HOST", "127.0.0.1")
port = int(os.getenv("POSTGRES_PORT", "5432"))
user = os.getenv("POSTGRES_USER", "airflow")
password = os.getenv("POSTGRES_PASSWORD", "airflow")
db = os.getenv("POSTGRES_DB", "airflow")

print(f"Attempting to connect to PostgreSQL:")
print(f"  Host: {host}")
print(f"  Port: {port}")
print(f"  User: {user}")
print(f"  Password: {'*' * len(password) if password else 'None'}")
print(f"  Database: {db}")
print()

try:
    conn = psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname=db,
    )
    print("✓ Connection successful!")
    cursor = conn.cursor()
    cursor.execute("SELECT 1")
    result = cursor.fetchone()
    print(f"✓ Query successful: {result}")
    cursor.close()
    conn.close()
except Exception as e:
    print(f"✗ Connection failed: {e}")
    print(f"  Error type: {type(e).__name__}")
    sys.exit(1)
