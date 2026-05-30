#!/usr/bin/env python
"""Test PostgreSQL connection - try postgres user."""
import psycopg2

print("Test 1: Connecting as postgres user (no password)...")
try:
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        user="postgres",
        password="",
        dbname="postgres",
    )
    print("✓ Connected as postgres!")
    cursor = conn.cursor()
    
    # List users
    cursor.execute("SELECT usename FROM pg_user;")
    users = cursor.fetchall()
    print(f"✓ Users in database: {[u[0] for u in users]}")
    
    cursor.close()
    conn.close()
except Exception as e:
    print(f"✗ Failed: {e}")

print("\nTest 2: Connecting as airflow user with password 'airflow'...")
try:
    conn = psycopg2.connect(
        host="127.0.0.1",
        port=5432,
        user="airflow",
        password="airflow",
        dbname="airflow",
    )
    print("✓ Connected as airflow!")
    conn.close()
except Exception as e:
    print(f"✗ Failed: {e}")
