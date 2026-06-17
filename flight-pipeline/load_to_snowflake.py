# load_to_snowflake.py — project root mein save karo
"""
Har 5 minute mein PostgreSQL se fresh data Snowflake mein load karta hai.
Ye Airflow DAG ke bina kaam karta hai.
Run karo: python load_to_snowflake.py
"""
import time
import json
import os
import psycopg2
import snowflake.connector
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

def load_batch():
    print(f"\n[{datetime.now().strftime('%H:%M:%S')}] Loading fresh data to Snowflake...")

    # PostgreSQL se fresh unprocessed data lo
    try:
        pg = psycopg2.connect(
            host='127.0.0.1', port=5433,
            user='airflow', password='airflow', dbname='airflow'
        )
        cur = pg.cursor()
        cur.execute("""
            SELECT id, raw_payload, ingested_at
            FROM staging.flights_raw
            WHERE processed = FALSE
            ORDER BY ingested_at ASC
            LIMIT 500
        """)
        rows = cur.fetchall()
        print(f"  PostgreSQL: {len(rows)} unprocessed rows found")
    except Exception as e:
        print(f"  PostgreSQL error: {e}")
        return

    if not rows:
        print("  No new data to load")
        pg.close()
        return

    # Snowflake connect
    try:
        snow = snowflake.connector.connect(
            account   = os.getenv('SNOWFLAKE_ACCOUNT'),
            user      = os.getenv('SNOWFLAKE_USER'),
            password  = os.getenv('SNOWFLAKE_PASSWORD'),
            database  = 'FLIGHT_DB',
            warehouse = 'FLIGHT_WH',
            role      = 'ACCOUNTADMIN',
            login_timeout = 30,
        )
        scur = snow.cursor()
    except Exception as e:
        print(f"  Snowflake connect error: {e}")
        pg.close()
        return

    # Insert each record
    inserted = 0
    now = datetime.now(timezone.utc)

    for row_id, raw_payload, ingested_at in rows:
        try:
            if isinstance(raw_payload, str):
                p = json.loads(raw_payload)
            elif isinstance(raw_payload, dict):
                p = raw_payload
            else:
                continue

            event_id = p.get('event_id', str(row_id))

            # RAW insert
            scur.execute("""
                INSERT INTO FLIGHT_DB.RAW.FLIGHTS_RAW (
                    event_id, flight_id, airline, airline_iata, flight_number,
                    source_airport, dest_airport, source_city, dest_city,
                    latitude, longitude, altitude, speed, heading,
                    status, delay_minutes, aircraft_type,
                    event_timestamp, source, raw_json, loaded_at
                )
                SELECT %s,%s,%s,%s,%s,%s,%s,%s,%s,
                       %s,%s,%s,%s,%s,%s,%s,%s,
                       CURRENT_TIMESTAMP(),%s,PARSE_JSON(%s),CURRENT_TIMESTAMP()
                WHERE NOT EXISTS (
                    SELECT 1 FROM FLIGHT_DB.RAW.FLIGHTS_RAW WHERE event_id=%s
                )
            """, (
                event_id,
                p.get('flight_id',''),
                p.get('airline',''),
                p.get('airline_iata',''),
                p.get('flight_number',''),
                p.get('source_airport',''),
                p.get('dest_airport',''),
                p.get('source_city',''),
                p.get('dest_city',''),
                float(p.get('latitude') or 0),
                float(p.get('longitude') or 0),
                int(p.get('altitude') or 0),
                float(p.get('speed') or 0),
                float(p.get('heading') or 0),
                p.get('status','active'),
                int(p.get('delay_minutes') or 0),
                p.get('aircraft_type',''),
                p.get('source','simulator'),
                json.dumps(p),
                event_id,
            ))
            inserted += 1
        except Exception:
            pass

    snow.commit()
    print(f"  RAW: {inserted} records inserted")

    # CLEAN mein transform
    try:
        scur.execute("""
            INSERT INTO FLIGHT_DB.CLEAN.FLIGHTS_CLEAN (
                event_id, flight_id, airline, airline_iata, flight_number,
                source_airport, dest_airport, source_city, dest_city,
                latitude, longitude, altitude, speed, heading, status,
                delay_minutes, aircraft_type, event_timestamp, source,
                is_international, delay_bucket, flight_phase, region,
                route_key, data_quality_flag, transformed_at
            )
            SELECT
                r.event_id, r.flight_id,
                INITCAP(TRIM(r.airline)),
                UPPER(TRIM(COALESCE(r.airline_iata,''))),
                r.flight_number,
                UPPER(TRIM(r.source_airport)),
                UPPER(TRIM(r.dest_airport)),
                r.source_city, r.dest_city,
                COALESCE(r.latitude,0), COALESCE(r.longitude,0),
                GREATEST(COALESCE(r.altitude,0),0),
                GREATEST(LEAST(COALESCE(r.speed,0),1200),0),
                r.heading,
                LOWER(COALESCE(r.status,'active')),
                GREATEST(COALESCE(r.delay_minutes,0),0),
                r.aircraft_type,
                CURRENT_TIMESTAMP(),
                COALESCE(r.source,'simulator'),
                CASE WHEN UPPER(r.source_airport) IN (
                    'DEL','BOM','BLR','MAA','CCU','HYD','AMD','COK','GOI','JAI'
                ) AND UPPER(r.dest_airport) IN (
                    'DEL','BOM','BLR','MAA','CCU','HYD','AMD','COK','GOI','JAI'
                ) THEN FALSE ELSE TRUE END,
                CASE
                    WHEN COALESCE(r.delay_minutes,0)=0 THEN 'on_time'
                    WHEN r.delay_minutes<=15 THEN 'minor_delay'
                    WHEN r.delay_minutes<=60 THEN 'moderate_delay'
                    WHEN r.delay_minutes<=180 THEN 'major_delay'
                    ELSE 'severe_delay'
                END,
                CASE
                    WHEN COALESCE(r.altitude,0)<1000 THEN 'ground'
                    WHEN r.altitude<15000 THEN 'climbing'
                    WHEN r.altitude<32000 THEN 'mid_altitude'
                    ELSE 'cruise'
                END,
                CASE
                    WHEN r.latitude BETWEEN 8 AND 37
                    AND r.longitude BETWEEN 68 AND 97 THEN 'India'
                    WHEN r.latitude>=35 THEN 'Europe_Asia'
                    WHEN r.latitude<=0 THEN 'Southern'
                    ELSE 'Middle_East_SE_Asia'
                END,
                UPPER(TRIM(r.source_airport))||'→'||UPPER(TRIM(r.dest_airport)),
                FALSE,
                CURRENT_TIMESTAMP()
            FROM FLIGHT_DB.RAW.FLIGHTS_RAW r
            WHERE r.is_transformed = FALSE
            AND NOT EXISTS (
                SELECT 1 FROM FLIGHT_DB.CLEAN.FLIGHTS_CLEAN c
                WHERE c.event_id = r.event_id
            )
        """)
        snow.commit()

        # FACT_FLIGHTS
        scur.execute("""
            INSERT INTO FLIGHT_DB.ANALYTICS.FACT_FLIGHTS (
                event_id, flight_id, airline_iata,
                source_airport, dest_airport, route_key,
                latitude, longitude, altitude, speed, status,
                delay_minutes, delay_bucket, flight_phase,
                is_international, region, data_quality_flag,
                event_date, event_hour, day_of_week,
                event_timestamp, transformed_at
            )
            SELECT
                c.event_id, c.flight_id, c.airline_iata,
                c.source_airport, c.dest_airport, c.route_key,
                c.latitude, c.longitude, c.altitude, c.speed, c.status,
                c.delay_minutes, c.delay_bucket, c.flight_phase,
                c.is_international, c.region, c.data_quality_flag,
                CURRENT_DATE(),
                HOUR(CURRENT_TIMESTAMP()),
                DAYOFWEEK(CURRENT_TIMESTAMP()),
                CURRENT_TIMESTAMP(),
                CURRENT_TIMESTAMP()
            FROM FLIGHT_DB.CLEAN.FLIGHTS_CLEAN c
            WHERE NOT EXISTS (
                SELECT 1 FROM FLIGHT_DB.ANALYTICS.FACT_FLIGHTS f
                WHERE f.event_id = c.event_id
            )
            AND c.data_quality_flag = FALSE
        """)
        snow.commit()

        # Mark RAW as transformed
        scur.execute("""
            UPDATE FLIGHT_DB.RAW.FLIGHTS_RAW
            SET is_transformed=TRUE, transformed_at=CURRENT_TIMESTAMP()
            WHERE is_transformed=FALSE
        """)
        snow.commit()

        # Get counts
        scur.execute("SELECT COUNT(*) FROM FLIGHT_DB.ANALYTICS.FACT_FLIGHTS WHERE event_date=CURRENT_DATE()")
        fact_count = scur.fetchone()[0]
        print(f"  FACT_FLIGHTS today: {fact_count} records")

    except Exception as e:
        print(f"  Transform error: {e}")

    # Mark PostgreSQL as processed
    try:
        ids = [str(r[0]) for r in rows]
        cur.execute(f"UPDATE staging.flights_raw SET processed=TRUE WHERE id IN ({','.join(ids)})")
        pg.commit()
    except Exception as e:
        print(f"  PG update error: {e}")

    scur.close()
    snow.close()
    cur.close()
    pg.close()
    print(f"  Done! Dashboard refresh karo: http://localhost:8501")


# Main loop
print("Snowflake Loader started! Har 5 minute mein data load hoga.")
print("Ctrl+C se band karo.")
print()

load_batch()  # Pehli baar turant run karo

while True:
    print(f"\nNext load in 5 minutes... (Ctrl+C to stop)")
    time.sleep(300)  # 5 minutes
    load_batch()