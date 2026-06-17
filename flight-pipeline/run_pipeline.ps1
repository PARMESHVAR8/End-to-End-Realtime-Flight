# run_pipeline.ps1 — Run this every time you start the project

Write-Host "=== Flight Pipeline Startup ===" -ForegroundColor Cyan

# 1. Start Docker
Write-Host "`n[1/4] Starting Docker services..." -ForegroundColor Yellow
docker compose up -d
Start-Sleep -Seconds 35
docker compose ps

# 2. Check PostgreSQL and create tables if needed
Write-Host "`n[2/4] Setting up PostgreSQL..." -ForegroundColor Yellow
python -c "
import psycopg2
for port in [5433, 5432]:
    try:
        c = psycopg2.connect(host='127.0.0.1',port=port,
            user='airflow',password='airflow',dbname='airflow',connect_timeout=3)
        c.autocommit=True; cur=c.cursor()
        cur.execute('CREATE SCHEMA IF NOT EXISTS staging')
        cur.execute('''CREATE TABLE IF NOT EXISTS staging.flights_raw(
            id SERIAL PRIMARY KEY,flight_id VARCHAR(50),airline VARCHAR(100),
            flight_number VARCHAR(20),source_airport VARCHAR(10),dest_airport VARCHAR(10),
            altitude INTEGER,speed FLOAT,latitude FLOAT,longitude FLOAT,
            status VARCHAR(30),raw_payload JSONB,
            ingested_at TIMESTAMP DEFAULT NOW(),processed BOOLEAN DEFAULT FALSE)''')
        cur.execute('GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA staging TO airflow')
        cur.execute('GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA staging TO airflow')
        cur.execute('SELECT COUNT(*) FROM staging.flights_raw')
        print(f'PostgreSQL ready on port {port} | rows={cur.fetchone()[0]}')
        c.close(); break
    except Exception as e: print(f'Port {port}: {str(e)[:50]}')
"

# 3. Create Kafka topics
Write-Host "`n[3/4] Creating Kafka topics..." -ForegroundColor Yellow
docker exec flight_kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic flights_raw --partitions 3 --replication-factor 1 2>$null
docker exec flight_kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic flights_clean --partitions 3 --replication-factor 1 2>$null
Write-Host "Topics: $(docker exec flight_kafka kafka-topics --bootstrap-server localhost:9092 --list)" -ForegroundColor Green

# 4. Open browsers
Write-Host "`n[4/4] Opening service URLs..." -ForegroundColor Yellow
Start-Process "http://localhost:8080"   # Airflow
Start-Process "http://localhost:8501"   # Dashboard
Start-Process "http://localhost:8085"   # Kafka UI

Write-Host "`n=== Ready! Now run in separate terminals: ===" -ForegroundColor Green
Write-Host "Terminal 1: python -m flight_kafka.producer --source simulator --interval 3" -ForegroundColor White
Write-Host "Terminal 2: python -m flight_kafka.consumer" -ForegroundColor White
Write-Host "Airflow:    http://localhost:8080  (trigger DAGs manually)" -ForegroundColor White
Write-Host "Dashboard:  http://localhost:8501" -ForegroundColor White

# .\run_pipeline.ps1