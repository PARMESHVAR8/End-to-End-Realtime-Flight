# start_pipeline.ps1

# Run with:

# PowerShell: .\start_pipeline.ps1

Write-Host "Starting Flight Pipeline..." -ForegroundColor Cyan

# Step 1: Start Docker services

Write-Host "`n[1/6] Starting Docker services..." -ForegroundColor Yellow
docker compose up -d
Start-Sleep -Seconds 35

# Step 2: Check all services

Write-Host "`n[2/6] Checking service status..." -ForegroundColor Yellow
docker compose ps

# Step 3: Check Kafka

Write-Host "`n[3/6] Checking Kafka..." -ForegroundColor Yellow
$kafkaStatus = docker compose ps kafka --format "{{.Status}}"

if ($kafkaStatus -like "*healthy*" -or $kafkaStatus -like "*Up*") {
Write-Host "Kafka is running" -ForegroundColor Green
}
else {
Write-Host "Kafka is NOT running" -ForegroundColor Red
docker logs flight_kafka --tail 20
exit 1
}

# Step 4: Create Kafka topics

# Step 4: Create Kafka topics
Write-Host "`n[4/6] Creating Kafka topics..." -ForegroundColor Yellow

docker exec flight_kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic flights_raw --partitions 3 --replication-factor 1

docker exec flight_kafka kafka-topics --bootstrap-server localhost:9092 --create --if-not-exists --topic flights_clean --partitions 3 --replication-factor 1

Write-Host "Topics created:" -ForegroundColor Green

docker exec flight_kafka kafka-topics --bootstrap-server localhost:9092 --list

# Step 5: Create PostgreSQL tables

Write-Host "`n[5/6] Creating PostgreSQL staging tables..." -ForegroundColor Yellow

$sql = @'
CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.flights_raw (
id SERIAL PRIMARY KEY,
flight_id VARCHAR(50),
airline VARCHAR(100),
flight_number VARCHAR(20),
source_airport VARCHAR(10),
dest_airport VARCHAR(10),
altitude INTEGER,
speed FLOAT,
latitude FLOAT,
longitude FLOAT,
status VARCHAR(30),
raw_payload JSONB,
ingested_at TIMESTAMP DEFAULT NOW(),
processed BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS staging.pipeline_runs (
id SERIAL PRIMARY KEY,
run_id VARCHAR(100),
dag_id VARCHAR(100),
start_time TIMESTAMP,
end_time TIMESTAMP,
records_read INTEGER DEFAULT 0,
records_written INTEGER DEFAULT 0,
status VARCHAR(20),
error_message TEXT
);

CREATE TABLE IF NOT EXISTS staging.dead_letter_queue (
id SERIAL PRIMARY KEY,
event_id VARCHAR(100),
flight_id VARCHAR(50),
source VARCHAR(100),
dag_id VARCHAR(100),
run_id VARCHAR(200),
error_message TEXT,
retry_count INTEGER DEFAULT 0,
raw_payload JSONB,
created_at TIMESTAMP DEFAULT NOW(),
status VARCHAR(30) DEFAULT 'pending'
);

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA staging TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA staging TO airflow;

SELECT 'PostgreSQL ready' AS result;
'@

docker exec -i flight_postgres psql `    -U airflow`
-d airflow `
-c $sql

# Step 6: Test Python connections

Write-Host "`n[6/6] Testing Python connections..." -ForegroundColor Yellow

$pythonCode = @'
import psycopg2
from dotenv import load_dotenv
from kafka import KafkaAdminClient

load_dotenv()

print("\n--- PostgreSQL Test ---")

for port in [5433, 5432]:
try:
conn = psycopg2.connect(
host="127.0.0.1",
port=port,
user="airflow",
password="airflow",
dbname="airflow",
connect_timeout=3
)

```
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM staging.flights_raw")
    rows = cur.fetchone()[0]

    print(f"PostgreSQL OK on port {port} - rows: {rows}")

    cur.close()
    conn.close()
    break

except Exception as e:
    print(f"PostgreSQL port {port} failed: {e}")
```

print("\n--- Kafka Test ---")

try:
admin = KafkaAdminClient(
bootstrap_servers="localhost:9092",
request_timeout_ms=5000
)

```
topics = list(admin.list_topics())

print(f"Kafka OK - topics: {topics}")

admin.close()
```

except Exception as e:
print(f"Kafka failed: {e}")
'@

python -c $pythonCode

Write-Host "`nPipeline ready!" -ForegroundColor Green

Write-Host "`nRun these in separate terminals:" -ForegroundColor Cyan

Write-Host "Terminal 1:" -ForegroundColor White
Write-Host "python -m flight_kafka.producer --source simulator --interval 3"

Write-Host ""

Write-Host "Terminal 2:" -ForegroundColor White
Write-Host "python -m flight_kafka.consumer"

Write-Host ""

Write-Host "Dashboard:" -ForegroundColor White
Write-Host "http://localhost:8501"

Write-Host ""

Write-Host "Airflow:" -ForegroundColor White
Write-Host "http://localhost:8080"
Write-Host "Username: admin"
Write-Host "Password: admin"
