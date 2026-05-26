# Install required packages
# pip install pandas numpy snowflake-connector-python python-dotenv

# Test the transformer standalone with mock data
# python3 - <<'EOF'
import pandas as pd
from transformation.clean_flights import FlightTransformer
from transformation.deduplication import FlightDeduplicator
from transformation.validate import FlightDataValidator

# Create sample data with intentional quality issues
data = {
    "event_id"      : ["E001","E002","E002","E003","E004"],  # E002 is a dup
    "flight_id"     : ["AI101","6E202",None,"SG303","UK404"],
    "airline"       : ["air india","indigo","spicejet","spicejet","vistara"],
    "airline_iata"  : ["AI","6E","SG","SG","UK"],
    "flight_number" : ["101","202","303","303","404"],
    "source_airport": ["DEL","BOM","???","CCU","DEL"],       # ??? is bad
    "dest_airport"  : ["BOM","BLR","HYD","CCU","BOM"],       # CCU→CCU is self-loop
    "latitude"      : [22.3, 15.1, None, 22.6, 28.5],        # None is bad
    "longitude"     : [73.1, 74.9, 78.4, 88.4, 77.1],
    "altitude"      : [35000, 25000, -500, 0, 38000],         # -500 is bad
    "speed"         : [850, 750, 0, -10, 900],                # -10 is bad
    "status"        : ["active","active","IN_AIR","active","landed"],
    "delay_minutes" : [0, 15, None, 45, 0],
    "event_timestamp": ["2024-06-15T10:00:00Z","2024-06-15T10:01:00Z",
                         "2024-06-15T10:02:00Z","2024-06-15T10:03:00Z",
                         "2024-06-15T10:04:00Z"],
    "loaded_at"     : ["2024-06-15T10:05:00Z"] * 5,
}
df = pd.DataFrame(data)

print("=== STEP 1: Validate raw data ===")
validator = FlightDataValidator()
report    = validator.validate(df)
report.print_report()

print("\n=== STEP 2: Deduplicate ===")
deduplicator = FlightDeduplicator()
df_deduped, dedup_stats = deduplicator.deduplicate(df)
print(f"Dedup stats: {dedup_stats}")
print(f"Records: {len(df)} → {len(df_deduped)}")

print("\n=== STEP 3: Transform ===")
transformer = FlightTransformer()
df_clean, transform_stats = transformer.transform(df_deduped)
print(f"Transform stats: {transform_stats}")

print("\n=== CLEAN DATA SAMPLE ===")
cols = ["event_id","flight_id","airline","status","delay_bucket",
        "flight_phase","region","data_quality_flag","transformation_log"]
print(df_clean[[c for c in cols if c in df_clean.columns]].to_string())
EOF

# Expected output shows:
#   E002 duplicate removed
#   ??? airport flagged
#   CCU→CCU self-loop warned
#   None latitude flagged
#   -500 altitude capped to 0 + logged
#   -10 speed capped to 0 + logged
#   IN_AIR normalised to active
#   delay_bucket, flight_phase, region all derived

# Run the full incremental pipeline (needs Snowflake connection)
# python -m transformation.run_transformation --limit 1000 --run-id test_run_001