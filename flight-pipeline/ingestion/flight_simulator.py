# ingestion/flight_simulator.py
"""
Flight Data Simulator
=====================
Generates realistic fake flight data for pipeline testing.
Simulates real-world patterns: delays, route corridors, altitude profiles.

WHY THIS EXISTS:
  - API rate limits prevent continuous testing
  - Lets us inject controlled "bad" data to test validation
  - Can run 24/7 without using API credits
  - Simulates edge cases: cancelled flights, extreme delays, null values

USAGE:
  from ingestion.flight_simulator import FlightSimulator
  sim = FlightSimulator()
  event = sim.generate_flight_event()
"""

import uuid
import random
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

# ─── Logger setup ────────────────────────────────────────────────────────────
# We use Python's built-in logging instead of print()
# Reason: logging has levels (DEBUG/INFO/WARNING/ERROR), timestamps,
# and can be redirected to files or monitoring systems
logger = logging.getLogger(__name__)


# ─── Realistic reference data ─────────────────────────────────────────────────
# These are real Indian and international airports with real coordinates.
# Using real lat/long means our Streamlit map will show flights in the right places.

AIRPORTS = {
    # Code  : (Name,              City,          Lat,      Long,     Country)
    "DEL"  : ("Indira Gandhi Intl", "Delhi",     28.5562,  77.1000,  "IN"),
    "BOM"  : ("Chhatrapati Shivaji", "Mumbai",   19.0896,  72.8656,  "IN"),
    "BLR"  : ("Kempegowda Intl",   "Bangalore", 13.1979,  77.7063,  "IN"),
    "MAA"  : ("Chennai Intl",      "Chennai",   12.9900,  80.1693,  "IN"),
    "CCU"  : ("Netaji Subhash",    "Kolkata",   22.6547,  88.4467,  "IN"),
    "HYD"  : ("Rajiv Gandhi Intl", "Hyderabad", 17.2403,  78.4294,  "IN"),
    "AMD"  : ("Sardar Vallabhbhai","Ahmedabad", 23.0775,  72.6347,  "IN"),
    "COK"  : ("Cochin Intl",       "Kochi",     10.1520,  76.4019,  "IN"),
    "GOI"  : ("Goa Intl",          "Goa",       15.3808,  73.8314,  "IN"),
    "JAI"  : ("Jaipur Intl",       "Jaipur",    26.8242,  75.8122,  "IN"),
    # International
    "DXB"  : ("Dubai Intl",        "Dubai",     25.2532,  55.3657,  "AE"),
    "SIN"  : ("Changi",            "Singapore", 1.3644,   103.9915, "SG"),
    "LHR"  : ("Heathrow",          "London",    51.4775,  -0.4614,  "GB"),
    "JFK"  : ("John F Kennedy",    "New York",  40.6413,  -73.7781, "US"),
    "BKK"  : ("Suvarnabhumi",      "Bangkok",   13.6900,  100.7501, "TH"),
    "KUL"  : ("KLIA",              "Kuala Lumpur",2.7456, 101.7072, "MY"),
    "CDG"  : ("Charles de Gaulle", "Paris",     49.0097,   2.5479,  "FR"),
    "FRA"  : ("Frankfurt Intl",    "Frankfurt", 50.0333,   8.5706,  "DE"),
}

AIRLINES = [
    # (Name,            IATA, Typical fleet)
    ("Air India",       "AI",  ["B788", "B789", "A320", "A321"]),
    ("IndiGo",          "6E",  ["A320", "A321", "ATR72"]),
    ("SpiceJet",        "SG",  ["B738", "B737", "Q400"]),
    ("Vistara",         "UK",  ["A320", "A321", "B787"]),
    ("GoFirst",         "G8",  ["A320", "A319"]),
    ("Emirates",        "EK",  ["B777", "A380", "B788"]),
    ("Singapore Air",   "SQ",  ["A350", "B787", "A380"]),
    ("British Airways", "BA",  ["B777", "A320", "B787"]),
    ("Qatar Airways",   "QR",  ["A350", "B787", "A380"]),
    ("Air Arabia",      "G9",  ["A320", "A321"]),
]

# Common India routes with realistic flight durations (minutes)
ROUTES = [
    ("DEL", "BOM", 120), ("BOM", "DEL", 120),
    ("DEL", "BLR", 165), ("BLR", "DEL", 165),
    ("DEL", "MAA", 175), ("MAA", "DEL", 175),
    ("BOM", "BLR", 90),  ("BLR", "BOM", 90),
    ("DEL", "CCU", 135), ("CCU", "DEL", 135),
    ("DEL", "HYD", 140), ("HYD", "DEL", 140),
    ("BOM", "GOI", 60),  ("GOI", "BOM", 60),
    ("BLR", "COK", 55),  ("COK", "BLR", 55),
    ("DEL", "JAI", 45),  ("JAI", "DEL", 45),
    ("DEL", "DXB", 225), ("DXB", "DEL", 210),
    ("BOM", "DXB", 195), ("DXB", "BOM", 195),
    ("DEL", "SIN", 360), ("SIN", "DEL", 360),
    ("BOM", "LHR", 555), ("LHR", "BOM", 540),
    ("DEL", "CDG", 510), ("CDG", "DEL", 495),
]


class FlightSimulator:
    """
    Generates realistic flight data events.

    Design decisions:
      - Stateful: tracks "active" flights so the same flight_id
        produces consistent updates (altitude rising → cruising → descending)
      - Configurable error injection for testing data quality checks
      - Reproducible: pass a seed for deterministic output in tests
    """

    def __init__(
        self,
        inject_errors: bool = False,
        error_rate: float = 0.05
    ):
        """
        Args:
            inject_errors : If True, randomly injects bad/missing data (5% by default)
                            Use this to test your validation pipeline
            error_rate    : Fraction of records that will have injected errors
        """
        self.inject_errors = inject_errors
        self.error_rate = error_rate
        # Track active flights: flight_id → flight state dict
        # This lets us simulate a flight progressing through its journey
        self._active_flights: dict = {}
        logger.info(
            "FlightSimulator initialised | inject_errors=%s | error_rate=%.0f%%",
            inject_errors, error_rate * 100
        )

    # ─── Public API ──────────────────────────────────────────────────────────

    def generate_flight_event(self) -> dict:
        """
        Generate one flight event.
        Returns a dict that matches our flight_event.json schema.

        Either continues an existing active flight or starts a new one.
        ~70% chance to continue an existing flight (realistic — skies are busy).
        """
        # If we have active flights and random dice says "continue existing"
        if self._active_flights and random.random() < 0.70:
            flight_id = random.choice(list(self._active_flights.keys()))
            event = self._update_existing_flight(flight_id)
        else:
            # Start a brand-new flight
            event = self._create_new_flight()

        # Optionally corrupt the data to test validation
        if self.inject_errors and random.random() < self.error_rate:
            event = self._inject_error(event)
            logger.debug("Injected error into event %s", event.get("event_id"))

        return event

    def generate_batch(self, count: int) -> list[dict]:
        """Generate multiple flight events at once."""
        return [self.generate_flight_event() for _ in range(count)]

    # ─── Private helpers ─────────────────────────────────────────────────────

    def _create_new_flight(self) -> dict:
        """Initialise a brand-new flight and store its state."""
        # Pick random route
        src, dst, duration_mins = random.choice(ROUTES)
        src_data = AIRPORTS[src]
        dst_data = AIRPORTS[dst]

        # Pick random airline
        airline_name, airline_iata, fleet = random.choice(AIRLINES)

        # Build flight_id: airline code + 3–4 digit number
        flight_number = str(random.randint(100, 9999))
        flight_id = f"{airline_iata}{flight_number}"

        # Scheduled times: depart between 0–23h ago, arrive later
        now = datetime.now(timezone.utc)
        # Depart anywhere from 3h ago to 30 mins ago
        depart_offset = timedelta(minutes=random.randint(30, 180))
        departure_time = now - depart_offset
        arrival_time = departure_time + timedelta(minutes=duration_mins)

        # Delay: 60% on time, 30% slightly delayed, 10% significantly delayed
        delay_roll = random.random()
        if delay_roll < 0.60:
            delay_minutes = 0
        elif delay_roll < 0.90:
            delay_minutes = random.randint(1, 30)
        else:
            delay_minutes = random.randint(31, 180)

        # Starting position: somewhere along the route (not always at source)
        progress = random.uniform(0.1, 0.9)   # 10%–90% through the journey
        lat, lon = self._interpolate_position(
            src_data[2], src_data[3],
            dst_data[2], dst_data[3],
            progress
        )

        # Altitude profile: climbing below 20%, cruising 20–80%, descending above 80%
        altitude = self._altitude_for_progress(progress)

        # Speed profile: slower during climb/descent
        speed = self._speed_for_altitude(altitude)

        # Heading: rough direction from source to destination
        heading = self._calculate_heading(
            src_data[2], src_data[3],
            dst_data[2], dst_data[3]
        )

        state = {
            "flight_id"      : flight_id,
            "airline"        : airline_name,
            "airline_iata"   : airline_iata,
            "flight_number"  : flight_number,
            "source_airport" : src,
            "dest_airport"   : dst,
            "source_city"    : src_data[1],
            "dest_city"      : dst_data[1],
            "aircraft_type"  : random.choice(fleet),
            "departure_time" : departure_time.isoformat(),
            "arrival_time"   : arrival_time.isoformat(),
            "delay_minutes"  : delay_minutes,
            "status"         : "active",
            "progress"       : progress,
            "src_lat"        : src_data[2],
            "src_lon"        : src_data[3],
            "dst_lat"        : dst_data[2],
            "dst_lon"        : dst_data[3],
            "duration_mins"  : duration_mins,
            "heading"        : round(heading, 1),
            "lat"            : lat,
            "lon"            : lon,
            "altitude"       : altitude,
            "speed"          : speed,
        }

        # Store state so future calls can update this flight
        self._active_flights[flight_id] = state

        logger.debug("New flight created: %s %s→%s", flight_id, src, dst)
        return self._state_to_event(state)

    def _update_existing_flight(self, flight_id: str) -> dict:
        """Advance an existing flight along its route."""
        state = self._active_flights[flight_id]

        # Advance progress by 1–3% per update (roughly 30s intervals)
        state["progress"] = min(state["progress"] + random.uniform(0.01, 0.03), 1.0)

        # Recalculate position
        state["lat"], state["lon"] = self._interpolate_position(
            state["src_lat"], state["src_lon"],
            state["dst_lat"], state["dst_lon"],
            state["progress"]
        )

        # Update altitude and speed based on progress
        state["altitude"] = self._altitude_for_progress(state["progress"])
        state["speed"]    = self._speed_for_altitude(state["altitude"])

        # Update status
        if state["progress"] >= 0.99:
            state["status"] = "landed"
            # Remove from active flights — it's done
            del self._active_flights[flight_id]
            logger.debug("Flight %s has landed", flight_id)
        elif state["progress"] < 0.15:
            state["status"] = "active"  # climbing
        else:
            state["status"] = "active"

        return self._state_to_event(state)

    def _state_to_event(self, state: dict) -> dict:
        """Convert internal flight state to the external event schema."""
        now = datetime.now(timezone.utc)
        return {
            # ── Required schema fields ──
            "event_id"      : str(uuid.uuid4()),          # Unique per event
            "flight_id"     : state["flight_id"],
            "airline"       : state["airline"],
            "airline_iata"  : state["airline_iata"],
            "flight_number" : state["flight_number"],
            "source_airport": state["source_airport"],
            "dest_airport"  : state["dest_airport"],
            "source_city"   : state["source_city"],
            "dest_city"     : state["dest_city"],
            "latitude"      : round(state["lat"], 6),
            "longitude"     : round(state["lon"], 6),
            "altitude"      : state["altitude"],
            "speed"         : round(state["speed"], 1),
            "heading"       : state["heading"],
            "status"        : state["status"],
            "departure_time": state["departure_time"],
            "arrival_time"  : state["arrival_time"],
            "delay_minutes" : state["delay_minutes"],
            "aircraft_type" : state["aircraft_type"],
            "timestamp"     : now.isoformat(),
            "source"        : "simulator",
        }

    # ─── Physics helpers ─────────────────────────────────────────────────────

    @staticmethod
    def _interpolate_position(
        src_lat: float, src_lon: float,
        dst_lat: float, dst_lon: float,
        progress: float
    ) -> tuple[float, float]:
        """
        Linear interpolation between two lat/lon points.
        Real aircraft follow great-circle routes (slightly curved) but
        linear is fine for our visualisation purposes.
        Adds small random jitter to simulate GPS noise.
        """
        lat = src_lat + (dst_lat - src_lat) * progress + random.uniform(-0.05, 0.05)
        lon = src_lon + (dst_lon - src_lon) * progress + random.uniform(-0.05, 0.05)
        return round(lat, 6), round(lon, 6)

    @staticmethod
    def _altitude_for_progress(progress: float) -> int:
        """
        Simulate realistic altitude profile:
          0–15%  : Climbing  (0 → 35,000 ft)
          15–80% : Cruise    (~35,000 ft with small variation)
          80–100%: Descending (35,000 → 0 ft)
        """
        if progress < 0.15:
            # Climbing phase
            altitude = int((progress / 0.15) * 35000)
        elif progress > 0.80:
            # Descending phase
            descent_progress = (progress - 0.80) / 0.20
            altitude = int((1 - descent_progress) * 35000)
        else:
            # Cruise — slight variation
            altitude = random.randint(33000, 38000)
        return altitude

    @staticmethod
    def _speed_for_altitude(altitude: int) -> float:
        """
        Speed varies with altitude:
          Ground/taxi  : 0–50 km/h
          Climbing     : 400–750 km/h
          Cruise       : 800–950 km/h
          Descending   : 400–600 km/h
        """
        if altitude < 1000:
            return round(random.uniform(0, 50), 1)
        elif altitude < 20000:
            return round(random.uniform(400, 750), 1)
        elif altitude >= 30000:
            return round(random.uniform(800, 950), 1)
        else:
            return round(random.uniform(500, 700), 1)

    @staticmethod
    def _calculate_heading(
        src_lat: float, src_lon: float,
        dst_lat: float, dst_lon: float
    ) -> float:
        """Approximate compass heading from source to destination."""
        import math
        d_lon = dst_lon - src_lon
        d_lat = dst_lat - src_lat
        angle = math.degrees(math.atan2(d_lon, d_lat))
        # Normalise to 0–360
        return round((angle + 360) % 360, 1)

    def _inject_error(self, event: dict) -> dict:
        """
        Deliberately corrupt a record to test data validation.
        Randomly picks one type of error to inject.
        """
        error_types = [
            "null_altitude",
            "negative_speed",
            "invalid_status",
            "missing_airport",
            "extreme_coordinates",
            "duplicate_event_id",
        ]
        error_type = random.choice(error_types)

        if error_type == "null_altitude":
            event["altitude"] = None

        elif error_type == "negative_speed":
            event["speed"] = -999.9          # Physically impossible

        elif error_type == "invalid_status":
            event["status"] = "ghost_flight"  # Not in our enum

        elif error_type == "missing_airport":
            event["source_airport"] = None

        elif error_type == "extreme_coordinates":
            event["latitude"] = 999.99        # Impossible lat

        elif error_type == "duplicate_event_id":
            event["event_id"] = "DUPLICATE-0000-0000-0000-000000000000"

        logger.warning("Injected error type '%s' into event %s",
                       error_type, event.get("event_id"))
        return event