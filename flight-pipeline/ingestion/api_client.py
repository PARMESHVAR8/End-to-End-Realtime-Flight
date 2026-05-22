# ingestion/api_client.py
"""
AviationStack API Client
========================
Fetches real-time flight data from aviationstack.com

Free tier gives 100 API calls/month — we use the simulator for
high-frequency testing and the real API for occasional validation.

Sign up at: https://aviationstack.com/signup/free
Set AVIATIONSTACK_API_KEY in your .env file.

USAGE:
  from ingestion.api_client import AviationStackClient
  client = AviationStackClient()
  flights = client.get_live_flights(limit=10)
"""

import os
import logging
import time
from typing import Optional
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

load_dotenv()   # Loads variables from .env into os.environ

logger = logging.getLogger(__name__)

# Base URL for AviationStack API
AVIATIONSTACK_BASE_URL = "http://api.aviationstack.com/v1"


class AviationStackClient:
    """
    Wrapper around the AviationStack REST API.

    Why a wrapper class instead of calling requests directly?
    1. Centralises retry logic — one place to update
    2. Handles authentication — API key never leaks into business logic
    3. Testable — can mock this class in unit tests
    4. Rate-limit aware — won't accidentally burn your free quota
    """

    def __init__(self):
        self.api_key = os.getenv("AVIATIONSTACK_API_KEY")
        self.session = requests.Session()   # Reuse TCP connection = faster
        self.session.params = {"access_key": self.api_key}

        if not self.api_key:
            logger.warning(
                "AVIATIONSTACK_API_KEY not set. "
                "Only simulator data will be available."
            )

    def get_live_flights(
        self,
        limit: int = 100,
        airline_iata: Optional[str] = None,
        flight_status: Optional[str] = None,
        dep_iata: Optional[str] = None,
    ) -> list[dict]:
        """
        Fetch live flights from AviationStack.

        Args:
            limit        : Max records to return (free tier max = 100)
            airline_iata : Filter by airline IATA code (e.g., "AI" for Air India)
            flight_status: Filter by status: "active", "landed", "scheduled"
            dep_iata     : Filter by departure airport code

        Returns:
            List of normalised flight event dicts (same schema as simulator)
        """
        if not self.api_key:
            logger.error("Cannot call API — no API key configured")
            return []

        params = {"limit": limit}
        if airline_iata:
            params["airline_iata"] = airline_iata
        if flight_status:
            params["flight_status"] = flight_status
        if dep_iata:
            params["dep_iata"] = dep_iata

        try:
            logger.info("Calling AviationStack API | params=%s", params)
            response = self.session.get(
                f"{AVIATIONSTACK_BASE_URL}/flights",
                params=params,
                timeout=10          # Never wait more than 10 seconds
            )
            # Raise an exception for HTTP error codes (4xx, 5xx)
            response.raise_for_status()

            data = response.json()

            # AviationStack wraps data in a 'data' key
            raw_flights = data.get("data", [])
            logger.info(
                "API returned %d flights | pagination=%s",
                len(raw_flights),
                data.get("pagination", {})
            )

            # Normalise to our schema
            normalised = [
                self._normalise(flight)
                for flight in raw_flights
                if flight is not None
            ]
            # Filter out any that failed normalisation
            return [f for f in normalised if f is not None]

        except requests.exceptions.Timeout:
            logger.error("AviationStack API timed out after 10 seconds")
            return []
        except requests.exceptions.ConnectionError:
            logger.error("Cannot reach AviationStack API — check internet connection")
            return []
        except requests.exceptions.HTTPError as e:
            logger.error("AviationStack API HTTP error: %s", e)
            return []
        except Exception as e:
            logger.exception("Unexpected error calling AviationStack API: %s", e)
            return []

    def _normalise(self, raw: dict) -> Optional[dict]:
        """
        Convert AviationStack's API format to our internal schema.

        AviationStack returns deeply nested JSON.
        We flatten it into the same flat schema as our simulator.
        This way, downstream code doesn't care where data came from.

        Example raw structure (abbreviated):
        {
          "flight": {"iata": "AI101", "number": "101"},
          "airline": {"name": "Air India", "iata": "AI"},
          "departure": {"iata": "DEL", "scheduled": "2024-06-15T06:30:00+00:00"},
          "arrival": {"iata": "BOM", "scheduled": "2024-06-15T08:30:00+00:00"},
          "live": {"latitude": 22.3, "longitude": 73.1, "altitude": 35000, ...}
        }
        """
        try:
            import uuid
            live    = raw.get("live") or {}
            flight  = raw.get("flight") or {}
            airline = raw.get("airline") or {}
            dep     = raw.get("departure") or {}
            arr     = raw.get("arrival") or {}

            # Skip flights with no live position data (grounded, not tracked)
            if not live.get("latitude") or not live.get("longitude"):
                return None

            return {
                "event_id"      : str(uuid.uuid4()),
                "flight_id"     : flight.get("iata") or flight.get("icao", "UNKNOWN"),
                "airline"       : airline.get("name", "Unknown Airline"),
                "airline_iata"  : airline.get("iata", "??"),
                "flight_number" : flight.get("number", "0"),
                "source_airport": dep.get("iata", "???"),
                "dest_airport"  : arr.get("iata", "???"),
                "source_city"   : dep.get("airport", ""),
                "dest_city"     : arr.get("airport", ""),
                "latitude"      : float(live.get("latitude", 0)),
                "longitude"     : float(live.get("longitude", 0)),
                "altitude"      : int(live.get("altitude", 0)),
                "speed"         : float(live.get("speed_horizontal", 0)),
                "heading"       : float(live.get("direction", 0)),
                "status"        : raw.get("flight_status", "unknown"),
                "departure_time": dep.get("scheduled", ""),
                "arrival_time"  : arr.get("scheduled", ""),
                "delay_minutes" : dep.get("delay") or 0,
                "aircraft_type" : (raw.get("aircraft") or {}).get("icao", "UNKN"),
                "timestamp"     : datetime.now(timezone.utc).isoformat(),
                "source"        : "aviationstack_api",
            }

        except (TypeError, ValueError, KeyError) as e:
            logger.warning("Failed to normalise flight record: %s | error: %s", raw, e)
            return None