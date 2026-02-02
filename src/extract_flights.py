import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime
from typing import Dict, Tuple

import boto3
import requests


FLIGHTS_API_KEY = os.environ.get("FLIGHTS_API_KEY")

S3_BUCKET = os.environ.get("S3_BUCKET", "weather-flights-data-lake-project")
S3_RAW_PREFIX = "raw/flights/"
API_BASE = "https://api.aviationstack.com/v1"
# f"{API_BASE}/flights"
# f"{API_BASE}/airlines"
# f"{API_BASE}/airports"

# Retry settings
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 3

logging.basicConfig(
    level = logging.INFO,
    format = "%(asctime)s %(levelname)s %(message)s",
    handlers={logging.StreamHandler(sys.stdout)}
)

logger = logging.getLogger("extract_flights")

#def call_flights_api(flight_date : str, flight_number : str, airline_name : str,
#                    flight_status : str, dep_iata : str, arr_iata : str, flight_iata : str,
#                    api_key : str, params: dict = None) -> dict:
    
def call_flights_api(**params) -> dict:
    
    attempt = 0
    while attempt < MAX_RETRIES:
        try:
            resp = requests.get(API_BASE, params=params)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            attempt += 1
            logger.warning(
                 "Flight info request failed (attempt %d/%d): %s",
                attempt,
                MAX_RETRIES,
                exc,
            )
            if attempt >= MAX_RETRIES:
                logger.error("Max retries reached...")
                raise
            s