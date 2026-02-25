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
    


def call_flights_api(endpoint : str, **params) -> dict:
    # endpoint = "flights" or "airlines" or "airports"
    attempt = 0
    params.update({"access_key" : FLIGHTS_API_KEY})
    while attempt < MAX_RETRIES:
        try:
            resp = requests.get(f"{API_BASE}/{endpoint}", params=params)
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
            time.sleep(RETRY_BACKOFF_SECONDS * attempt)


def upload_flights_s3(data : dict, city : str, endpoint : str):
    timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    s3_key = S3_RAW_PREFIX + city + "/" + endpoint + "_" + timestamp + ".json"
    s3_client = boto3.client("s3")
    s3_client.put_object(Bucket=S3_BUCKET, Key=s3_key, Body=json.dumps(data))
    logger.info("Uploaded to s3://%s/%s", S3_BUCKET, s3_key)

test_data = {"flight": "test", "city": "ottawa"}
upload_flights_s3(test_data, "ottawa", "flights")