#!/usr/bin/env python3
"""
extract_flights.py

Fetch flights data from AviationStack API and enrich it with airline and airport details.
Uploads the resulting JSON to S3.

Usage examples:
  python extract_flights.py --dep-iata YOW --arr-iata YYZ
  python extract_flights.py --flight-date 2025-01-01
  python extract_flights.py --dep-iata YOW --dry-run
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime

import boto3
import requests

# --- Configuration ---
FLIGHTS_API_KEY = os.environ.get("FLIGHTS_API_KEY")
S3_BUCKET = os.environ.get("S3_BUCKET", "weather-flights-data-lake-project")
S3_RAW_PREFIX = "raw/flights/"
API_BASE = "https://api.aviationstack.com/v1"

# Retry settings
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("extract_flights")

# --- Persistent Caches ---
AIRLINE_CACHE_FILE = "airline_cache.json"
AIRPORT_CACHE_FILE = "airport_cache.json"

def load_cache(filename: str) -> dict:
    if os.path.exists(filename):
        try:
            with open(filename, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.warning("Could not load cache %s: %s", filename, e)
    return {}

def save_cache(filename: str, cache_data: dict) -> None:
    try:
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(cache_data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.warning("Could not save cache %s: %s", filename, e)

AIRLINE_CACHE = load_cache(AIRLINE_CACHE_FILE)
AIRPORT_CACHE = load_cache(AIRPORT_CACHE_FILE)


def call_aviationstack_api(endpoint: str, **params) -> dict:
    """Call an AviationStack endpoint with retries."""
    params["access_key"] = FLIGHTS_API_KEY
    attempt = 0
    while attempt < MAX_RETRIES:
        try:
            resp = requests.get(f"{API_BASE}/{endpoint}", params=params, timeout=20)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            attempt += 1
            logger.warning(
                "Request failed (attempt %d/%d): %s",
                attempt,
                MAX_RETRIES,
                exc,
            )
            if attempt >= MAX_RETRIES:
                logger.error("Max retries reached for endpoint=%s", endpoint)
                raise
            time.sleep(RETRY_BACKOFF_SECONDS * attempt)


def get_airline_details(iata_code: str) -> dict:
    if not iata_code:
        return {}
    if iata_code in AIRLINE_CACHE:
        return AIRLINE_CACHE[iata_code]
    
    logger.info("Fetching airline details for IATA: %s", iata_code)
    try:
        data = call_aviationstack_api("airlines", iata_code=iata_code)
        results = data.get("data", [])
        if results:
            info = results[0]
            AIRLINE_CACHE[iata_code] = {
                "airline_name": info.get("airline_name"),
                "country_iso2": info.get("country_iso2"),
                "date_founded": info.get("date_founded"),
                "country_name": info.get("country_name"),
                "type": info.get("type")
            }
        else:
            AIRLINE_CACHE[iata_code] = {}
    except Exception as e:
        logger.warning("Failed to fetch airline %s: %s", iata_code, e)
        AIRLINE_CACHE[iata_code] = {}
        
    return AIRLINE_CACHE[iata_code]


def get_airport_details(iata_code: str) -> dict:
    if not iata_code:
        return {}
    if iata_code in AIRPORT_CACHE:
        return AIRPORT_CACHE[iata_code]
    
    logger.info("Fetching airport details for IATA: %s", iata_code)
    try:
        data = call_aviationstack_api("airports", iata_code=iata_code)
        results = data.get("data", [])
        if results:
            info = results[0]
            AIRPORT_CACHE[iata_code] = {
                "airport_name": info.get("airport_name")
            }
        else:
            AIRPORT_CACHE[iata_code] = {}
    except Exception as e:
        logger.warning("Failed to fetch airport %s: %s", iata_code, e)
        AIRPORT_CACHE[iata_code] = {}
        
    return AIRPORT_CACHE[iata_code]


def enrich_flights_data(payload: dict) -> list:
    """Enrich flight payload with airline and airport data."""
    data_list = payload.get("data", [])
    enriched = []
    
    for item in data_list:
        airline_iata = item.get("airline", {}).get("iata")
        dep_iata = item.get("departure", {}).get("iata")
        arr_iata = item.get("arrival", {}).get("iata")
        
        airline_data = get_airline_details(airline_iata) if airline_iata else {}
        dep_airport_data = get_airport_details(dep_iata) if dep_iata else {}
        arr_airport_data = get_airport_details(arr_iata) if arr_iata else {}
        
        enriched.append({
            "flight_date": item.get("flight_date"),
            "flight_status": item.get("flight_status"),
            "flight_number": item.get("flight", {}).get("number"),
            "airline_name": airline_data.get("airline_name") or item.get("airline", {}).get("name"),
            "dep_airport_name": dep_airport_data.get("airport_name") or item.get("departure", {}).get("airport"),
            "arr_airport_name": arr_airport_data.get("airport_name") or item.get("arrival", {}).get("airport"),
            "country_iso2": airline_data.get("country_iso2"),
            "date_founded": airline_data.get("date_founded"),
            "country_name": airline_data.get("country_name"),
            "type": airline_data.get("type")
        })
            
    return enriched


def upload_json_to_s3(s3_client, bucket: str, key: str, data: list) -> None:
    body = json.dumps(data, ensure_ascii=False).encode("utf-8")
    s3_client.put_object(Bucket=bucket, Key=key, Body=body)
    logger.info("Uploaded to s3://%s/%s", bucket, key)


def build_s3_key(prefix: str, label: str = "") -> str:
    timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    suffix = f"_{label}" if label else ""
    return f"{prefix}flights{suffix}_{timestamp}.json"


def main():
    parser = argparse.ArgumentParser(
        description="Extract and enrich AviationStack flight data and save to S3."
    )
    
    # flights filters
    parser.add_argument("--dep-iata", help="Departure airport IATA code")
    parser.add_argument("--arr-iata", help="Arrival airport IATA code")
    parser.add_argument("--flight-date", help="Flight date YYYY-MM-DD")
    parser.add_argument("--flight-status", help="Flight status e.g. active, landed")
    parser.add_argument("--flight-iata", help="Specific flight IATA code")

    parser.add_argument("--s3-bucket", default=S3_BUCKET, help="S3 bucket to write to")
    parser.add_argument("--dry-run", action="store_true", help="Don't upload to S3; just print")
    args = parser.parse_args()

    if not FLIGHTS_API_KEY:
        logger.error("FLIGHTS_API_KEY environment variable not set. Exiting.")
        sys.exit(1)

    params = {}
    if args.dep_iata: params["dep_iata"] = args.dep_iata
    if args.arr_iata: params["arr_iata"] = args.arr_iata
    if args.flight_date: params["flight_date"] = args.flight_date
    if args.flight_status: params["flight_status"] = args.flight_status
    if args.flight_iata: params["flight_iata"] = args.flight_iata

    logger.info("Calling AviationStack flights endpoint with params: %s", params)

    try:
        payload = call_aviationstack_api("flights", **params)
    except Exception as e:
        logger.exception("Failed to fetch data from endpoint=flights: %s", e)
        sys.exit(1)

    record_count = len(payload.get("data", []))
    logger.info("Received %d record(s) from flights endpoint", record_count)

    logger.info("Enriching flight data with airline and airport details...")
    data_to_store = enrich_flights_data(payload)

    logger.info("Saving caches to disk to reduce future API calls...")
    save_cache(AIRLINE_CACHE_FILE, AIRLINE_CACHE)
    save_cache(AIRPORT_CACHE_FILE, AIRPORT_CACHE)

    if args.dry_run:
        logger.info(
            "Dry run enabled — would upload %d bytes (enriched %d flights)",
            len(json.dumps(data_to_store)),
            len(data_to_store)
        )
        logger.info("Sample record: %s", json.dumps(data_to_store[0] if data_to_store else {}, indent=2))
        return

    # Use a short label for the S3 key
    label = params.get("dep_iata") or params.get("flight_iata") or ""
    key = build_s3_key(S3_RAW_PREFIX, label)

    s3_client = boto3.client("s3")
    try:
        upload_json_to_s3(s3_client, args.s3_bucket, key, data_to_store)
    except Exception:
        logger.exception("Failed to upload enriched flight data to S3")
        sys.exit(1)

    logger.info("Extraction job complete.")


if __name__ == "__main__":
    main()