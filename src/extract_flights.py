#!/usr/bin/env python3
"""
extract_flights.py

Fetch data from AviationStack API (flights, airlines, or airports)
and upload JSON responses to S3.

Usage examples:
  python extract_flights.py --endpoint flights --dep-iata YOW --arr-iata YYZ
  python extract_flights.py --endpoint flights --flight-date 2025-01-01
  python extract_flights.py --endpoint airlines --airline-name "Air Canada"
  python extract_flights.py --endpoint airports --country-name "Canada"
  python extract_flights.py --endpoint flights --dep-iata YOW --dry-run
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

VALID_ENDPOINTS = ("flights", "airlines", "airports")

# Retry settings
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 3

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("extract_flights")


def call_flights_api(endpoint: str, **params) -> dict:
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


def upload_json_to_s3(s3_client, bucket: str, key: str, data: dict) -> None:
    body = json.dumps(data, ensure_ascii=False).encode("utf-8")
    s3_client.put_object(Bucket=bucket, Key=key, Body=body)
    logger.info("Uploaded to s3://%s/%s", bucket, key)


def build_s3_key(prefix: str, endpoint: str, label: str = "") -> str:
    timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    suffix = f"_{label}" if label else ""
    return f"{prefix}{endpoint}/{endpoint}{suffix}_{timestamp}.json"


def build_query_params(endpoint: str, args: argparse.Namespace) -> dict:
    """Build API query params from CLI args depending on the endpoint."""
    params = {}
    if endpoint == "flights":
        if args.dep_iata:
            params["dep_iata"] = args.dep_iata
        if args.arr_iata:
            params["arr_iata"] = args.arr_iata
        if args.flight_date:
            params["flight_date"] = args.flight_date
        if args.flight_status:
            params["flight_status"] = args.flight_status
        if args.flight_iata:
            params["flight_iata"] = args.flight_iata
    elif endpoint == "airlines":
        if args.airline_name:
            params["airline_name"] = args.airline_name
    elif endpoint == "airports":
        if args.country_name:
            params["country_name"] = args.country_name
    return params


def main():
    parser = argparse.ArgumentParser(
        description="Extract AviationStack data and save to S3."
    )
    parser.add_argument(
        "--endpoint",
        required=True,
        choices=VALID_ENDPOINTS,
        help="AviationStack endpoint to call",
    )

    # flights filters
    parser.add_argument("--dep-iata", help="Departure airport IATA code (flights only)")
    parser.add_argument("--arr-iata", help="Arrival airport IATA code (flights only)")
    parser.add_argument("--flight-date", help="Flight date YYYY-MM-DD (flights only)")
    parser.add_argument("--flight-status", help="Flight status e.g. active, landed (flights only)")
    parser.add_argument("--flight-iata", help="Specific flight IATA code (flights only)")

    # airlines filters
    parser.add_argument("--airline-name", help="Airline name to search (airlines only)")

    # airports filters
    parser.add_argument("--country-name", help="Country name to filter airports (airports only)")

    parser.add_argument("--s3-bucket", default=S3_BUCKET, help="S3 bucket to write to")
    parser.add_argument("--dry-run", action="store_true", help="Don't upload to S3; just print")
    args = parser.parse_args()

    if not FLIGHTS_API_KEY:
        logger.error("FLIGHTS_API_KEY environment variable not set. Exiting.")
        sys.exit(1)

    endpoint = args.endpoint
    params = build_query_params(endpoint, args)

    logger.info("Calling AviationStack endpoint: %s with params: %s", endpoint, params)

    try:
        payload = call_flights_api(endpoint, **params)
    except Exception as e:
        logger.exception("Failed to fetch data from endpoint=%s: %s", endpoint, e)
        sys.exit(1)

    record_count = len(payload.get("data", []))
    logger.info("Received %d record(s) from endpoint=%s", record_count, endpoint)

    if args.dry_run:
        logger.info(
            "Dry run enabled — would upload %d bytes for endpoint=%s",
            len(json.dumps(payload)),
            endpoint,
        )
        return

    # Use a short label for the S3 key (e.g. dep_iata value if filtering flights)
    label = params.get("dep_iata") or params.get("airline_name") or params.get("country_name") or ""
    key = build_s3_key(S3_RAW_PREFIX, endpoint, label)

    s3_client = boto3.client("s3")
    try:
        upload_json_to_s3(s3_client, args.s3_bucket, key, payload)
    except Exception:
        logger.exception("Failed to upload data for endpoint=%s to S3", endpoint)
        sys.exit(1)

    logger.info("Extraction job complete.")


if __name__ == "__main__":
    main()