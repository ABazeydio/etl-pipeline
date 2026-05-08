#!/usr/bin/env python3
"""
extract_weather.py

Fetch Current Weather (free) data for one or more locations and upload JSON responses to S3.

Usage examples:
  python src/extract_weather.py --locations "ottawa,tokyo"
  python src/extract_weather.py --location "ottawa"
  python src/extract_weather.py --config config/locations.json

"""

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

# --- Configuration ---
OPENWEATHER_API_KEY = os.environ.get("OPENWEATHER_API_KEY")  #env var
S3_BUCKET = os.environ.get("S3_BUCKET", "weather-flights-data-lake-project")
S3_RAW_PREFIX = "raw/weather/"
CURRENT_WEATHER_BASE = "https://api.openweathermap.org/data/2.5/weather"

# Retry settings
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 3



logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("extract_weather")


def parse_locations_arg(locations_arg: str) -> list:
    return [loc.strip() for loc in locations_arg.split(",") if loc.strip()]


def load_locations_from_file(path: str) -> list:
    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    if isinstance(data, dict):
        return list(data.keys())
    elif isinstance(data, list):
        return data
    return []


def call_current_weather(city: str, api_key: str, params: dict = None) -> dict:
    """Call the OpenWeather Current Weather API with retries."""
    if params is None:
        params = {}
    params.update({"q": city, "appid": api_key, "units": "metric"})

    attempt = 0
    while attempt < MAX_RETRIES:
        try:
            resp = requests.get(CURRENT_WEATHER_BASE, params=params, timeout=20)
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            attempt += 1
            logger.warning(
                "Current Weather request failed (attempt %d/%d): %s",
                attempt,
                MAX_RETRIES,
                exc,
            )
            if attempt >= MAX_RETRIES:
                logger.error("Max retries reached for city=%s", city)
                raise
            time.sleep(RETRY_BACKOFF_SECONDS * attempt)


def build_s3_key(prefix: str) -> str:
    now = datetime.utcnow()
    return f"{prefix}year={now.strftime('%Y')}/month={now.strftime('%m')}/day={now.strftime('%d')}/weather_{now.strftime('%Y%m%d_%H%M%S')}.ndjson"


def main():
    parser = argparse.ArgumentParser(description="Extract OpenWeather Current Weather data and save to S3.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--locations", help="Comma-separated city names (e.g., ottawa,tokyo)")
    group.add_argument("--config", help="Path to JSON config file with locations")
    group.add_argument("--location", help="Single location name")
    parser.add_argument("--s3-bucket", default=S3_BUCKET, help="S3 bucket to write to")
    parser.add_argument("--dry-run", action="store_true", help="Don't upload to S3; just print")
    args = parser.parse_args()

    if not OPENWEATHER_API_KEY:
        logger.error("OPENWEATHER_API_KEY environment variable not set. Exiting.")
        sys.exit(1)

    # Resolve locations
    if args.locations:
        locations = parse_locations_arg(args.locations)
    elif args.config:
        locations = load_locations_from_file(args.config)
    else:
        locations = [args.location]

    logger.info("Found %d location(s) to fetch: %s", len(locations), locations)

    # Setup S3 client
    s3_client = boto3.client("s3")

    collected_data = []

    for city in locations:
        logger.info("Fetching Current Weather for %s", city)
        try:
            payload = call_current_weather(city=city, api_key=OPENWEATHER_API_KEY)
        except Exception as e:
            logger.exception("Failed to fetch data for %s: %s", city, e)
            continue

        record = {
            "_etl_city_queried": city,
            "_etl_timestamp": datetime.utcnow().isoformat(),
            "api_response": payload
        }
        collected_data.append(record)

    if not collected_data:
        logger.warning("No data collected. Exiting.")
        return

    # Convert to JSON Lines (NDJSON)
    ndjson_body = "\n".join(json.dumps(rec, ensure_ascii=False) for rec in collected_data).encode("utf-8")

    if args.dry_run:
        logger.info(
            "Dry run enabled — would upload batch payload of %d records: %d bytes",
            len(collected_data),
            len(ndjson_body),
        )
        return

    key = build_s3_key(S3_RAW_PREFIX)
    try:
        s3_client.put_object(Bucket=args.s3_bucket, Key=key, Body=ndjson_body)
        logger.info("Uploaded to s3://%s/%s", args.s3_bucket, key)
    except Exception:
        logger.exception("Failed to upload batch data to S3")

    logger.info("Extraction job complete.")


if __name__ == "__main__":
    main()
