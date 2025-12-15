#!/usr/bin/env python3
"""
extract_weather.py

Fetch Current Weather (free) data for one or more locations and upload JSON responses to S3.

Usage examples:
  python src/extract_weather.py --locations ottawa=45.4215,-75.6972,tokyo=35.6895,139.6917
  python src/extract_weather.py --location "ottawa" --lat 45.4215 --lon -75.6972
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
OPENWEATHER_API_KEY = os.environ.get("OPENWEATHER_API_KEY")  # recommended to set as env var
S3_BUCKET = os.environ.get("S3_BUCKET", "weather-flights-data-lake-project")
S3_RAW_PREFIX = "raw/weather/"
CURRENT_WEATHER_BASE = "https://api.openweathermap.org/data/2.5/weather"

# Retry settings
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 3

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("extract_weather")


def parse_locations_arg(locations_arg: str) -> Dict[str, Tuple[float, float]]:
    locations = {}
    for pair in locations_arg.split(","):
        if "=" in pair:
            name, coords = pair.split("=", 1)
            lat_str, lon_str = coords.split(",", 1)
            locations[name.strip()] = (float(lat_str), float(lon_str))
        else:
            logger.warning("Skipping invalid location token: %s", pair)
    return locations


def load_locations_from_file(path: str) -> Dict[str, Tuple[float, float]]:
    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)
    locations = {name: (float(coords[0]), float(coords[1])) for name, coords in data.items()}
    return locations


def call_current_weather(lat: float, lon: float, api_key: str, params: dict = None) -> dict:
    """Call the OpenWeather Current Weather API with retries."""
    if params is None:
        params = {}
    params.update({"lat": lat, "lon": lon, "appid": api_key, "units": "metric"})

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
                logger.error("Max retries reached for lat=%s lon=%s", lat, lon)
                raise
            time.sleep(RETRY_BACKOFF_SECONDS * attempt)


def upload_json_to_s3(s3_client, bucket: str, key: str, data: dict) -> None:
    body = json.dumps(data, ensure_ascii=False).encode("utf-8")
    s3_client.put_object(Bucket=bucket, Key=key, Body=body)
    logger.info("Uploaded to s3://%s/%s", bucket, key)


def build_s3_key(prefix: str, city_name: str) -> str:
    timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    safe_city = city_name.replace(" ", "_").lower()
    return f"{prefix}{safe_city}/weather_{timestamp}.json"


def main():
    parser = argparse.ArgumentParser(description="Extract OpenWeather Current Weather data and save to S3.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--locations", help="Comma-separated name=lat,lon pairs")
    group.add_argument("--config", help="Path to JSON config file with locations")
    group.add_argument("--location", help="Single location name (requires --lat and --lon)")
    parser.add_argument("--lat", type=float, help="Latitude for single location")
    parser.add_argument("--lon", type=float, help="Longitude for single location")
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
        if args.lat is None or args.lon is None:
            logger.error("--location requires --lat and --lon")
            sys.exit(1)
        locations = {args.location: (args.lat, args.lon)}

    logger.info("Found %d location(s) to fetch: %s", len(locations), list(locations.keys()))

    # Setup S3 client
    s3_client = boto3.client("s3")

    for name, (lat, lon) in locations.items():
        logger.info("Fetching Current Weather for %s (lat=%s lon=%s)", name, lat, lon)
        try:
            payload = call_current_weather(lat=lat, lon=lon, api_key=OPENWEATHER_API_KEY)
        except Exception as e:
            logger.exception("Failed to fetch data for %s: %s", name, e)
            continue

        # Optional: extract key fields for cleaner JSON
        data_to_store = {
            "city": name,
            "lat": lat,
            "lon": lon,
            "temp": payload["main"]["temp"],
            "humidity": payload["main"]["humidity"],
            "wind_speed": payload["wind"]["speed"],
            "description": payload["weather"][0]["description"],
            "timestamp": payload["dt"],
        }

        if args.dry_run:
            logger.info(
                "Dry run enabled — would upload payload for %s: %d bytes",
                name,
                len(json.dumps(data_to_store)),
            )
            continue

        key = build_s3_key(S3_RAW_PREFIX, name)
        try:
            upload_json_to_s3(s3_client, args.s3_bucket, key, data_to_store)
        except Exception:
            logger.exception("Failed to upload data for %s to S3", name)

    logger.info("Extraction job complete.")


if __name__ == "__main__":
    main()
