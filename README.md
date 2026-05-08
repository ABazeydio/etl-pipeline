# ETL Pipeline

This project is an end-to-end data engineering pipeline that extracts weather and flight data from the OpenWeather API and AviationStack API, enriches the data, and stores it in an AWS S3 data lake.

## Setup

Before running the scripts, you must set the required environment variables:

- `OPENWEATHER_API_KEY`: Your OpenWeather API key.
- `FLIGHTS_API_KEY`: Your AviationStack API key.
- `S3_BUCKET` (optional): The target S3 bucket name. Defaults to `weather-flights-data-lake-project`.

## Extract Flights Data

The `src/extract_flights.py` script fetches flight data from the AviationStack API, enriches it with airline and airport details, and uploads the resulting JSON to S3. The script caches airline and airport details locally to reduce API calls.

### Usage

```bash
# Fetch flights by departure and arrival IATA codes
python src/extract_flights.py --dep-iata YOW --arr-iata YYZ

# Fetch flights by date
python src/extract_flights.py --flight-date 2025-01-01

# Run without uploading to S3 (dry run)
python src/extract_flights.py --dep-iata YOW --dry-run
```

**Arguments:**
- `--dep-iata`: Departure airport IATA code
- `--arr-iata`: Arrival airport IATA code
- `--flight-date`: Flight date (YYYY-MM-DD)
- `--flight-status`: Flight status (e.g., active, landed)
- `--flight-iata`: Specific flight IATA code
- `--s3-bucket`: Override the default S3 bucket
- `--dry-run`: Fetch data and print to console without uploading to S3

## Extract Weather Data

The `src/extract_weather.py` script fetches current weather data for one or more locations using the OpenWeather API and uploads the data to S3 in NDJSON format.

### Usage

```bash
# Fetch weather for a single location
python src/extract_weather.py --location "ottawa"

# Fetch weather for multiple comma-separated locations
python src/extract_weather.py --locations "ottawa,tokyo"

# Fetch weather using a JSON config file
python src/extract_weather.py --config config/locations.json
```

**Arguments:**
You must provide exactly one of the following location arguments:
- `--location`: Single location name
- `--locations`: Comma-separated city names
- `--config`: Path to a JSON config file containing locations

Optional arguments:
- `--s3-bucket`: Override the default S3 bucket
- `--dry-run`: Fetch data and print to console without uploading to S3
