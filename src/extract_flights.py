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
FLIGHT_BASE = ""