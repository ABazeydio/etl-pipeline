"""
test_flights_api.py

Unit tests for extract_flights.py
Run with: pytest test_flights_api.py -v
"""

from unittest.mock import patch, MagicMock, call
import pytest
import requests

from extract_flights import call_flights_api, build_s3_key, build_query_params


# --- call_flights_api tests ---

@patch("extract_flights.requests.get")
def test_call_flights_api_success(mock_get):
    """Returns JSON on a successful response."""
    mock_response = MagicMock()
    mock_response.json.return_value = {"data": [{"flight": "AC123"}]}
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    result = call_flights_api("flights", dep_iata="YOW")

    assert result == {"data": [{"flight": "AC123"}]}
    mock_get.assert_called_once()


@patch("extract_flights.time.sleep", return_value=None)
@patch("extract_flights.requests.get")
def test_call_flights_api_retries_then_succeeds(mock_get, mock_sleep):
    """Retries on failure and returns result when a later attempt succeeds."""
    fail_response = MagicMock()
    fail_response.raise_for_status.side_effect = requests.RequestException("timeout")

    success_response = MagicMock()
    success_response.json.return_value = {"data": []}
    success_response.raise_for_status.return_value = None

    mock_get.side_effect = [fail_response, success_response]

    result = call_flights_api("flights")

    assert result == {"data": []}
    assert mock_get.call_count == 2


@patch("extract_flights.time.sleep", return_value=None)
@patch("extract_flights.requests.get")
def test_call_flights_api_raises_after_max_retries(mock_get, mock_sleep):
    """Raises after MAX_RETRIES consecutive failures."""
    fail_response = MagicMock()
    fail_response.raise_for_status.side_effect = requests.RequestException("timeout")
    mock_get.return_value = fail_response

    with pytest.raises(requests.RequestException):
        call_flights_api("airports")

    assert mock_get.call_count == 3  # MAX_RETRIES = 3


# --- build_s3_key tests ---

def test_build_s3_key_with_label():
    key = build_s3_key("raw/flights/", "flights", "YOW")
    assert key.startswith("raw/flights/flights/flights_YOW_")
    assert key.endswith(".json")


def test_build_s3_key_without_label():
    key = build_s3_key("raw/flights/", "airlines")
    assert "airlines" in key
    assert key.endswith(".json")


# --- build_query_params tests ---

def test_build_query_params_flights():
    parser_ns = MagicMock()
    parser_ns.dep_iata = "YOW"
    parser_ns.arr_iata = "YYZ"
    parser_ns.flight_date = "2025-01-01"
    parser_ns.flight_status = None
    parser_ns.flight_iata = None

    params = build_query_params("flights", parser_ns)

    assert params == {"dep_iata": "YOW", "arr_iata": "YYZ", "flight_date": "2025-01-01"}


def test_build_query_params_airlines():
    parser_ns = MagicMock()
    parser_ns.airline_name = "Air Canada"

    params = build_query_params("airlines", parser_ns)

    assert params == {"airline_name": "Air Canada"}


def test_build_query_params_airports():
    parser_ns = MagicMock()
    parser_ns.country_name = "Canada"

    params = build_query_params("airports", parser_ns)

    assert params == {"country_name": "Canada"}