"""
test_flights_api.py

Unit tests for extract_flights.py
Run with: pytest test_flights_api.py -v
"""

from unittest.mock import patch, MagicMock, call
import pytest
import requests

from extract_flights import call_aviationstack_api, build_s3_key, enrich_flights_data, AIRLINE_CACHE, AIRPORT_CACHE


# --- call_aviationstack_api tests ---

@patch("extract_flights.requests.get")
def test_call_aviationstack_api_success(mock_get):
    """Returns JSON on a successful response."""
    mock_response = MagicMock()
    mock_response.json.return_value = {"data": [{"flight": "AC123"}]}
    mock_response.raise_for_status.return_value = None
    mock_get.return_value = mock_response

    result = call_aviationstack_api("flights", dep_iata="YOW")

    assert result == {"data": [{"flight": "AC123"}]}
    mock_get.assert_called_once()


@patch("extract_flights.time.sleep", return_value=None)
@patch("extract_flights.requests.get")
def test_call_aviationstack_api_retries_then_succeeds(mock_get, mock_sleep):
    """Retries on failure and returns result when a later attempt succeeds."""
    fail_response = MagicMock()
    fail_response.raise_for_status.side_effect = requests.RequestException("timeout")

    success_response = MagicMock()
    success_response.json.return_value = {"data": []}
    success_response.raise_for_status.return_value = None

    mock_get.side_effect = [fail_response, success_response]

    result = call_aviationstack_api("flights")

    assert result == {"data": []}
    assert mock_get.call_count == 2


@patch("extract_flights.time.sleep", return_value=None)
@patch("extract_flights.requests.get")
def test_call_aviationstack_api_raises_after_max_retries(mock_get, mock_sleep):
    """Raises after MAX_RETRIES consecutive failures."""
    fail_response = MagicMock()
    fail_response.raise_for_status.side_effect = requests.RequestException("timeout")
    mock_get.return_value = fail_response

    with pytest.raises(requests.RequestException):
        call_aviationstack_api("airports")

    assert mock_get.call_count == 3  # MAX_RETRIES = 3


# --- build_s3_key tests ---

def test_build_s3_key_with_label():
    key = build_s3_key("raw/flights/", "YOW")
    assert key.startswith("raw/flights/flights_YOW_")
    assert key.endswith(".json")


def test_build_s3_key_without_label():
    key = build_s3_key("raw/flights/")
    assert "flights_" in key
    assert key.endswith(".json")


# --- enrich_flights_data tests ---
@patch("extract_flights.get_airline_details")
@patch("extract_flights.get_airport_details")
def test_enrich_flights_data(mock_get_airport, mock_get_airline):
    mock_get_airline.return_value = {
        "airline_name": "Air Canada",
        "country_iso2": "CA",
        "date_founded": "1937",
        "country_name": "Canada",
        "type": "scheduled"
    }
    
    def airport_side_effect(iata):
        if iata == "YOW": return {"airport_name": "Ottawa Macdonald-Cartier"}
        if iata == "YYZ": return {"airport_name": "Toronto Pearson"}
        return {}
    mock_get_airport.side_effect = airport_side_effect
    
    payload = {
        "data": [
            {
                "flight_date": "2026-05-07",
                "flight_status": "active",
                "flight": {"number": "123"},
                "airline": {"name": "AC", "iata": "AC"},
                "departure": {"airport": "YOW", "iata": "YOW"},
                "arrival": {"airport": "YYZ", "iata": "YYZ"}
            }
        ]
    }
    
    enriched = enrich_flights_data(payload)
    
    assert len(enriched) == 1
    assert enriched[0]["flight_date"] == "2026-05-07"
    assert enriched[0]["airline_name"] == "Air Canada"
    assert enriched[0]["dep_airport_name"] == "Ottawa Macdonald-Cartier"
    assert enriched[0]["arr_airport_name"] == "Toronto Pearson"
    assert enriched[0]["country_iso2"] == "CA"