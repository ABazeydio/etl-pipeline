#Testing Flight Api Call

from unittest.mock import patch, MagicMock
from extract_flights import call_flights_api

@patch("extract_flights.requests.get")
def test_call_flights_api_success(mock_get):
    # Mock response object
    mock_response = MagicMock()
    mock_response.json.return_value = {"data": "success"}
    mock_response.raise_for_status.return_value = None

    mock_get.return_value = mock_response

    result = call_flights_api("flights")

    assert result == {"data": "success"}
    mock_get.assert_called_once()