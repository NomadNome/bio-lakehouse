"""
Oura API v2 client using stdlib urllib (no external dependencies).

Handles authentication, pagination, and error handling for daily data endpoints.
"""

import json
import urllib.request
import urllib.error


OURA_BASE_URL = "https://api.ouraring.com"

ENDPOINTS = {
    "readiness": "/v2/usercollection/daily_readiness",
    "sleep": "/v2/usercollection/daily_sleep",
    "activity": "/v2/usercollection/daily_activity",
}


def fetch_daily_data(token, data_type, start_date, end_date):
    """Fetch daily data from Oura API v2 with pagination.

    Args:
        token: Oura Personal Access Token
        data_type: One of 'readiness', 'sleep', 'activity'
        start_date: ISO date string (YYYY-MM-DD)
        end_date: ISO date string (YYYY-MM-DD)

    Returns:
        List of data dicts from the API

    Raises:
        ValueError: If token is invalid (401)
        RuntimeError: For other API errors
    """
    endpoint = ENDPOINTS[data_type]
    all_data = []
    next_token = None

    while True:
        url = (
            f"{OURA_BASE_URL}{endpoint}"
            f"?start_date={start_date}&end_date={end_date}"
        )
        if next_token:
            url += f"&next_token={next_token}"

        req = urllib.request.Request(url)
        req.add_header("Authorization", f"Bearer {token}")

        try:
            with urllib.request.urlopen(req, timeout=30) as resp:
                body = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 401:
                raise ValueError(
                    "Oura API returned 401 Unauthorized. "
                    "Check that /bio-lakehouse/oura-api-token is valid."
                )
            if e.code == 429:
                print(f"Oura API rate limited (429) for {data_type}. Will retry next run.")
                break
            raise RuntimeError(f"Oura API error {e.code} for {data_type}: {e.reason}")

        all_data.extend(body.get("data", []))
        next_token = body.get("next_token")
        if not next_token:
            break

    return all_data
