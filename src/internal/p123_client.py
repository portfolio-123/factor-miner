import polars as pl
import requests
from typing import Any

from src.core.types.models import P123AuthKeys
from src.internal.config import API_BASE_URL


def _request(
    method: str,
    endpoint: str,
    token: str | None = None,
    timeout=30,
    json: Any = None,
    params: Any = None,
) -> requests.Response:
    headers = {"Content-Type": "application/json", "Source": "FactorMiner"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    response = requests.request(
        method, endpoint, headers=headers, timeout=timeout, json=json, params=params
    )
    response.raise_for_status()
    return response


def authenticate(apiId: int, apiKey: str) -> str:
    try:
        auth_data: P123AuthKeys = {"apiId": apiId, "apiKey": apiKey}
        response = _request("POST", f"{API_BASE_URL}/auth", json=auth_data, timeout=10)
        return response.text.strip('"')
    except Exception:
        raise PermissionError("Authentication failed")


def fetch_benchmark_data(
    benchmark_ticker: str, access_token: str, start_date: str, end_date: str
) -> pl.DataFrame:
    try:
        response = _request(
            "GET",
            f"{API_BASE_URL}/data/prices/{benchmark_ticker}",
            token=access_token,
            params={"start": start_date, "end": end_date},
        )
        data = response.json()

        return pl.DataFrame(data["prices"]).select(["dt", "close"])
    except Exception as e:
        raise PermissionError("Failed to fetch benchmark data: " + str(e))
