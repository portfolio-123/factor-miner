import polars as pl

from src.core.types.models import APICredentials
from src.internal.config import API_BASE_URL


import p123api


def authenticate(apiId: int, apiKey: str) -> None:
    try:
        client = p123api.Client(api_id=str(apiId), api_key=apiKey, endpoint=API_BASE_URL)
        client.auth()
    except Exception as e:
        raise PermissionError("Authentication failed", e)


def fetch_benchmark_data(benchmark_ticker: str, api_credentials: APICredentials, start_date: str, end_date: str) -> pl.DataFrame:
    try:
        client = p123api.Client(**api_credentials, endpoint=API_BASE_URL)
        data = client.data_prices(benchmark_ticker, start_date, end_date)

        return pl.DataFrame(data["prices"]).select(["dt", "close"])
    except Exception as e:
        raise PermissionError("Failed to fetch benchmark data: " + str(e))
