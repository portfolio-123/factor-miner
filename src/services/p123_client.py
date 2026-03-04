import pandas as pd
import requests

from src.core.config.environment import API_BASE_URL
from src.core.types.models import TokenPayload


def _request(
    method: str, endpoint: str, token: str | None = None, timeout: int = 30, **kwargs
) -> requests.Response:
    headers = {"Content-Type": "application/json", "Source": "0"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    response = requests.request(
        method, f"{API_BASE_URL}{endpoint}", headers=headers, timeout=timeout, **kwargs
    )
    response.raise_for_status()
    return response


def authenticate(payload: TokenPayload) -> str:
    try:
        auth_data = {"apiId": payload.apiId, "apiKey": payload.apiKey}
        response = _request("POST", "/auth", json=auth_data, timeout=10)
        return response.text.strip('"')
    except Exception:
        raise PermissionError("Authentication failed")


def verify_factor_list_access(fl_id: str, access_token: str) -> dict:
    try:
        response = _request("GET", f"/factorList/{fl_id}", token=access_token)
        return response.json()
    except Exception:
        raise PermissionError("Factor List not accessible or invalid session")


def fetch_benchmark_data(
    benchmark_ticker: str, access_token: str, start_date: str, end_date: str
) -> pd.DataFrame:
    try:
        response = _request(
            "GET",
            f"/data/prices/{benchmark_ticker}",
            token=access_token,
            params={"start": start_date, "end": end_date},
        )
        data = response.json()

        return pd.DataFrame(data["prices"])[["dt", "close"]]
    except Exception as e:
        raise PermissionError("Failed to fetch benchmark data: " + str(e))
