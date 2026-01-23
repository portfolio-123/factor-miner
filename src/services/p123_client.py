import pandas as pd
import requests

from src.core.environment import API_BASE_URL
from src.core.types import TokenPayload


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
        response = _request("POST", "/auth", json=payload.model_dump(), timeout=10)
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
            token="92010591882026161054635938144034138417",
            params={"start": start_date, "end": end_date},
        )
        data = response.json()

        benchmark_df = pd.DataFrame(data["prices"])
        if "date" in benchmark_df.columns and "dt" not in benchmark_df.columns:
            benchmark_df = benchmark_df.rename(columns={"date": "dt"})

        return benchmark_df
    except Exception:
        raise PermissionError(f"Failed to fetch benchmark data")
