import pandas as pd
import requests
import p123api

from src.core.environment import API_BASE_URL


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


def create_client(api_id: int, api_key: str) -> p123api.Client:
    return p123api.Client(api_id=str(api_id), api_key=api_key, endpoint=API_BASE_URL)


def validate_credentials(api_id: int, api_key: str) -> bool:
    try:
        create_client(api_id, api_key)
        return True
    except p123api.ClientException:
        return False


def verify_factor_list_access(fl_id: str, access_token: str) -> dict:
    try:
        response = _request("GET", f"/factorList/{fl_id}", token=access_token)
        return response.json()
    except Exception:
        raise PermissionError("Factor List not accessible or invalid session")


def fetch_benchmark_data(
    benchmark_ticker: str, api_id: int, api_key: str, start_date: str, end_date: str
) -> pd.DataFrame:
    try:
        client = create_client(api_id, api_key)
        client.auth()
        data = client.data_prices(
            identifier=benchmark_ticker,
            start=start_date,
            end=end_date,
            to_pandas=False,
        )

        benchmark_df = pd.DataFrame(data["prices"])
        if "date" in benchmark_df.columns and "dt" not in benchmark_df.columns:
            benchmark_df = benchmark_df.rename(columns={"date": "dt"})

        return benchmark_df
    except p123api.ClientException as e:
        raise PermissionError(f"Failed to fetch benchmark data: {e}")
