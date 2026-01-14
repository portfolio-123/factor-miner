from typing import Optional
import os

import pandas as pd
import requests


API_BASE_URL = os.getenv("API_BASE_URL")


def _validate_benchmark_data(df: Optional[pd.DataFrame]) -> bool:
    if df is None or df.empty:
        return False
    if "close" not in df.columns or "dt" not in df.columns:
        return False
    return True


def authenticate(api_id: str, api_key: str) -> Optional[str]:
    try:
        url = f"{API_BASE_URL}/auth"

        payload = {"apiId": api_id, "apiKey": api_key}
        headers = {"Content-Type": "application/json"}
        response = requests.post(url, json=payload, headers=headers, timeout=10)
        if response.status_code == 200:
            return response.text.strip('"')

        return None
    except Exception:
        return None


def verify_factor_list_access(factor_list_uid: str, access_token: str) -> dict:
    try:
        url = f"{API_BASE_URL}/factorList/{factor_list_uid}"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Source": "0",
        }
        response = requests.get(url, headers=headers, timeout=30)

        if response.status_code == 401:
            raise ValueError("Session expired, please re-authenticate")
        if response.status_code == 404:
            raise ValueError("Factor List not accessible or not found")
        if response.status_code != 200:
            raise ValueError(f"Error: {response.status_code} - {response.text}")

        return response.json()
    except requests.exceptions.ConnectionError:
        raise ValueError("Connection refused")
    except ValueError:
        raise
    except Exception as e:
        raise ValueError(f"Error: {str(e)}")


def fetch_benchmark_data(
    benchmark_ticker: str,
    access_token: str,
    start_date: str,
    end_date: str,
) -> pd.DataFrame:
    try:
        if not access_token:
            raise ValueError("Missing access token")

        url = f"{API_BASE_URL}/data/prices/{benchmark_ticker}"

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Source": "0",
        }

        params = {"start": start_date, "end": end_date}

        response = requests.get(url, headers=headers, params=params, timeout=30)

        if response.status_code == 401:
            raise ValueError("401 Unauthorized: Invalid Token or Credentials.")

        if response.status_code == 403:
            raise ValueError(
                f"403 Forbidden: Access denied for ticker '{benchmark_ticker}'."
            )

        if response.status_code != 200:
            raise ValueError(
                f"Error fetching data: {response.status_code} - {response.text}"
            )

        data = response.json()

        benchmark_df = pd.DataFrame(data["prices"])
        if "date" in benchmark_df.columns and "dt" not in benchmark_df.columns:
            benchmark_df = benchmark_df.rename(columns={"date": "dt"})

        if not _validate_benchmark_data(benchmark_df):
            raise ValueError(
                f"Benchmark ticker '{benchmark_ticker}' returned invalid data"
            )

        return benchmark_df

    except requests.exceptions.ConnectionError:
        raise ValueError("Connection refused")
    except ValueError:
        raise
    except Exception as e:
        raise ValueError(f"Error: {str(e)}")
