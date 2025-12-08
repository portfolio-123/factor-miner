from typing import Optional, Tuple

import pandas as pd
import p123api


def _validate_benchmark_data(df: Optional[pd.DataFrame]) -> bool:
    if df is None or df.empty or len(df) == 0:
        return False
    if 'close' not in df.columns or 'dt' not in df.columns:
        return False
    return True


def fetch_benchmark_data(
    benchmark_ticker: str,
    api_key: str,
    start_date: str,
    end_date: str,
    api_id: str | None = None
) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    try:
        client = p123api.Client(api_id=api_id, api_key=api_key)
        benchmark_response = client.data_prices(
            benchmark_ticker,
            start_date,
            end_date,
            False
        )
        benchmark_df = pd.DataFrame(benchmark_response["prices"])

        if not _validate_benchmark_data(benchmark_df):
            return None, f"Benchmark ticker '{benchmark_ticker}' returned invalid data"

        return benchmark_df, None

    except Exception as e:
        error_msg = str(e)
        eml = error_msg.lower()
        if "authentication" in eml or "invalid id/key" in eml or "401" in error_msg:
            return None, "Invalid API Key"
        if "404" in error_msg or "not found" in eml:
            return None, f"Benchmark ticker '{benchmark_ticker}' not found"
        if "timeout" in eml or "connection" in eml:
            return None, "Connection error. Please check your internet connection"
        return None, f"Error: {error_msg}"