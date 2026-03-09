import logging
import numpy as np
import pandas as pd

from src.core.types.models import Frequency
from src.core.config.constants import (
    INTERNAL_FUTURE_PERF_COL,
    INTERNAL_BENCHMARK_COL,
)

logger = logging.getLogger("calculations")


def calculate_benchmark_returns(
    raw_data: pd.DataFrame,
    benchmark_data: pd.DataFrame,
    frequency: Frequency,
) -> pd.DataFrame:
    """
    Calculate forward-looking benchmark returns for each date in the dataset.
    Returns the benchmark return from current period's transaction date to next period's transaction date.

    Args:
        raw_data: Dataset with 'Date' column
        benchmark_data: Benchmark price data with 'dt' and 'close' columns
        frequency: Dataset frequency to determine the forward period

    Returns:
        DataFrame with 'benchmark' column added
    """
    benchmark_data["dt"] = pd.to_datetime(benchmark_data["dt"])
    benchmark_df = benchmark_data.dropna(subset=["dt", "close"]).sort_values("dt").reset_index(drop=True)

    # benchmark_df only contains trading days
    benchmark_dates = benchmark_df["dt"].values
    close_prices = benchmark_df["close"].values

    # get unique dates from raw_data (saturday rebalance dates)
    unique_date_values = pd.to_datetime(raw_data["Date"].unique())
    unique_date_values = np.sort(unique_date_values)

    # find the next trading day
    # side="left" finds first trading day >= the Saturday date
    start_positions = np.searchsorted(benchmark_dates, unique_date_values, side="left")

    # For end date, use the start date of the next period
    n_dates = len(unique_date_values)
    end_positions = np.zeros(n_dates, dtype=int)

    for i in range(n_dates - 1):
        # end position is the start position of the next period
        next_dataset_date = unique_date_values[i + 1]
        end_positions[i] = np.searchsorted(benchmark_dates, next_dataset_date, side="left")

    # for the last date, estimate using frequency (no next dataset date available)
    if n_dates > 0:
        forward_trading_days = frequency.weeks * 5
        end_positions[-1] = start_positions[-1] + forward_trading_days

    # Valid if start position is within range
    valid_mask = start_positions < len(close_prices)

    # Clip positions to valid range
    last_idx = len(close_prices) - 1
    start_positions_clipped = np.clip(start_positions, 0, last_idx)
    end_positions_clipped = np.clip(end_positions, 0, last_idx)

    # Get prices at positions
    curr_prices = np.where(valid_mask, close_prices[start_positions_clipped], np.nan)
    next_prices = np.where(valid_mask, close_prices[end_positions_clipped], np.nan)

    # Forward return: (next_price - curr_price) / curr_price
    benchmark_returns = (next_prices - curr_prices) / curr_prices

    bench_df = pd.DataFrame({
        "Date": unique_date_values,
        INTERNAL_BENCHMARK_COL: benchmark_returns
    })

    result = raw_data.merge(bench_df, on="Date", how="left")
    return result


def calculate_future_performance(
    raw_data: pd.DataFrame,
    price_column: str,
) -> pd.DataFrame:
    """
    Add a column for future performance to the dataframe, which is the return of the next period for that same stock.
    (e.g. 0.07, or 7%)

    Args:
        raw_data: DataFrame with Date, Ticker, price columns
        price_column: Name of the price column (next period's price, e.g. Close(-1))

    Returns:
        DataFrame with Date, Ticker, and internal future perf column
    """
    df = raw_data[["Date", "Ticker", price_column]].copy()
    df = df.sort_values(["Ticker", "Date"]).reset_index(drop=True)

    next_price = df.groupby("Ticker")[price_column].shift(-1)

    # Set current price=0 to NaN
    df.loc[df[price_column] == 0, price_column] = np.nan

    # Calculate return - if 0, capture -100% return
    df[INTERNAL_FUTURE_PERF_COL] = (next_price - df[price_column]) / df[price_column]

    # Replace inf with nan (happens when price = 0) and warn
    inf_mask = np.isinf(df[INTERNAL_FUTURE_PERF_COL])
    if inf_mask.any():
        df["_next_price"] = next_price
        bad_df = df[inf_mask][["Date", "Ticker", price_column, "_next_price"]].head(5)
        logger.warning(f"Found {inf_mask.sum()} inf values in future perf (division by zero):")
        for _, row in bad_df.iterrows():
            logger.warning(f"  {row['Date']} | {row['Ticker']} | price={row[price_column]} | next_price={row['_next_price']}")
        df = df.drop(columns=["_next_price"])
        df[INTERNAL_FUTURE_PERF_COL] = df[INTERNAL_FUTURE_PERF_COL].replace([np.inf, -np.inf], np.nan)

    df = df.drop(columns=[price_column])

    return df
