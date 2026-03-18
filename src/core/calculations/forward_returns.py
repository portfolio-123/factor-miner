import numpy as np
import polars as pl

from src.core.types.models import Frequency
from src.core.config.constants import (
    INTERNAL_FUTURE_PERF_COL,
    INTERNAL_BENCHMARK_COL,
)


def calculate_benchmark_returns(
    raw_data: pl.DataFrame, benchmark_data: pl.DataFrame, frequency: Frequency
) -> pl.DataFrame:
    """
    Calculate forward-looking benchmark returns for each date in the dataset.
    Returns the benchmark return from the first benchmark trading day on/after the
    current rebalance date to the first benchmark trading day on/after the next rebalance date.

    Args:
        raw_data: Dataset with 'Date' column
        benchmark_data: Benchmark price data with 'dt' and 'close' columns
        frequency: Dataset frequency to determine the forward period

    Returns:
        DataFrame with 'benchmark' column added
    """
    benchmark_df = (
        benchmark_data.with_columns(pl.col("dt").str.to_date("%Y-%m-%d").alias("dt"))
        .drop_nulls(subset=["dt", "close"])
        .sort("dt")
    )

    # benchmark_df is expected to contain benchmark trading dates
    benchmark_dates = benchmark_df["dt"].to_numpy()
    close_prices = benchmark_df["close"].to_numpy()

    # get unique rebalance dates from raw_data
    raw_data = raw_data.with_columns(pl.col("Date").str.to_date("%Y-%m-%d"))
    unique_date_values = raw_data["Date"].unique().sort().to_numpy()

    # find first trading day >= each rebalance date (monday, tuesday, etc.)
    start_positions = np.searchsorted(benchmark_dates, unique_date_values, side="left")

    # for end date, use the start date of the next period
    n_dates = len(unique_date_values)
    end_positions = np.zeros(n_dates, dtype=int)

    for i in range(n_dates - 1):
        # end position is the start position of the next period
        next_dataset_date = unique_date_values[i + 1]
        end_positions[i] = np.searchsorted(
            benchmark_dates, next_dataset_date, side="left"
        )

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

    bench_df = pl.DataFrame(
        {"Date": unique_date_values, INTERNAL_BENCHMARK_COL: benchmark_returns}
    )

    result = raw_data.join(bench_df, on="Date", how="left")
    return result


def calculate_future_performance(
    raw_data: pl.DataFrame,
    price_column: str,
) -> pl.DataFrame:
    """
    Add a column for future performance to the dataframe, which is the return of the next period for that same stock.
    (e.g. 0.07, or 7%)

    Args:
        raw_data: DataFrame with Date, Ticker, price columns
        price_column: Name of the price column (next period's price, e.g. Close(-1))

    Returns:
        DataFrame with Date, Ticker, and internal future perf column
    """
    df = raw_data.select(["Date", "Ticker", price_column]).sort(["Ticker", "Date"])

    df = df.with_columns(
        pl.col(price_column).shift(-1).over("Ticker").alias("_next_price")
    )

    # treat zero current prices as null
    df = df.with_columns(
        pl.when(pl.col(price_column) == 0)
        .then(None)
        .otherwise(pl.col(price_column))
        .alias(price_column)
    )

    # Forward return: (next_price - current_price) / current_price
    df = df.with_columns(
        ((pl.col("_next_price") - pl.col(price_column)) / pl.col(price_column)).alias(
            INTERNAL_FUTURE_PERF_COL
        )
    )

    df = df.drop([price_column, "_next_price"])

    return df
