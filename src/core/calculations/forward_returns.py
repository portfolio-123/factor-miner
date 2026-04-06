import polars as pl
from src.core.config.constants import (
    INTERNAL_FUTURE_PERF_COL,
    INTERNAL_BENCHMARK_COL,
)


def calculate_benchmark_returns(
    dates: pl.DataFrame, benchmark_prices: pl.DataFrame
) -> pl.DataFrame:

    # benchmark prices to expected format
    benchmark_prices = (
        benchmark_prices.with_columns(pl.col("dt").str.to_date("%Y-%m-%d"))
        .drop_nulls(subset=["dt", "close"])
        .rename({"dt": "Date", "close": "_price"})
    )

    # align benchmark returns to their date
    dates_with_benchmark = dates.join_asof(
        benchmark_prices, on="Date", strategy="forward"
    )

    # use the next price to calculate the benchmark return
    dates_with_benchmark = dates_with_benchmark.with_columns(
        pl.col("_price").shift(-1).alias("_next_price")
    ).with_columns(
        ((pl.col("_next_price") - pl.col("_price")) / pl.col("_price")).alias(
            INTERNAL_BENCHMARK_COL
        )
    )

    return dates_with_benchmark.select(["Date", INTERNAL_BENCHMARK_COL])


def add_future_performance_column(
    df: pl.DataFrame,  # with date, ticker and price column
    price_column: str,
) -> pl.DataFrame:
    return (
        df.sort(["Ticker", "Date"])
        .with_columns(
            pl.col(price_column)
            .shift(-1)
            .over("Ticker")
            .alias(
                "_next_price"
            ),  # add a column that contains the next period's price for each ticker
            pl.col(price_column).replace(
                0, None
            ),  # when a stock goes to 0, set price to null
        )
        .with_columns(
            (
                (pl.col("_next_price") - pl.col(price_column)) / pl.col(price_column)
            ).alias(INTERNAL_FUTURE_PERF_COL),
        )
        .drop("_next_price")
    )
