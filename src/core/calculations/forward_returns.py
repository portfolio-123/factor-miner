import polars as pl
from src.core.config.constants import INTERNAL_FUTURE_PERF_COL, INTERNAL_BENCHMARK_COL


def calculate_benchmark_returns(dates: pl.DataFrame, benchmark_prices: pl.DataFrame) -> pl.DataFrame:

    # benchmark prices to expected format
    benchmark_prices = benchmark_prices.with_columns(pl.col("dt").str.to_date("%Y-%m-%d")).drop_nulls(subset=["dt", "close"])

    # align benchmark returns to their date
    dates_with_benchmark = dates.join_asof(benchmark_prices, left_on="Date", right_on="dt", strategy="forward").drop("dt")

    # use the next price to calculate the benchmark return
    return dates_with_benchmark.select(pl.col("Date"), ((pl.col("close").shift(-1) / pl.col("close") - 1)).alias(INTERNAL_BENCHMARK_COL))


def add_future_performance_column(df: pl.DataFrame, date_column: str, ticker_column: str, price_column: str) -> pl.DataFrame:
    return df.sort([ticker_column, date_column]).with_columns(
        ((pl.col(price_column).shift(-1).over(ticker_column) / pl.col(price_column).replace(0, None)) - 1)
        .cast(pl.Float32)
        .alias(INTERNAL_FUTURE_PERF_COL)
    )
