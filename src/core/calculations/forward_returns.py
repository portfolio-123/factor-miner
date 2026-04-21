import polars as pl

from src.core.config.constants import INTERNAL_BENCHMARK_COL


def calculate_benchmark_returns(dates: pl.LazyFrame, benchmark_prices: pl.LazyFrame) -> pl.LazyFrame:

    # benchmark prices to expected format
    benchmark_prices_lazy = benchmark_prices.with_columns(pl.col("dt").str.to_date("%Y-%m-%d")).drop_nulls(subset=["dt", "close"])

    # align benchmark returns to their date
    dates_with_benchmark = dates.join_asof(benchmark_prices_lazy, left_on="Date", right_on="dt", strategy="forward")

    # use the next price to calculate the benchmark return
    return dates_with_benchmark.select(pl.col("Date"), ((pl.col("close").shift(-1) / pl.col("close") - 1)).alias(INTERNAL_BENCHMARK_COL))
