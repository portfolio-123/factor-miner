import polars as pl


def calculate_benchmark_returns(dates: pl.LazyFrame, benchmark_prices: pl.LazyFrame) -> pl.LazyFrame:
    """
    Calculate benchmark returns aligned to the given dates.
    dates columns: dt (date)
    benchmark_prices columns: dt (string), close (float)
    """

    # benchmark prices to expected format
    benchmark_prices_lazy = benchmark_prices.with_columns(pl.col("dt").str.to_date("%Y-%m-%d")).drop_nulls(subset=["dt", "close"])

    # align benchmark returns to their date
    dates_with_benchmark = dates.join_asof(benchmark_prices_lazy, on="dt", strategy="forward")

    # use the next price to calculate the benchmark return
    return dates_with_benchmark.select(pl.col("dt"), ((pl.col("close").shift(-1) / pl.col("close") - 1)).alias("ret"))
