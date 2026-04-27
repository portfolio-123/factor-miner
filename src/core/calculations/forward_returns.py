import polars as pl


def calculate_benchmark_returns(dates: pl.LazyFrame, benchmark_prices: pl.LazyFrame) -> pl.LazyFrame:
    """
    Calculate benchmark returns aligned to the given dates.
    dates columns: Date (date)
    benchmark_prices columns: Date (string), Close (float)

    Returns a LazyFrame with columns: Date (date), Return (float)
    """

    # align benchmark returns to their date
    dates_with_benchmark = dates.join_asof(benchmark_prices, on="Date", strategy="forward")

    # use the next price to calculate the benchmark return
    return dates_with_benchmark.select(pl.col("Date"), ((pl.col("Close").shift(-1) / pl.col("Close") - 1)).alias("Return"))
