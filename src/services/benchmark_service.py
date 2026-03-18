import polars as pl


# convert p123 benchmark tickers to be compatible with yfinance
MARKET_SUFFIX_MAP = {
    "USA": "",  # US exchanges
    "CAN": ".TO",  # Toronto Stock Exchange
    "DEU": ".DE",  # Germany (XETRA)
    "GBR": ".L",  # London Stock Exchange
    "NLD": ".AS",  # Amsterdam (Euronext)
    "ITA": ".MI",  # Milan (Borsa Italiana)
    "CHE": ".SW",  # Swiss Exchange
}


def convert_to_yfinance_ticker(ticker_with_market: str) -> str:
    if ":" not in ticker_with_market:
        return ticker_with_market

    ticker, market = ticker_with_market.split(":", 1)

    if market not in MARKET_SUFFIX_MAP:
        raise ValueError(
            f"Unsupported market '{market}' for ticker '{ticker_with_market}'. "
            f"Supported markets: {', '.join(MARKET_SUFFIX_MAP.keys())}"
        )

    return f"{ticker}{MARKET_SUFFIX_MAP[market]}"


def fetch_benchmark_external(
    ticker: str, start_date: str, end_date: str
) -> pl.DataFrame:
    import yfinance as yf

    yf_ticker = convert_to_yfinance_ticker(ticker)
    data = yf.download(yf_ticker, start=start_date, end=end_date, progress=False)

    if data.empty:
        raise ValueError(
            f"No data found for '{ticker}' (yfinance: '{yf_ticker}') "
            f"between {start_date} and {end_date}. "
            f"Ticker may not exist on yfinance or may be delisted."
        )

    # yfinance returns pandas, convert
    pandas_df = data[["Close"]].reset_index()
    pandas_df.columns = ["dt", "close"]

    df = pl.from_pandas(pandas_df)
    df = df.with_columns(pl.col("dt").dt.strftime("%Y-%m-%d").alias("dt"))

    return df
