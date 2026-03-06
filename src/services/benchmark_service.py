import pandas as pd
import yfinance as yf


def fetch_benchmark_external(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
    yf_ticker = ticker.split(":")[0] if ":" in ticker else ticker
    data = yf.download(yf_ticker, start=start_date, end=end_date, progress=False)

    if data.empty:
        raise ValueError(f"No data found for ticker {ticker} between {start_date} and {end_date}")

    df = data[["Close"]].reset_index()
    df.columns = ["dt", "close"]
    df["dt"] = pd.to_datetime(df["dt"]).dt.strftime("%Y-%m-%d")

    return df
