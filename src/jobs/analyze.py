"""
Standalone analyze_factors implementation for background worker.
This version uses a callback for logging instead of Streamlit's add_debug_log.
"""
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Optional, Union, Callable, List


def analyze_factors_standalone(
    df: Optional[pd.DataFrame],
    future_perf_df: pd.DataFrame,
    parquet_path: Optional[Union[str, Path]] = None,
    factor_columns: Optional[List[str]] = None,
    top_pct: float = 30.0,
    bottom_pct: float = 30.0,
    log_fn: Optional[Callable[[str], None]] = None
) -> pd.DataFrame:
    """
    Analyze factors by calculating top X% vs bottom X% performance difference.
    For Parquet files, reads factors one by one to reduce memory usage.

    Args:
        df: DataFrame with factor columns (for CSV files, without Future Perf)
        future_perf_df: Pre-calculated future performance (Date, Ticker, Future Perf)
        parquet_path: Path to parquet file (for Parquet files)
        factor_columns: List of factor columns to analyze (for Parquet files)
        top_pct: Percentage of top stocks to include (default: 30.0)
        bottom_pct: Percentage of bottom stocks to include (default: 30.0)
        log_fn: Optional logging function (prints to stdout if None)

    Returns:
        DataFrame with factor analysis results (Date, factor, ret)
    """
    def log(msg: str) -> None:
        if log_fn:
            log_fn(msg)
        else:
            print(msg, flush=True)

    results = []

    if parquet_path is not None and factor_columns is not None:
        # Import here to avoid circular imports
        from src.data.readers import ParquetDataReader

        total_factors = len(factor_columns)

        for idx, col in enumerate(factor_columns, 1):
            log(f'Analyzing factor {idx}/{total_factors}: {col}')

            # Read only the current factor column plus Date and Ticker
            reader = ParquetDataReader(parquet_path)
            factor_df = reader.read_columns(['Date', 'Ticker', col])

            factor_df['Date'] = pd.to_datetime(factor_df['Date'])
            factor_df[col] = pd.to_numeric(factor_df[col], errors='coerce')

            # Merge with future performance
            merged_df = factor_df.merge(future_perf_df, on=['Date', 'Ticker'], how='inner')
            merged_df['Future Perf'] = pd.to_numeric(merged_df['Future Perf'], errors='coerce')

            # Analyze by date
            for date, group in merged_df.groupby('Date'):
                group_sorted = group.sort_values(by=col, ascending=False)

                n = len(group_sorted)
                top_n = int(n * (top_pct / 100.0))
                bottom_n = int(n * (bottom_pct / 100.0))

                if top_n == 0 or bottom_n == 0:
                    continue

                top_stocks = group_sorted.iloc[:top_n]
                bottom_stocks = group_sorted.iloc[-bottom_n:]

                top_sum = top_stocks['Future Perf'].sum()
                bottom_sum = bottom_stocks['Future Perf'].sum()

                value = (top_sum - bottom_sum) / (top_n + bottom_n)
                results.append({'Date': date, 'factor': col, 'ret': value})

            del factor_df
            del merged_df
    else:
        # for csv files - merge with future performance
        df_copy = df.merge(future_perf_df, on=['Date', 'Ticker'], how='inner')

        df_copy['Date'] = pd.to_datetime(df_copy['Date'])
        df_copy['Future Perf'] = pd.to_numeric(df_copy['Future Perf'], errors='coerce')

        excluded_columns = ['Date', 'Ticker', 'P123 ID', 'benchmark', 'Future Perf']
        numeric_columns = df_copy.select_dtypes(include=[np.number]).columns.tolist()
        factors = [col for col in numeric_columns if col not in excluded_columns]

        total_factors = len(factors)

        for idx, col in enumerate(factors, 1):
            if col in df_copy.columns:
                log(f'Analyzing factor {idx}/{total_factors}: {col}')

                df_copy[col] = pd.to_numeric(df_copy[col], errors='coerce')

                for date, group in df_copy.groupby('Date'):
                    group_sorted = group.sort_values(by=col, ascending=False)

                    n = len(group_sorted)
                    top_n = int(n * (top_pct / 100.0))
                    bottom_n = int(n * (bottom_pct / 100.0))

                    if top_n == 0 or bottom_n == 0:
                        continue

                    top_stocks = group_sorted.iloc[:top_n]
                    bottom_stocks = group_sorted.iloc[-bottom_n:]

                    top_sum = top_stocks['Future Perf'].sum()
                    bottom_sum = bottom_stocks['Future Perf'].sum()

                    value = (top_sum - bottom_sum) / (top_n + bottom_n)
                    results.append({'Date': date, 'factor': col, 'ret': value})

    return pd.DataFrame(results)
