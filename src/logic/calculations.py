import pandas as pd
import numpy as np
import scipy.stats as stats
import p123api
from pathlib import Path
from typing import Callable, List, Optional, Tuple, Union
from src.data.readers import ParquetDataReader
from src.core.constants import PRICE_COLUMN


def get_dataset_date_range(df: pd.DataFrame) -> Tuple[str, str]:
    """
    Get the date range from dataset with padding for benchmark data.

    Args:
        df: DataFrame with 'Date' column

    Returns:
        Tuple of (start_date, end_date) as strings

    Raises:
        ValueError: If Date column missing or no valid dates
    """
    if 'Date' not in df.columns:
        raise ValueError("Dataset must contain a 'Date' column")

    dates = pd.to_datetime(df['Date'])

    if dates.empty:
        raise ValueError("Dataset contains no valid dates")

    earliest_date = dates.min()
    latest_date = dates.max()

    # Subtract 14 days from earliest to ensure previous week data
    start_date = earliest_date - pd.Timedelta(days=14)

    # Add 14 days to latest to ensure future week data
    end_date = latest_date + pd.Timedelta(days=14)

    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")


def find_prior_trading_day(
    saturday_date: pd.Timestamp,
    benchmark_df: pd.DataFrame
) -> Optional[float]:
    """
    Find the closing price of the last trading day before a Saturday.
    Tries Friday first, then Thursday if Friday not available.

    Args:
        saturday_date: The Saturday date to find prior trading day for
        benchmark_df: DataFrame with benchmark prices

    Returns:
        Closing price or None if not found
    """
    # Try Friday
    friday_date = saturday_date - pd.Timedelta(days=1)
    friday_match = benchmark_df[benchmark_df['dt'] == friday_date]

    if not friday_match.empty:
        return friday_match.iloc[0]['close']

    # Try Thursday
    thursday_date = saturday_date - pd.Timedelta(days=2)
    thursday_match = benchmark_df[benchmark_df['dt'] == thursday_date]

    if not thursday_match.empty:
        return thursday_match.iloc[0]['close']

    return None


def calculate_benchmark_returns(
    raw_data: pd.DataFrame,
    benchmark_data: pd.DataFrame
) -> Tuple[pd.DataFrame, str]:
    """
    Calculate weekly benchmark returns for each date in the dataset.

    Args:
        raw_data: Dataset with 'Date' column
        benchmark_data: Benchmark price data with 'dt' and 'close' columns

    Returns:
        Tuple of (DataFrame with 'benchmark' column added, error message)
    """
    df = raw_data.copy()
    benchmark_df = benchmark_data.copy()

    df['Date'] = pd.to_datetime(df['Date'])
    benchmark_df['dt'] = pd.to_datetime(benchmark_df['dt'])

    df['benchmark'] = float('nan')

    unique_saturdays = df['Date'].unique()

    for saturday in unique_saturdays:
        saturday_ts = pd.Timestamp(saturday)

        current_price = find_prior_trading_day(saturday_ts, benchmark_df)

        if current_price is None:
            continue

        previous_saturday = saturday_ts - pd.Timedelta(days=7)
        previous_price = find_prior_trading_day(previous_saturday, benchmark_df)

        if previous_price is None:
            continue

        weekly_return = (current_price - previous_price) / previous_price

        df.loc[df['Date'] == saturday, 'benchmark'] = weekly_return

    return df, ""


def calculate_future_performance(
    raw_data: pd.DataFrame,
    price_column: str,
) -> pd.DataFrame:
    """
    Calculate future performance for each stock (next week return).

    Args:
        raw_data: DataFrame with Date, Ticker, price columns
        price_column: Name of the price column

    Returns:
        DataFrame with Date, Ticker, and 'Future Perf' columns
    """
    df = raw_data[['Date', 'Ticker', price_column]].copy()

    df['Date'] = pd.to_datetime(df['Date'])
    df[price_column] = pd.to_numeric(df[price_column], errors='coerce')

    # sort by Ticker and Date
    df = df.sort_values(['Ticker', 'Date']).reset_index(drop=True)

    # shift to get next week's values for each ticker
    df['Next_Date'] = df.groupby('Ticker')['Date'].shift(-1)
    df['Next_Price'] = df.groupby('Ticker')[price_column].shift(-1)

    # calculate return only where conditions are true
    valid_mask = (
        (df[price_column].notna()) &
        (df[price_column] != 0) &
        (df['Next_Price'].notna())
    )

    df['Future Perf'] = float('nan')
    df.loc[valid_mask, 'Future Perf'] = (
        (df.loc[valid_mask, 'Next_Price'] - df.loc[valid_mask, price_column]) /
        df.loc[valid_mask, price_column]
    )

    # clean up temporary columns
    df = df.drop(columns=['Next_Date', 'Next_Price', price_column])

    return df


def analyze_factors(
    df: Optional[pd.DataFrame],
    future_perf_df: pd.DataFrame,
    parquet_path: Optional[Union[str, Path]] = None,
    factor_columns: Optional[List[str]] = None,
    top_pct: float = 30.0,
    bottom_pct: float = 30.0,
    progress_fn: Optional[Callable[[int, int, str], None]] = None
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
        progress_fn: Optional callback (completed, total, current_factor) for progress updates

    Returns:
        DataFrame with factor analysis results (Date, factor, ret)
    """
    results = []

    # Ensure future_perf_df Date is datetime
    future_perf_df = future_perf_df.copy()
    future_perf_df['Date'] = pd.to_datetime(future_perf_df['Date'])

    if parquet_path is not None and factor_columns is not None:
        # for parquet files
        total_factors = len(factor_columns)
        reader = ParquetDataReader(parquet_path)

        for idx, col in enumerate(factor_columns, 1):

            # Read only the current factor column plus Date and Ticker (reuse reader)
            factor_df = reader.read_columns(['Date', 'Ticker', col])

            factor_df['Date'] = pd.to_datetime(factor_df['Date'])
            factor_df[col] = pd.to_numeric(factor_df[col], errors='coerce')

            # Merge with future performance
            merged_df = factor_df.merge(future_perf_df, on=['Date', 'Ticker'], how='inner')
            merged_df['Future Perf'] = pd.to_numeric(merged_df['Future Perf'], errors='coerce')

            # Vectorized per-date ranking and aggregation
            grp = merged_df.groupby('Date', sort=False)
            n_in_group = grp[col].transform('size')
            top_n = (n_in_group * (top_pct / 100.0)).astype(int)
            bottom_n = (n_in_group * (bottom_pct / 100.0)).astype(int)

            ranks = grp[col].rank(method='first', ascending=False)
            bottom_threshold = n_in_group - bottom_n

            is_top = ranks <= top_n
            is_bottom = ranks > bottom_threshold

            agg = pd.DataFrame({
                'Date': merged_df['Date'],
                'top_sum': (merged_df['Future Perf'] * is_top).groupby(merged_df['Date']).transform('sum'),
                'bottom_sum': (merged_df['Future Perf'] * is_bottom).groupby(merged_df['Date']).transform('sum'),
                'top_n': top_n,
                'bottom_n': bottom_n,
            })
            reduced = agg.drop_duplicates(subset=['Date'])
            denom = (reduced['top_n'] + reduced['bottom_n']).replace(0, pd.NA)
            ret_values = (reduced['top_sum'] - reduced['bottom_sum']) / denom

            results.extend(
                {'Date': d, 'factor': col, 'ret': v}
                for d, v in zip(reduced['Date'], ret_values)
                if pd.notna(v)
            )

            del factor_df
            del merged_df

            # Report progress
            if progress_fn:
                progress_fn(idx, total_factors, col)
    else:
        # for csv files - merge with future performance
        df = df.copy()
        df['Date'] = pd.to_datetime(df['Date'])
        df_copy = df.merge(future_perf_df, on=['Date', 'Ticker'], how='inner')
        df_copy['Future Perf'] = pd.to_numeric(df_copy['Future Perf'], errors='coerce')

        excluded_columns = ['Date', 'Ticker', 'P123 ID', 'benchmark', 'Future Perf', PRICE_COLUMN]
        numeric_columns = df_copy.select_dtypes(include=[np.number]).columns.tolist()
        factors = [col for col in numeric_columns if col not in excluded_columns]

        total_factors = len(factors)

        for idx, col in enumerate(factors, 1):
            if col in df_copy.columns:
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

            # Report progress
            if progress_fn:
                progress_fn(idx, total_factors, col)

    return pd.DataFrame(results)


def calculate_factor_metrics(
    results_df: pd.DataFrame,
    raw_data: pd.DataFrame
) -> pd.DataFrame:
    """
    Calculate statistical metrics for each factor.
    Computes alpha, beta, t-statistic, and p-value.

    Args:
        results_df: DataFrame with factor returns (Date, factor, ret)
        raw_data: DataFrame with benchmark data from p123 api

    Returns:
        DataFrame with factor metrics
    """
    benchmark = raw_data[['Date', 'benchmark']].drop_duplicates()
    benchmark['Date'] = pd.to_datetime(benchmark['Date'])

    results_df_copy = results_df.copy()
    results_df_copy['Date'] = pd.to_datetime(results_df_copy['Date'])

    merged_data = results_df_copy.merge(benchmark, on='Date', how='inner')

    metrics = []
    unique_factors = results_df['factor'].unique()

    for col in unique_factors:
        subset = merged_data[merged_data['factor'] == col]

        x = subset['benchmark']
        y = subset['ret']

        # Linear regression: return = alpha + beta * benchmark
        beta, alpha = np.polyfit(x, y, deg=1)

        # annualized alpha with weekly data
        ann_alpha = 100 * ((1 + alpha) ** 52 - 1)

        # T-student test
        t_stat, p_value = stats.ttest_1samp(y, popmean=0)

        metrics.append({
            'column': col,
            'T Statistic': t_stat,
            'p-value': p_value,
            'beta': beta,
            'alpha': alpha,
            'annualized alpha %': ann_alpha
        })

    return pd.DataFrame(metrics)


def calculate_correlation_matrix(results_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate correlation matrix between factors.

    Args:
        results_df: DataFrame with factor returns (Date, factor, ret)

    Returns:
        Correlation matrix DataFrame
    """
    pivot_df = results_df.pivot(index='Date', columns='factor', values='ret')
    corr_matrix = pivot_df.corr()

    return corr_matrix


def select_best_features(
    metrics_df: pd.DataFrame,
    correlation_matrix: pd.DataFrame,
    N: int = 20,
    correlation_threshold: float = 0.5,
    a_min: float = 0.5
) -> list:
    """
    Select N best features based on alpha and low correlation.

    Args:
        metrics_df: DataFrame with feature metrics
        correlation_matrix: Correlation matrix of features
        N: Number of features to select
        correlation_threshold: Maximum allowed correlation
        a_min: Minimum absolute annualized alpha %

    Returns:
        List of selected feature names
    """
    # Filter features by alpha threshold (absolute alpha >= a_min)
    filtered_alpha = metrics_df[metrics_df['annualized alpha %'].abs() >= a_min]

    # Sort features by absolute alpha in descending order
    sorted_alpha = filtered_alpha.sort_values(by='annualized alpha %', key=abs, ascending=False)

    # Initialize selected features list
    selected_features = []

    for feature in sorted_alpha['column']:
        # Check correlation with already selected features
        if all(
            abs(correlation_matrix.loc[feature, selected]) < correlation_threshold
            for selected in selected_features
        ):
            selected_features.append(feature)

        if len(selected_features) >= N:
            break

    return selected_features
