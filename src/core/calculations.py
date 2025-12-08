import pandas as pd
import numpy as np
import scipy.stats as stats
from typing import Callable, List, Optional, Tuple
from src.services.readers import ParquetDataReader
from src.core.constants import REQUIRED_COLUMNS


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
        DataFrame with 'benchmark' column added
    """
    df = raw_data.copy()
    benchmark_df = benchmark_data.copy()

    df['Date'] = pd.to_datetime(df['Date'])
    benchmark_df['dt'] = pd.to_datetime(benchmark_df['dt'])

    benchmark_df = benchmark_df.sort_values('dt').dropna(subset=['dt', 'close'])

    unique_dates = pd.DataFrame({'Date': df['Date'].unique()}).sort_values('Date')
    unique_dates['Prev_Date'] = unique_dates["Date"] - pd.Timedelta(days=7)

    current_prices = pd.merge_asof(unique_dates, benchmark_df[['dt', 'close']], left_on="Date", right_on="dt", direction="backward", tolerance=pd.Timedelta(days=4)).rename(columns={"close": "curr_price"})

    last_week_prices = pd.merge_asof(current_prices, benchmark_df[['dt', 'close']], left_on="Prev_Date", right_on="dt", direction="backward", tolerance= pd.Timedelta(days=4)).rename(columns={"close": "prev_price"})

    last_week_prices["benchmark"] = (last_week_prices["curr_price"] - last_week_prices["prev_price"]) / last_week_prices["prev_price"]

    result_df = df.merge(last_week_prices[['Date', 'benchmark']], on="Date", how="left")
    return result_df

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
    future_perf_df: pd.DataFrame,
    *,
    df: Optional[pd.DataFrame] = None,
    reader: Optional[ParquetDataReader] = None,
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
        factor_columns: List of factor columns to analyze (for Parquet files)
        top_pct: Percentage of top stocks to include (default: 30.0)
        bottom_pct: Percentage of bottom stocks to include (default: 30.0)
        progress_fn: Optional callback (completed, total, current_factor) for progress updates

    Returns:
        DataFrame with factor analysis results (Date, factor, ret)
    """
    future_perf_df = future_perf_df.copy()
    future_perf_df['Date'] = pd.to_datetime(future_perf_df['Date'])
    future_perf_df['Future Perf'] = pd.to_numeric(future_perf_df['Future Perf'], errors='coerce')

    results: List[dict] = []
    excluded_common = REQUIRED_COLUMNS

    if reader is not None:
        if factor_columns is None:
            columns = reader.get_column_names()
            factor_columns = [c for c in columns if c not in excluded_common]
        total_factors = len(factor_columns)

        for idx, col in enumerate(factor_columns, 1):
            factor_df = reader.read_columns(['Date', 'Ticker', col])
            factor_df['Date'] = pd.to_datetime(factor_df['Date'])
            factor_df[col] = pd.to_numeric(factor_df[col], errors='coerce')

            merged_df = factor_df.merge(future_perf_df, on=['Date', 'Ticker'], how='inner')
            merged_df['Future Perf'] = pd.to_numeric(merged_df['Future Perf'], errors='coerce')

            for date, group in merged_df.groupby('Date', sort=False):
                group = group.dropna(subset=[col, 'Future Perf'])
                if group.empty:
                    continue

                n = len(group)
                top_n = int(n * (top_pct / 100.0))
                bottom_n = int(n * (bottom_pct / 100.0))
                if top_n == 0 or bottom_n == 0:
                    continue

                group_sorted = group.sort_values(by=col, ascending=False)
                top_sum = group_sorted['Future Perf'].iloc[:top_n].sum()
                bottom_sum = group_sorted['Future Perf'].iloc[-bottom_n:].sum()
                value = (top_sum - bottom_sum) / (top_n + bottom_n)

                results.append({'Date': pd.to_datetime(date), 'factor': col, 'ret': value})

            if progress_fn:
                progress_fn(idx, total_factors, col)
    else:
        if df is None:
            return pd.DataFrame(columns=['Date', 'factor', 'ret'])

        df_local = df.copy()
        df_local['Date'] = pd.to_datetime(df_local['Date'])
        merged_df = df_local.merge(future_perf_df, on=['Date', 'Ticker'], how='inner')
        merged_df['Future Perf'] = pd.to_numeric(merged_df['Future Perf'], errors='coerce')

        excluded_columns = excluded_common + ['benchmark', 'Future Perf']
        numeric_columns = merged_df.select_dtypes(include=[np.number]).columns.tolist()
        factors = [c for c in numeric_columns if c not in excluded_columns]

        total_factors = len(factors)
        for idx, col in enumerate(factors, 1):
            if col not in merged_df.columns:
                continue

            merged_df[col] = pd.to_numeric(merged_df[col], errors='coerce')

            for date, group in merged_df.groupby('Date', sort=False):
                group = group.dropna(subset=[col, 'Future Perf'])
                if group.empty:
                    continue

                n = len(group)
                top_n = int(n * (top_pct / 100.0))
                bottom_n = int(n * (bottom_pct / 100.0))
                if top_n == 0 or bottom_n == 0:
                    continue

                group_sorted = group.sort_values(by=col, ascending=False)
                top_sum = group_sorted['Future Perf'].iloc[:top_n].sum()
                bottom_sum = group_sorted['Future Perf'].iloc[-bottom_n:].sum()
                value = (top_sum - bottom_sum) / (top_n + bottom_n)

                results.append({'Date': pd.to_datetime(date), 'factor': col, 'ret': value})

            if progress_fn:
                progress_fn(idx, total_factors, col)

    if not results:
        return pd.DataFrame(columns=['Date', 'factor', 'ret'])
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


