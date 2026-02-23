import logging
import time
import pandas as pd
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Dict, List, Optional, Tuple

logger = logging.getLogger("calculations")
from src.services.dataset_service import DatasetService
from src.core.types.models import Frequency
from src.core.config.constants import (
    INTERNAL_FUTURE_PERF_COL,
    INTERNAL_BENCHMARK_COL,
    DEFAULT_CORRELATION_THRESHOLD,
    DEFAULT_MIN_ALPHA,
    DEFAULT_MAX_NA_PCT,
    DEFAULT_MIN_IC,
    DEFAULT_N_FACTORS,
)


def calculate_benchmark_returns(
    raw_data: pd.DataFrame,
    benchmark_data: pd.DataFrame,
    frequency: Frequency,
) -> pd.DataFrame:
    """
    Calculate benchmark returns for each date in the dataset based on data frequency.

    Args:
        raw_data: Dataset with 'Date' column
        benchmark_data: Benchmark price data with 'dt' and 'close' columns
        frequency: Dataset frequency to determine the lookback period

    Returns:
        DataFrame with 'benchmark' column added
    """
    benchmark_data["dt"] = pd.to_datetime(benchmark_data["dt"])
    benchmark_df = benchmark_data.dropna(subset=["dt", "close"])

    # benchmark_df only contains trading days
    close_prices = benchmark_df["close"].values

    # 5 trading days per week
    lookback_trading_days = frequency.weeks * 5

    # get unique dates from raw_data
    unique_date_values = raw_data["Date"].unique()

    # for each date, find the next trading day (get monday, but if holiday get tuesday)
    date_positions = np.searchsorted(benchmark_df["dt"].values, unique_date_values, side="left")

    prev_positions = date_positions - lookback_trading_days

    valid_mask = (date_positions < len(close_prices)) & (prev_positions >= 0)

    # get prices at positions, or NaN if invalid. clip/maximum prevent bad index errors.
    curr_prices = np.where(valid_mask, close_prices[np.clip(date_positions, 0, len(close_prices) - 1)], np.nan)
    prev_prices = np.where(valid_mask, close_prices[np.maximum(prev_positions, 0)], np.nan)

    benchmark_returns = (curr_prices - prev_prices) / prev_prices

    bench_df = pd.DataFrame({
        "Date": unique_date_values,
        INTERNAL_BENCHMARK_COL: benchmark_returns
    })
    
    result = raw_data.merge(bench_df, on="Date", how="left")
    return result


def calculate_future_performance(
    raw_data: pd.DataFrame,
    price_column: str,
) -> pd.DataFrame:
    """
    Add a column for future performance to the dataframe, which is the return of the next period for that same stock.
    (e.g. 0.07, or 7%)

    Args:
        raw_data: DataFrame with Date, Ticker, price columns
        price_column: Name of the price column

    Returns:
        DataFrame with Date, Ticker, and internal future perf column
    """
    df = raw_data[["Date", "Ticker", price_column]].copy()
    df = df.sort_values(["Ticker", "Date"]).reset_index(drop=True)

    # calculate return as (next_price - current_price) / current_price
    next_price = df.groupby("Ticker")[price_column].shift(-1)
    df[INTERNAL_FUTURE_PERF_COL] = (next_price - df[price_column]) / df[price_column]
    df = df.drop(columns=[price_column])

    return df


def analyze_factors(
    future_perf_df: pd.DataFrame,
    dataset_svc: DatasetService,
    *,
    factor_columns: Optional[List[str]] = None,
    top_pct: float = 30.0,
    bottom_pct: float = 30.0,
    progress_fn: Optional[Callable[[int, int], None]] = None,
    batch_size: int = 50,
) -> Tuple[pd.DataFrame, Dict[str, dict]]:
    """
    Analyze factors by calculating top X% vs bottom X% performance difference.
    Reads factors in batches from Parquet.

    Args:
        future_perf_df: Pre-calculated future performance (Date, Ticker, internal future perf column)
        dataset_svc: DatasetService for reading factor data
        factor_columns: List of factor columns to analyze (auto-detected if None)
        top_pct: Percentage of top stocks to include (default: 20.0)
        bottom_pct: Percentage of bottom stocks to include (default: 20.0)
        progress_fn: Optional callback (completed, total, current_factor) for progress updates
        batch_size: Number of factors to read per batch (default: 50)

    Returns:
        Tuple of (DataFrame with factor analysis results, dict of factor stats per factor including NA%, IC, IC t-stat)
    """
    logger.info(f"analyze_factors: starting with {len(factor_columns)} factors")
    total_factors = len(factor_columns)

    all_dates: List[np.ndarray] = []
    all_factors: List[np.ndarray] = []
    all_rets: List[np.ndarray] = []
    factor_stats_dict: Dict[str, dict] = {}

    # read date/ticker once and track row indices
    logger.info("analyze_factors: reading Date/Ticker columns")
    base_df = dataset_svc.read_columns(["Date", "Ticker"])
    logger.info(f"analyze_factors: read {len(base_df)} rows")

    base_df["_row_idx"] = np.arange(len(base_df))

    # add future performance column to base dataframe. TODO: determine how to handle missing values, or stocks exiting/entering the universe
    logger.info("analyze_factors: merging with future_perf_df")
    merged_base = base_df.merge(future_perf_df, on=["Date", "Ticker"], how="inner")
    valid_indices = merged_base["_row_idx"].to_numpy()
    perf_arr = merged_base[INTERNAL_FUTURE_PERF_COL].to_numpy()
    logger.info(f"analyze_factors: merged, {len(merged_base)} rows")

    # the inverse checks the row and maps it to an index. all the rows with the first date of the period will be index 0, and so on.
    unique_dates, date_inverse = np.unique(merged_base["Date"], return_inverse=True)
    perf_valid = ~np.isnan(perf_arr)

    n_dates = len(unique_dates)
    logger.info(f"analyze_factors: {n_dates} unique dates")
    del base_df, merged_base

    def process_factor(col: str) -> Tuple[np.ndarray, np.ndarray, np.ndarray, dict]:
        factor_arr = batch_df[col].to_numpy()[valid_indices]

        # Calculate NA stats
        total_values = len(factor_arr)
        na_count = np.isnan(factor_arr).sum()
        na_pct = (na_count / total_values * 100) if total_values > 0 else 100.0

        valid_mask = perf_valid & ~np.isnan(factor_arr)

        # filter to valid rows only
        inverse_f = date_inverse[valid_mask]
        factor_f = factor_arr[valid_mask]
        perf_f = perf_arr[valid_mask]

        # count how many stocks are per date
        counts = np.bincount(inverse_f, minlength=n_dates)

        # Sort by (date, factor)
        sort_keys = np.lexsort((factor_f, inverse_f))
        inverse_sorted = inverse_f[sort_keys]
        perf_sorted = perf_f[sort_keys]

        # Compute period boundaries
        group_starts = np.zeros(n_dates + 1, dtype=np.int64)
        group_starts[1:] = np.cumsum(counts)
        # get position of each row within its period
        rank_in_group = np.arange(len(inverse_sorted)) - group_starts[inverse_sorted]
        group_sizes = counts[inverse_sorted]
        group_sizes_f = group_sizes.astype(np.float64)
        # calculate percentile rank of each row within its period. +0.5 to get the middle of the range - e.g. 1/3 => 1.5/3=0.5. 
        f_rank = (rank_in_group + 0.5) / group_sizes_f

        # sort by (date, perf)
        perf_order_within_date = np.lexsort((perf_sorted, inverse_sorted))
        perf_rank_pos = np.empty_like(perf_order_within_date)
        perf_rank_pos[perf_order_within_date] = np.arange(len(perf_order_within_date))
        # Adjust to within-group position
        perf_rank_in_group = perf_rank_pos - group_starts[inverse_sorted]
        r_rank = (perf_rank_in_group + 0.5) / group_sizes_f

        # Tail weights: w = 1 + alpha * |rank - 0.5|
        alpha = 4.0
        weights = 1.0 + alpha * np.abs(f_rank - 0.5)

        # Weighted Spearman correlation (IC) per date.
        # sum of weights per date, avoid div by zero
        w_sum = np.bincount(inverse_sorted, weights=weights, minlength=n_dates)
        w_sum = np.where(w_sum > 0, w_sum, 1.0)

        # weighted mean of factor and perf ranks per date
        mean_f = np.bincount(inverse_sorted, weights=weights * f_rank, minlength=n_dates) / w_sum
        mean_r = np.bincount(inverse_sorted, weights=weights * r_rank, minlength=n_dates) / w_sum

        # center ranks by subtracting their date's mean
        f_centered = f_rank - mean_f[inverse_sorted]
        r_centered = r_rank - mean_r[inverse_sorted]

        # weighted covariance and variances per date
        cov_fr = np.bincount(inverse_sorted, weights=weights * f_centered * r_centered, minlength=n_dates) / w_sum
        var_f = np.bincount(inverse_sorted, weights=weights * f_centered ** 2, minlength=n_dates) / w_sum
        var_r = np.bincount(inverse_sorted, weights=weights * r_centered ** 2, minlength=n_dates) / w_sum

        # IC = cov / (std_f * std_r), require at least 4 stocks
        denom = np.sqrt(var_f * var_r)
        ic_per_date = np.where((denom > 0) & (counts >= 4), cov_fr / denom, np.nan)

        # aggregate: mean IC across all valid dates
        valid_ic_mask = (counts >= 4) & ~np.isnan(ic_per_date) & (np.abs(ic_per_date) < 1.0)
        valid_ic = ic_per_date[valid_ic_mask]
        n_ic = len(valid_ic)
        mean_ic = np.mean(valid_ic) if n_ic > 0 else np.nan

        # IC t-statistic: mean(IC) / (std(IC) / sqrt(n))
        if n_ic > 1:
            std_ic = np.std(valid_ic, ddof=1)
            ic_tstat = mean_ic / (std_ic / np.sqrt(n_ic)) if std_ic > 0 else np.nan
        else:
            ic_tstat = np.nan

        factor_stats = {
            'na_pct': round(na_pct, 2),
            'ic': mean_ic,
            'ic_tstat': ic_tstat,
        }

        # considering how many stocks per date (counts), calculate what topx% and bottomx% translate to.
        top_n = (counts * (top_pct / 100.0)).astype(np.int64)
        bottom_n = (counts * (bottom_pct / 100.0)).astype(np.int64)

        # expand top/bottom counts per stock
        top_n_expanded = top_n[inverse_sorted]
        bottom_n_expanded = bottom_n[inverse_sorted]

        # genereates array of bools. True if stock is in bottom x%, false if not.
        is_bottom = rank_in_group < bottom_n_expanded
        is_top = rank_in_group >= (group_sizes - top_n_expanded)

        # np.where so we only keep returns from top/bottom stocks. else add 0. np.bincount to sum values per date.
        top_sums = np.bincount(
            inverse_sorted, weights=np.where(is_top, perf_sorted, 0.0), minlength=n_dates
        )
        bottom_sums = np.bincount(
            inverse_sorted, weights=np.where(is_bottom, perf_sorted, 0.0), minlength=n_dates
        )

        # total number of stocks used for the calculation per date.
        divisor = top_n + bottom_n
        # only grab dates where there are top and bottom stocks.
        valid_date_mask = divisor > 0
        # create array with nans, but with length as the amount of dates.
        ret = np.full(n_dates, np.nan)
        # calculate values only for valid dates. remains as nan if invalid.
        ret[valid_date_mask] = (
            (top_sums[valid_date_mask] - bottom_sums[valid_date_mask]) / divisor[valid_date_mask]
        )

        # build results as arrays instead of dicts
        valid_results = valid_date_mask & ~np.isnan(ret)
        valid_indices_arr = np.where(valid_results)[0]

        return (
            unique_dates[valid_indices_arr],
            np.full(len(valid_indices_arr), col, dtype=object),
            ret[valid_indices_arr],
            factor_stats,
        )

    logger.info("analyze_factors: entering ThreadPoolExecutor")
    with ThreadPoolExecutor() as executor:
        logger.info("analyze_factors: ThreadPoolExecutor started")
        completed_count = 0
        last_progress_time = 0.0
        progress_interval = 3.0  # throttle progress updates to every 3 seconds

        # Process factors in batches
        for batch_start in range(0, total_factors, batch_size):
            batch_cols = factor_columns[batch_start : batch_start + batch_size]
            logger.info(f"analyze_factors: reading batch {batch_start}, {len(batch_cols)} columns")

            batch_df = dataset_svc.read_columns(batch_cols)
            logger.info(f"analyze_factors: batch read complete, {len(batch_df)} rows")

            # executor.submit() schedules process_factor(col) to run in a background thread.
            # It returns a Future object immediately (non-blocking).
            # We build a dict mapping Future -> column name so we can identify results later.
            future_to_col = {
                executor.submit(process_factor, col): col for col in batch_cols
            }

            # as_completed() yields futures sa they finish.
            for future in as_completed(future_to_col):
                # Look up which column this future was processing
                col = future_to_col[future]
                # future.result() blocks until the task completes and returns the result.
                # If the task raised an exception, .result() re-raises it here.
                dates_arr, factors_arr, rets_arr, factor_stats = future.result()
                factor_stats_dict[col] = factor_stats
                if len(dates_arr) > 0:
                    all_dates.append(dates_arr)
                    all_factors.append(factors_arr)
                    all_rets.append(rets_arr)

                # Update progress but throttle
                completed_count += 1
                if progress_fn:
                    now = time.monotonic()
                    is_complete = completed_count == total_factors
                    if is_complete or (now - last_progress_time) >= progress_interval:
                        progress_fn(completed_count, total_factors)
                        last_progress_time = now

            del batch_df

    # concatenate all arrays and build DataFrame once
    if all_dates:
        return (
            pd.DataFrame({
                "Date": np.concatenate(all_dates),
                "factor": np.concatenate(all_factors),
                "ret": np.concatenate(all_rets),
            }),
            factor_stats_dict,
        )
    else:
        return pd.DataFrame(columns=["Date", "factor", "ret"]), factor_stats_dict


def calculate_factor_metrics(
    results_df: pd.DataFrame,
    raw_data: pd.DataFrame,
    periods_per_year: int = 52,
    factor_stats: Optional[Dict[str, dict]] = None,
) -> pd.DataFrame:
    """
    Calculate statistical metrics for each factor using vectorized operations.
    Computes alpha, beta, and t-statistic.

    Args:
        results_df: DataFrame with factor returns (Date, factor, ret)
        raw_data: DataFrame with benchmark data from p123 api
        periods_per_year: Number of periods per year for annualization (default: 52 for weekly)
        factor_stats: Optional dict mapping factor names to their statistics (NA%, IC, IC t-stat)

    Returns:
        DataFrame with factor metrics including NA %, IC, and IC t-stat
    """
    # get unique benchmark values per date
    benchmark = raw_data[["Date", INTERNAL_BENCHMARK_COL]].drop_duplicates()

    # merge benchmark with factor returns
    merged_data = results_df.merge(benchmark, on="Date", how="inner")

    # filter invalid values upfront
    valid_mask = np.isfinite(merged_data["ret"]) & np.isfinite(merged_data[INTERNAL_BENCHMARK_COL])
    merged_data = merged_data[valid_mask]

    # pivot to get factors as columns: (n_dates, n_factors)
    pivot = merged_data.pivot_table(index="Date", columns="factor", values="ret", aggfunc="first")
    factor_names = pivot.columns.tolist()
    y = pivot.values  # (n_dates, n_factors)

    # get benchmark values aligned with pivot index
    bench_aligned = benchmark.set_index("Date").reindex(pivot.index)[INTERNAL_BENCHMARK_COL]
    x = bench_aligned.values[:, np.newaxis]  # (n_dates, 1)

    valid = ~np.isnan(y)
    n_valid = valid.sum(axis=0)  # count per factor

    # mask out NaN values for calculations (replace with 0)
    y_masked = np.where(valid, y, 0.0)
    x_masked = np.where(valid, x, 0.0)

    # mean/average calculation
    y_mean = y_masked.sum(axis=0) / n_valid
    x_mean = x_masked.sum(axis=0) / n_valid

    # centered values for regression. how much is the difference relative to the mean, to calculate beta.
    y_centered = np.where(valid, y - y_mean, 0.0)
    x_centered = np.where(valid, x - x_mean, 0.0)

    # beta = cov(x, y) / var(x). how much does x change when y changes.
    numerator = (x_centered * y_centered).sum(axis=0)
    denominator = (x_centered * x_centered).sum(axis=0)
    beta = numerator / denominator

    # alpha = how off is the return from the benchmark in absolute terms. y is factor returns, x is benchmark returns.
    alpha = y_mean - beta * x_mean

    # annualized alpha
    ann_alpha = 100 * ((1 + alpha) ** periods_per_year - 1)

    # t-statistic
    y_var = (y_centered ** 2).sum(axis=0) / (n_valid - 1)
    y_std = np.sqrt(y_var)
    y_stderr = y_std / np.sqrt(n_valid)
    t_stat = y_mean / y_stderr

    # filter factors with insufficient data
    valid_factors = n_valid >= 2
    valid_factor_names = np.array(factor_names)[valid_factors]

    result = pd.DataFrame({
        "column": valid_factor_names,
        "T-Stat": t_stat[valid_factors],
        "beta": beta[valid_factors],
        "alpha": alpha[valid_factors],
        "annualized alpha %": ann_alpha[valid_factors],
    })

    if factor_stats is not None:
        result["NA %"] = result["column"].map(
            {k: v.get("na_pct", 0.0) for k, v in factor_stats.items()}
        ).fillna(0.0)
        result["IC"] = result["column"].map(
            {k: v.get("ic", np.nan) for k, v in factor_stats.items()}
        ).fillna(np.nan)
        result["IC t-stat"] = result["column"].map(
            {k: v.get("ic_tstat", np.nan) for k, v in factor_stats.items()}
        ).fillna(np.nan)
    else:
        result["NA %"] = 0.0
        result["IC"] = np.nan
        result["IC t-stat"] = np.nan

    return result


def calculate_correlation_matrix(results_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate correlation matrix between factors.

    Args:
        results_df: DataFrame with factor returns (Date, factor, ret)

    Returns:
        Correlation matrix DataFrame
    """
    pivot_df = results_df.pivot(index="Date", columns="factor", values="ret")
    corr_matrix = pivot_df.corr()

    return corr_matrix


def select_best_features(
    metrics_df: pd.DataFrame,
    correlation_matrix: pd.DataFrame,
    N: int = DEFAULT_N_FACTORS,
    correlation_threshold: float = DEFAULT_CORRELATION_THRESHOLD,
    a_min: float = DEFAULT_MIN_ALPHA,
    max_na_pct: float = DEFAULT_MAX_NA_PCT,
    min_ic: float = DEFAULT_MIN_IC,
    rank_by: str = "Alpha",
) -> tuple[list, dict[str, str]]:
    """
    Select N best features based on alpha or IC and low correlation.
    Also classifies all factors into categories.

    Args:
        metrics_df: DataFrame with feature metrics
        correlation_matrix: Correlation matrix of features
        N: Number of features to select
        correlation_threshold: Maximum allowed correlation
        a_min: Minimum absolute annualized alpha %
        max_na_pct: Maximum allowed NA percentage
        min_ic: Minimum absolute IC threshold
        rank_by: Metric to rank by ("Alpha" or "IC")

    Returns:
        Tuple of (selected feature names, classifications dict)
        Classifications: "best", "below_alpha", "correlation_conflict", "n_limit", "high_na", or "below_ic"
    """
    classifications = {}
    selected_features = []
    selected_indices = []

    corr_arr = correlation_matrix.values
    col_to_idx = {c: i for i, c in enumerate(correlation_matrix.columns)}

    sort_col = "IC" if rank_by == "IC" else "annualized alpha %"
    sorted_metrics = metrics_df.sort_values(by=sort_col, key=abs, ascending=False)

    has_na_col = "NA %" in sorted_metrics.columns
    has_ic_col = "IC" in sorted_metrics.columns

    columns = sorted_metrics["column"].to_numpy()
    alphas = sorted_metrics["annualized alpha %"].to_numpy()
    na_pcts = sorted_metrics["NA %"].to_numpy() if has_na_col else np.zeros(len(sorted_metrics))
    ics = sorted_metrics["IC"].to_numpy() if has_ic_col else None

    feat_indices = np.array([col_to_idx.get(c, -1) for c in columns])

    valid_na = na_pcts <= max_na_pct
    valid_alpha = np.abs(alphas) >= a_min
    valid_ic = np.abs(ics) >= min_ic if ics is not None else None

    for i in range(len(columns)):
        feature = columns[i]

        if not valid_na[i]:
            classifications[feature] = "high_na"
            continue

        if rank_by == "IC":
            if valid_ic is not None and not valid_ic[i]:
                classifications[feature] = "below_ic"
                continue
        else:
            if not valid_alpha[i]:
                classifications[feature] = "below_alpha"
                continue

        if len(selected_features) >= N:
            classifications[feature] = "n_limit"
            continue

        feat_idx = feat_indices[i]
        if feat_idx < 0 or (
            len(selected_indices) > 0
            and np.any(np.abs(corr_arr[feat_idx, selected_indices]) >= correlation_threshold)
        ):
            classifications[feature] = "correlation_conflict"
            continue

        selected_features.append(feature)
        selected_indices.append(feat_idx)
        classifications[feature] = "best"

    return selected_features, classifications
