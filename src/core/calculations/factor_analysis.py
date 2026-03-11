import logging
import os
import traceback
import numpy as np
import polars as pl
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Dict, List, Tuple

from src.services.dataset_service import DatasetService
from src.core.config.constants import (
    INTERNAL_FUTURE_PERF_COL,
    INTERNAL_BENCHMARK_COL,
)

logger = logging.getLogger("calculations")


def _process_factor(
    col: str,
    factor_arr: np.ndarray,
    perf_arr: np.ndarray,
    perf_valid: np.ndarray,
    date_inverse: np.ndarray,
    unique_dates: np.ndarray,
    n_dates: int,
    top_pct: float,
    bottom_pct: float,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, dict]:
    """
    Process a single factor column to calculate returns and statistics.

    Args:
        col: Factor column name
        factor_arr: Factor values array (already filtered to valid indices)
        perf_arr: Performance values array
        perf_valid: Boolean mask for valid performance values
        date_inverse: Array mapping each row to its date index
        unique_dates: Array of unique dates
        n_dates: Number of unique dates
        top_pct: Percentage of top stocks to include
        bottom_pct: Percentage of bottom stocks to include

    Returns:
        Tuple of (dates array, factor names array, returns array, factor stats dict)
    """
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
    group_sizes_f = group_sizes.astype(np.float32)
    # calculate percentile rank of each row within its period
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
    # to avoid division by zero
    valid_denom = (denom > 0) & (counts >= 4)
    ic_per_date = np.full(n_dates, np.nan)
    np.divide(cov_fr, denom, out=ic_per_date, where=valid_denom)

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
    # Use round() instead of truncation so 2.9 -> 3, 2.4 -> 2
    top_n = np.round(counts * (top_pct / 100.0)).astype(np.int64)
    bottom_n = np.round(counts * (bottom_pct / 100.0)).astype(np.int64)

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

    # Calculate average long/short returns per date
    long_ret = np.full(n_dates, np.nan)
    short_ret = np.full(n_dates, np.nan)
    valid_top_mask = top_n > 0
    valid_bottom_mask = bottom_n > 0
    long_ret[valid_top_mask] = top_sums[valid_top_mask] / top_n[valid_top_mask]
    short_ret[valid_bottom_mask] = bottom_sums[valid_bottom_mask] / bottom_n[valid_bottom_mask]

    valid_long = ~np.isnan(long_ret) & np.isfinite(long_ret)
    valid_short = ~np.isnan(short_ret) & np.isfinite(short_ret)
    n_long_periods = int(valid_long.sum())
    n_short_periods = int(valid_short.sum())

    if valid_long.any():
        log_cum_long = np.sum(np.log1p(long_ret[valid_long]))
        cumulative_long_ret = np.exp(log_cum_long) - 1
    else:
        cumulative_long_ret = np.nan

    if valid_short.any():
        log_cum_short = np.sum(np.log1p(short_ret[valid_short]))
        cumulative_short_ret = np.exp(log_cum_short) - 1
    else:
        cumulative_short_ret = np.nan

    # Add to factor_stats
    factor_stats['cumulative_long_ret'] = cumulative_long_ret
    factor_stats['cumulative_short_ret'] = cumulative_short_ret
    factor_stats['n_long_periods'] = n_long_periods
    factor_stats['n_short_periods'] = n_short_periods

    # build results as arrays instead of dicts
    valid_results = valid_date_mask & ~np.isnan(ret)
    valid_indices_arr = np.where(valid_results)[0]

    return (
        unique_dates[valid_indices_arr],
        np.full(len(valid_indices_arr), col, dtype=object),
        ret[valid_indices_arr],
        factor_stats,
    )


def analyze_factors(
    future_perf_df: pl.DataFrame,
    dataset_svc: DatasetService,
    *,
    factor_columns: List[str],
    top_pct: float = 10.0,
    bottom_pct: float = 10.0,
    batch_size: int = 50,
    on_progress: Callable[[int, int], None] | None = None,
) -> Tuple[pl.DataFrame, Dict[str, dict]]:
    """
    Analyze factors by calculating top X% vs bottom X% performance difference.
    Reads factors in batches from Parquet.

    Args:
        future_perf_df: Pre-calculated future performance (Date, Ticker, internal future perf column)
        dataset_svc: DatasetService for reading factor data
        factor_columns: List of factor columns to analyze (auto-detected if None)
        top_pct: Percentage of top stocks to include (default: 10.0)
        bottom_pct: Percentage of bottom stocks to include (default: 10.0)
        batch_size: Number of factors to read per batch (default: 50)

    Returns:
        Tuple of (DataFrame with factor analysis results, dict of factor stats per factor including NA%, IC, IC t-stat)
    """
    total_factors = len(factor_columns)

    all_dates: List[np.ndarray] = []
    all_factors: List[np.ndarray] = []
    all_rets: List[np.ndarray] = []
    factor_stats_dict: Dict[str, dict] = {}

    # read date/ticker once and track row indices
    base_df = dataset_svc.read_columns(["Date", "Ticker"])
    base_df = base_df.with_row_index("_row_idx")

    # add future performance column to base dataframe
    merged_base = base_df.join(future_perf_df, on=["Date", "Ticker"], how="inner")
    valid_indices = merged_base["_row_idx"].to_numpy()
    perf_arr = merged_base[INTERNAL_FUTURE_PERF_COL].to_numpy()

    # the inverse checks the row and maps it to an index
    unique_dates, date_inverse = np.unique(merged_base["Date"].to_numpy(), return_inverse=True)
    perf_valid = ~np.isnan(perf_arr)

    n_dates = len(unique_dates)
    del base_df, merged_base

    completed_count = 0
    max_workers = min(8, os.cpu_count() or 4)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Process factors in batches
        for batch_start in range(0, total_factors, batch_size):
            batch_cols = factor_columns[batch_start : batch_start + batch_size]
            batch_df = dataset_svc.read_columns(batch_cols)

            # Submit tasks with pre-extracted factor values
            future_to_col = {}
            for col in batch_cols:
                factor_arr = batch_df[col].to_numpy()[valid_indices]
                future = executor.submit(
                    _process_factor,
                    col,
                    factor_arr,
                    perf_arr,
                    perf_valid,
                    date_inverse,
                    unique_dates,
                    n_dates,
                    top_pct,
                    bottom_pct,
                )
                future_to_col[future] = col

            # as_completed() yields futures as they finish.
            for future in as_completed(future_to_col):
                col = future_to_col[future]
                try:
                    dates_arr, factors_arr, rets_arr, factor_stats = future.result()
                except Exception as e:
                    logger.error(f"THREAD CRASHED - Factor {col}: {type(e).__name__}: {e}")
                    logger.error(traceback.format_exc())
                    completed_count += 1
                    if on_progress:
                        on_progress(completed_count, total_factors)
                    continue
                factor_stats_dict[col] = factor_stats
                if len(dates_arr) > 0:
                    all_dates.append(dates_arr)
                    all_factors.append(factors_arr)
                    all_rets.append(rets_arr)
                completed_count += 1
                if on_progress:
                    on_progress(completed_count, total_factors)

            del batch_df

    # concatenate all arrays and build DataFrame once
    if all_dates:
        return (
            pl.DataFrame({
                "Date": np.concatenate(all_dates),
                "factor": np.concatenate(all_factors),
                "ret": np.concatenate(all_rets),
            }),
            factor_stats_dict,
        )
    else:
        return pl.DataFrame(schema={"Date": pl.Utf8, "factor": pl.Utf8, "ret": pl.Float64}), factor_stats_dict


def calculate_factor_metrics(
    results_df: pl.DataFrame,
    raw_data: pl.DataFrame,
    factor_stats: Dict[str, dict],
    periods_per_year: float = 52.0,
) -> pl.DataFrame:
    """
    Calculate statistical metrics for each factor.

    Args:
        results_df: DataFrame with factor returns (Date, factor, ret)
        raw_data: DataFrame with benchmark returns
        factor_stats: Dict mapping factor names to their statistics (NA%, IC, IC t-stat, cumulative returns)
        periods_per_year: Number of periods per year for annualization

    Returns:
        DataFrame with factor metrics (T-Stat, beta, NA %, IC, annualized returns, alpha)
    """
    # get unique benchmark values per date
    benchmark = raw_data.select(["Date", INTERNAL_BENCHMARK_COL]).unique()
    results_df = results_df.with_columns(pl.col("Date").str.to_date("%Y-%m-%d"))

    # merge benchmark with factor returns
    merged_data = results_df.join(benchmark, on="Date", how="inner")

    # filter invalid values upfront
    merged_data = merged_data.filter(
        pl.col("ret").is_finite() & pl.col(INTERNAL_BENCHMARK_COL).is_finite()
    )

    # pivot to get factors as columns: (n_dates, n_factors)
    pivot = merged_data.pivot(index="Date", on="factor", values="ret", aggregate_function="first")
    factor_names = [c for c in pivot.columns if c != "Date"]
    y = pivot.select(factor_names).to_numpy()  # (n_dates, n_factors)

    # get benchmark values aligned with pivot index
    pivot_dates = pivot["Date"].to_list()
    bench_aligned = benchmark.filter(pl.col("Date").is_in(pivot_dates)).sort("Date")
    # Ensure alignment with pivot order
    bench_dict = dict(zip(bench_aligned["Date"].to_list(), bench_aligned[INTERNAL_BENCHMARK_COL].to_list()))
    x = np.array([[bench_dict.get(d, np.nan)] for d in pivot_dates])  # (n_dates, 1)

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

    # t-statistic (based on factor returns)
    y_var = (y_centered ** 2).sum(axis=0) / (n_valid - 1)
    y_std = np.sqrt(y_var)
    y_stderr = y_std / np.sqrt(n_valid)
    t_stat = y_mean / y_stderr

    # filter factors with insufficient data
    valid_factors = n_valid >= 2
    valid_factor_names = np.array(factor_names)[valid_factors].tolist()

    stats_records = []
    for k, v in factor_stats.items():
        stats_records.append({
            "column": k,
            "NA %": v.get("na_pct", 0.0),
            "IC": v.get("ic", np.nan),
            "IC t-stat": v.get("ic_tstat", np.nan),
            "cumulative_long_ret": v.get("cumulative_long_ret", np.nan),
            "cumulative_short_ret": v.get("cumulative_short_ret", np.nan),
            "n_long_periods": v.get("n_long_periods", 0),
            "n_short_periods": v.get("n_short_periods", 0),
        })
    stats_df = pl.DataFrame(stats_records)

    result = pl.DataFrame({
        "column": valid_factor_names,
        "T-Stat": t_stat[valid_factors],
        "beta": beta[valid_factors],
    })

    # join with stats
    result = result.join(stats_df, on="column", how="left")

    # CAGR = (1 + cumulative_return) ^ (1 / years) - 1
    result = result.with_columns([
        (100 * ((1 + pl.col("cumulative_long_ret")) ** (1 / (pl.col("n_long_periods") / periods_per_year)) - 1)).alias("annualized long %"),
        (100 * ((1 + pl.col("cumulative_short_ret")) ** (1 / (pl.col("n_short_periods") / periods_per_year)) - 1)).alias("annualized short %"),
    ])

    factor_mean_per_period = y_mean[valid_factors]
    bench_mean_per_period = x_mean[valid_factors].flatten()

    # alpha calculation
    alpha_per_period = factor_mean_per_period - result["beta"].to_numpy() * bench_mean_per_period
    annualized_alpha = 100 * ((1 + alpha_per_period) ** periods_per_year - 1)

    result = result.with_columns(pl.Series("annualized alpha %", annualized_alpha))

    # Select final columns in desired order
    result = result.select([
        "column", "T-Stat", "beta", "NA %", "IC", "IC t-stat",
        "annualized long %", "annualized short %", "annualized alpha %",
    ])

    return result
