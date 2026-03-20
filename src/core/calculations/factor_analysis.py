from collections.abc import Callable
import logging
from os import cpu_count
import traceback
import numpy as np
import polars as pl
import polars.selectors as cs
from concurrent.futures import ThreadPoolExecutor, as_completed

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
    perf_is_valid: np.ndarray,
    date_index_by_row: np.ndarray,
    unique_dates: np.ndarray,
    n_dates: int,
    top_pct: float,
    bottom_pct: float,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, dict]:
    """
    Process a single factor column to calculate returns and statistics.

    Args:
        col: Factor column name
        factor_arr: Factor values array (already filtered to valid indices)
        perf_arr: Performance values array
        perf_is_valid: Boolean mask for valid performance values
        date_index_by_row: Array mapping each row to its date index
        unique_dates: Array of unique dates
        n_dates: Number of unique dates
        top_pct: Percentage of top stocks to include
        bottom_pct: Percentage of bottom stocks to include

    Returns:
        Tuple of (dates array, factor names array, returns array, factor stats dict)
    """

    total_values = len(factor_arr)
    na_count = np.isnan(factor_arr).sum()
    na_pct = (na_count / total_values * 100) if total_values > 0 else 100.0

    valid_mask = perf_is_valid & ~np.isnan(factor_arr)

    # keep only rows where both factor and future perf exist
    date_index_valid = date_index_by_row[valid_mask]
    factor_valid = factor_arr[valid_mask]
    perf_valid = perf_arr[valid_mask]

    count_per_date = np.bincount(date_index_valid, minlength=n_dates)

    # sort by (date, factor) so rank is computed inside each date bucket
    sort_idx_by_date_factor = np.lexsort((factor_valid, date_index_valid))
    date_index_sorted = date_index_valid[sort_idx_by_date_factor]
    perf_sorted_by_factor = perf_valid[sort_idx_by_date_factor]

    # define date boundaries
    # e.g count_per_date=[3,2] -> date_start_offsets=[0,3,5]
    date_start_offsets = np.zeros(n_dates + 1, dtype=np.int64)
    date_start_offsets[1:] = np.cumsum(count_per_date)
    # rank each row within its date, e.g [0,1,2]
    rank_within_date = (
        np.arange(len(date_index_sorted)) - date_start_offsets[date_index_sorted]
    )
    date_size_per_row = count_per_date[date_index_sorted]
    # convert index rank to percentile-like rank in (0,1).
    # example for size=4 -> [0.125, 0.375, 0.625, 0.875]
    factor_rank_pct = (rank_within_date + 0.5) / date_size_per_row

    # repeat ranking, but on perf to build spearman-style correlation
    sort_idx_by_date_perf = np.lexsort((perf_sorted_by_factor, date_index_sorted))
    perf_rank_position = np.empty_like(sort_idx_by_date_perf)
    perf_rank_position[sort_idx_by_date_perf] = np.arange(len(sort_idx_by_date_perf))
    perf_rank_within_date = perf_rank_position - date_start_offsets[date_index_sorted]
    perf_rank_pct = (perf_rank_within_date + 0.5) / date_size_per_row

    alpha = 4.0
    weights = 1.0 + alpha * np.abs(factor_rank_pct - 0.5)

    # weighted spearman correlation (ic) per date
    weight_sum_per_date = np.bincount(
        date_index_sorted, weights=weights, minlength=n_dates
    )
    weight_sum_per_date = np.where(weight_sum_per_date > 0, weight_sum_per_date, 1.0)

    # weighted means of factor/perf ranks per date
    mean_factor_rank_per_date = (
        np.bincount(
            date_index_sorted, weights=weights * factor_rank_pct, minlength=n_dates
        )
        / weight_sum_per_date
    )
    mean_perf_rank_per_date = (
        np.bincount(
            date_index_sorted, weights=weights * perf_rank_pct, minlength=n_dates
        )
        / weight_sum_per_date
    )

    # center ranks by their date mean, then compute weighted cov/var
    factor_rank_centered = (
        factor_rank_pct - mean_factor_rank_per_date[date_index_sorted]
    )
    perf_rank_centered = perf_rank_pct - mean_perf_rank_per_date[date_index_sorted]
    cov_factor_perf_rank = (
        np.bincount(
            date_index_sorted,
            weights=weights * factor_rank_centered * perf_rank_centered,
            minlength=n_dates,
        )
        / weight_sum_per_date
    )
    var_factor_rank = (
        np.bincount(
            date_index_sorted,
            weights=weights * factor_rank_centered**2,
            minlength=n_dates,
        )
        / weight_sum_per_date
    )
    var_perf_rank = (
        np.bincount(
            date_index_sorted,
            weights=weights * perf_rank_centered**2,
            minlength=n_dates,
        )
        / weight_sum_per_date
    )

    # ic = cov / (std_f * std_r), require at least 4 names per date
    ic_denominator = np.sqrt(var_factor_rank * var_perf_rank)
    valid_ic_denominator = (ic_denominator > 0) & (count_per_date >= 4)
    ic_per_date = np.full(n_dates, np.nan)
    np.divide(
        cov_factor_perf_rank,
        ic_denominator,
        out=ic_per_date,
        where=valid_ic_denominator,
    )

    valid_ic_mask = (
        (count_per_date >= 4) & ~np.isnan(ic_per_date) & (np.abs(ic_per_date) < 1.0)
    )
    valid_ic_values = ic_per_date[valid_ic_mask]
    n_ic = len(valid_ic_values)
    mean_ic = np.mean(valid_ic_values) if n_ic > 0 else np.nan

    # t-stat for ic series: mean / (std / sqrt(n))
    if n_ic > 1:
        std_ic = np.std(valid_ic_values, ddof=1)
        ic_tstat = mean_ic / (std_ic / np.sqrt(n_ic)) if std_ic > 0 else np.nan
    else:
        ic_tstat = np.nan

    # translate percentage to count per date.
    # e.g. count_per_date=23 and top_pct=10 -> round(2.3)=2 names in top bucket
    top_count_per_date = np.round(count_per_date * (top_pct / 100.0)).astype(np.int64)
    bottom_count_per_date = np.round(count_per_date * (bottom_pct / 100.0)).astype(
        np.int64
    )
    top_count_per_row = top_count_per_date[date_index_sorted]
    bottom_count_per_row = bottom_count_per_date[date_index_sorted]

    # mark rows in top/bottom slices inside each date
    is_bottom_bucket = rank_within_date < bottom_count_per_row
    is_top_bucket = rank_within_date >= (date_size_per_row - top_count_per_row)
    top_return_sum_per_date = np.bincount(
        date_index_sorted,
        weights=np.where(is_top_bucket, perf_sorted_by_factor, 0.0),
        minlength=n_dates,
    )
    bottom_return_sum_per_date = np.bincount(
        date_index_sorted,
        weights=np.where(is_bottom_bucket, perf_sorted_by_factor, 0.0),
        minlength=n_dates,
    )

    # per-date long-short return: (sum(top) - sum(bottom)) / (n_top + n_bottom)
    selected_count_per_date = top_count_per_date + bottom_count_per_date
    has_selected_names_mask = selected_count_per_date > 0
    long_short_ret_per_date = np.full(n_dates, np.nan)
    long_short_ret_per_date[has_selected_names_mask] = (
        top_return_sum_per_date[has_selected_names_mask]
        - bottom_return_sum_per_date[has_selected_names_mask]
    ) / selected_count_per_date[has_selected_names_mask]

    # average leg returns per date: long=top mean, short=bottom mean
    long_ret_per_date = np.full(n_dates, np.nan)
    short_ret_per_date = np.full(n_dates, np.nan)
    has_top_mask = top_count_per_date > 0
    has_bottom_mask = bottom_count_per_date > 0
    long_ret_per_date[has_top_mask] = (
        top_return_sum_per_date[has_top_mask] / top_count_per_date[has_top_mask]
    )
    short_ret_per_date[has_bottom_mask] = (
        bottom_return_sum_per_date[has_bottom_mask]
        / bottom_count_per_date[has_bottom_mask]
    )

    valid_long_mask = np.isfinite(long_ret_per_date)
    valid_short_mask = np.isfinite(short_ret_per_date)
    n_long_periods = int(valid_long_mask.sum())
    n_short_periods = int(valid_short_mask.sum())

    if n_long_periods > 0:
        log_cum_long = np.sum(np.log1p(long_ret_per_date[valid_long_mask]))
        cumulative_long_ret = np.exp(log_cum_long) - 1
    else:
        cumulative_long_ret = np.nan

    if n_short_periods > 0:
        log_cum_short = np.sum(np.log1p(short_ret_per_date[valid_short_mask]))
        cumulative_short_ret = np.exp(log_cum_short) - 1
    else:
        cumulative_short_ret = np.nan

    factor_stats = {
        "column": col,
        "NA %": round(na_pct, 2),
        "IC": mean_ic,
        "IC t-stat": ic_tstat,
        "cumulative_long_ret": cumulative_long_ret,
        "cumulative_short_ret": cumulative_short_ret,
        "n_long_periods": n_long_periods,
        "n_short_periods": n_short_periods,
    }

    # build results as arrays instead of dicts
    valid_return_mask = has_selected_names_mask & ~np.isnan(long_short_ret_per_date)
    valid_date_indices = np.where(valid_return_mask)[0]

    return (
        unique_dates[valid_date_indices],
        np.full(len(valid_date_indices), col, dtype=object),
        long_short_ret_per_date[valid_date_indices],
        factor_stats,
    )


def analyze_factors(
    future_perf_df: pl.DataFrame,
    dataset_svc: DatasetService,
    *,
    core_df: pl.DataFrame,
    factor_columns: list[str],
    top_pct=10.0,
    bottom_pct=10.0,
    batch_size=10,
    on_progress: Callable[[int, int], None] | None = None,
) -> tuple[pl.DataFrame, dict[str, dict]]:
    """
    Analyze factors by calculating top X% vs bottom X% performance difference.
    Reads factors in batches from Parquet.

    Args:
        future_perf_df: Pre-calculated future performance (Date, Ticker, internal future perf column)
        dataset_svc: DatasetService for reading factor data
        core_df: Pre-read DataFrame containing Date and Ticker columns
        factor_columns: List of factor columns to analyze (auto-detected if None)
        top_pct: Percentage of top stocks to include (default: 10.0)
        bottom_pct: Percentage of bottom stocks to include (default: 10.0)
        batch_size: Number of factors to read per batch (default: 50)

    Returns:
        Tuple of (DataFrame with factor analysis results, dict of factor stats per factor including NA%, IC, IC t-stat)
    """
    total_factors = len(factor_columns)

    all_dates: list[np.ndarray] = []
    all_factors: list[np.ndarray] = []
    all_rets: list[np.ndarray] = []
    factor_stats_dict: dict[str, dict] = {}

    base_df = core_df.select(["Date", "Ticker"]).with_row_index("_row_idx")

    # add future performance column to base dataframe
    merged_base = base_df.join(future_perf_df, on=["Date", "Ticker"], how="inner")
    valid_indices = merged_base["_row_idx"].to_numpy()
    perf_arr = merged_base[INTERNAL_FUTURE_PERF_COL].to_numpy()

    # get date index per row
    unique_dates, date_index_by_row = np.unique(
        merged_base["Date"].to_numpy(), return_inverse=True
    )
    perf_is_valid = ~np.isnan(perf_arr)

    n_dates = len(unique_dates)
    del base_df, merged_base

    completed_count = 0
    max_workers = min(8, cpu_count() or 4)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for batch_start in range(0, total_factors, batch_size):
            batch_cols = factor_columns[batch_start : batch_start + batch_size]
            batch_df = dataset_svc.read_columns(batch_cols)

            future_to_col = {
                executor.submit(
                    _process_factor,
                    col,
                    batch_df[col].to_numpy()[valid_indices],
                    perf_arr,
                    perf_is_valid,
                    date_index_by_row,
                    unique_dates,
                    n_dates,
                    top_pct,
                    bottom_pct,
                ): col
                for col in batch_cols
            }

            # as_completed() yields futures as they finish.
            for future in as_completed(future_to_col):
                col = future_to_col[future]
                try:
                    dates_arr, factors_arr, rets_arr, factor_stats = future.result()
                except Exception as e:
                    logger.error(
                        f"THREAD CRASHED - Factor {col}: {type(e).__name__}: {e}"
                    )
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

    # concatenate all arrays and build DataFrame once
    if all_dates:
        return (
            pl.DataFrame(
                {
                    "Date": np.concatenate(all_dates),
                    "factor": np.concatenate(all_factors),
                    "ret": np.concatenate(all_rets).astype(np.float32),
                }
            ),
            factor_stats_dict,
        )
    else:
        return (
            pl.DataFrame(
                schema={"Date": pl.Utf8, "factor": pl.Utf8, "ret": pl.Float32}
            ),
            factor_stats_dict,
        )


def calculate_factor_metrics(
    results_df: pl.DataFrame,
    raw_data: pl.DataFrame,
    factor_stats: dict[str, dict],
    periods_per_year=52.0,
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
    # one benchmark return per date
    benchmark = raw_data.select(["Date", INTERNAL_BENCHMARK_COL]).unique()
    results_df = results_df.with_columns(pl.col("Date").str.to_date("%Y-%m-%d"))

    # align factor returns with benchmark by date
    merged_data = results_df.join(benchmark, on="Date", how="inner")

    # drop rows where either side is not finite
    merged_data = merged_data.filter(
        pl.col("ret").is_finite() & pl.col(INTERNAL_BENCHMARK_COL).is_finite()
    )

    # y matrix shape is (n_dates, n_factors)
    pivot = merged_data.pivot(
        index="Date", on="factor", values="ret", aggregate_function="first"
    )
    factor_names = [c for c in pivot.columns if c != "Date"]
    y = pivot.select(factor_names).to_numpy()  # (n_dates, n_factors)

    # x is benchmark as a single column aligned to pivot dates: shape (n_dates, 1)
    pivot_with_bench = pivot.join(benchmark, on="Date", how="left")
    x = pivot_with_bench[INTERNAL_BENCHMARK_COL].to_numpy().reshape(-1, 1)

    valid = ~np.isnan(y)
    n_valid = valid.sum(axis=0)  # count per factor

    y_masked = np.where(valid, y, 0.0)
    x_masked = np.where(valid, x, 0.0)

    # mean/average calculation
    y_mean = y_masked.sum(axis=0) / n_valid
    x_mean = x_masked.sum(axis=0) / n_valid

    # center by mean before covariance/variance math
    y_centered = np.where(valid, y - y_mean, 0.0)
    x_centered = np.where(valid, x - x_mean, 0.0)

    # beta = cov(x, y) / var(x), computed per factor column
    numerator = (x_centered * y_centered).sum(axis=0)
    denominator = (x_centered * x_centered).sum(axis=0)
    beta = numerator / denominator

    # t-stat per factor: mean(ret) / standard_error(ret)
    y_var = (y_centered**2).sum(axis=0) / (n_valid - 1)
    y_std = np.sqrt(y_var)
    y_stderr = y_std / np.sqrt(n_valid)
    t_stat = y_mean / y_stderr

    # keep factors with at least 2 periods
    valid_factors = n_valid >= 2
    valid_factor_names = np.array(factor_names)[valid_factors].tolist()

    stats_df = pl.DataFrame(factor_stats.values())

    result = pl.DataFrame(
        {
            "column": valid_factor_names,
            "T-Stat": t_stat[valid_factors],
            "beta": beta[valid_factors],
        }
    )

    # add ic, na %, and cumulative fields
    result = result.join(stats_df, on="column", how="left")

    # annualize cumulative return.
    # e.g. cum=0.10 over 10 weeks, periods_per_year=52 -> (1.10)^(5.2)-1
    result = result.with_columns(
        [
            (
                100
                * (
                    (1 + pl.col("cumulative_long_ret"))
                    ** (periods_per_year / pl.col("n_long_periods"))
                    - 1
                )
            ).alias("annualized long %"),
            (
                100
                * (
                    (1 + pl.col("cumulative_short_ret"))
                    ** (periods_per_year / pl.col("n_short_periods"))
                    - 1
                )
            ).alias("annualized short %"),
        ]
    )

    factor_mean_per_period = y_mean[valid_factors]
    bench_mean_per_period = x_mean[valid_factors].flatten()

    # alpha calculation
    alpha_per_period = (
        factor_mean_per_period - result["beta"].to_numpy() * bench_mean_per_period
    )
    annualized_alpha = 100 * ((1 + alpha_per_period) ** periods_per_year - 1)

    result = result.with_columns(pl.Series("annualized alpha %", annualized_alpha))

    # final display order
    result = result.select(
        [
            "column",
            "T-Stat",
            "beta",
            "NA %",
            "IC",
            "IC t-stat",
            "annualized long %",
            "annualized short %",
            "annualized alpha %",
        ]
    )

    result = result.cast({cs.float64(): pl.Float32})

    return result
