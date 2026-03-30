from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from os import cpu_count
import numpy as np
import polars as pl
from scipy.stats import ttest_1samp
import threading
import traceback
from typing import Any

from src.core.calculations.utils import (
    annualize_return,
    calculate_factor_metric,
    calculate_na_pct,
    weighted_ic,
)
from src.core.types.models import AnalysisParams, DateFactorResult, ProcessFactorResult
from src.services.dataset_service import DatasetService
from src.core.config.constants import INTERNAL_FUTURE_PERF_COL

logger = logging.getLogger("calculations")


def _process_factor_per_date(
    factor_arr: np.ndarray,
    perf_arr: np.ndarray,
    top_pct: float,
    bottom_pct: float,
    max_return_pct: float,
) -> DateFactorResult:
    # filter out stocks where factor or perf are nan
    valid_mask = ~np.isnan(perf_arr) & ~np.isnan(factor_arr)
    # filter out stocks where perf is over max ret %
    valid_mask &= perf_arr <= (max_return_pct / 100)

    factor_valid = factor_arr[valid_mask]
    perf_valid = perf_arr[valid_mask]

    ic = weighted_ic(factor_valid, perf_valid)

    # sort by factor, ascending. first positions are bottom bucket, last are top bucket
    sort_by_factor = np.argsort(factor_valid)
    perf_sorted_by_factor = perf_valid[sort_by_factor]

    # amount of stocks that fit in each bucket
    top_bucket_amount = int(len(factor_valid) * (top_pct / 100))
    bottom_bucket_amount = int(len(factor_valid) * (bottom_pct / 100))

    top_stocks = perf_sorted_by_factor[-top_bucket_amount:]
    bottom_stocks = perf_sorted_by_factor[:bottom_bucket_amount]

    return DateFactorResult(
        ic=ic,
        long_ret=top_stocks.mean(),
        short_ret=bottom_stocks.mean(),
    )


def analyze_factors(
    df: pl.DataFrame,
    benchmark_returns: np.ndarray,
    dataset_svc: DatasetService,
    factor_columns: list[str],
    params: AnalysisParams,
    periods_per_year: int,
    on_progress: Callable[[int, int], None],
) -> dict[str, ProcessFactorResult]:
    df = df.with_row_index("_row_idx")
    valid_indices = df["_row_idx"].to_numpy()

    perf_arr = df[INTERNAL_FUTURE_PERF_COL].to_numpy()

    # get date index per row
    unique_dates, date_index_by_row = np.unique(
        df["Date"].to_numpy(), return_inverse=True
    )

    factor_arr_per_date = [date_index_by_row == i for i in range(len(unique_dates))]

    completed_count = 0
    with ThreadPoolExecutor(max_workers=min(8, cpu_count() or 4)) as executor:
        lock = threading.Lock()

        def do_process_factor(factor) -> ProcessFactorResult:
            with lock:
                factor_arr = (
                    dataset_svc.read_column_pa(factor)
                    .to_numpy()[valid_indices]
                    .astype(np.float32)
                )

            factor_stats_per_date = np.fromiter(
                (
                    _process_factor_per_date(
                        factor_arr[mask],
                        perf_arr[mask],
                        params.top_pct,
                        params.bottom_pct,
                        params.max_return_pct,
                    )
                    for mask in factor_arr_per_date
                ),
                dtype="3f",
            )

            ic_per_date = factor_stats_per_date[:, 0]
            long_rets = factor_stats_per_date[:, 1]
            short_rets = factor_stats_per_date[:, 2]

            longshort_rets = long_rets - short_rets

            factor_metrics = calculate_factor_metric(
                longshort_rets,
                benchmark_returns,
                periods_per_year,
            )

            ic_t_stat: Any = ttest_1samp(ic_per_date, popmean=0)[0]

            return {
                "na_pct": round(calculate_na_pct(factor_arr), 2),
                "ic": float(np.nanmean(ic_per_date)),
                "ic_t_stat": float(ic_t_stat),
                "annualized_long_pct": annualize_return(long_rets, periods_per_year),
                "annualized_short_pct": annualize_return(short_rets, periods_per_year),
                "returns": longshort_rets,
                **factor_metrics,
            }

        future_to_col = {
            executor.submit(do_process_factor, col): col for col in factor_columns
        }

        factor_stats_dict: dict[str, ProcessFactorResult] = {}

        # store factor stats individually as threads finish
        for future in as_completed(future_to_col):
            factor = future_to_col[future]
            try:
                factor_stats_dict[factor] = future.result()
            except Exception as e:
                logger.error(
                    f"ANALYSIS FAILED - Factor {factor}: {type(e).__name__}: {e}"
                )
                logger.error(traceback.format_exc())
            completed_count += 1

            on_progress(completed_count, len(factor_columns))
        return factor_stats_dict
