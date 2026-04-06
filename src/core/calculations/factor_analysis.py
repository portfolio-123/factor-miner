from collections.abc import Callable
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
import logging
from os import cpu_count
import numpy as np
import polars as pl
from scipy.stats import ttest_1samp
import traceback
from typing import Any

from src.core.calculations.utils import annualize_return, calculate_factor_metric, calculate_na_pct, weighted_ic
from src.core.types.models import AnalysisParams, ProcessFactorResult, WorkerContext
from src.services.dataset_service import DatasetService
from src.core.config.constants import INTERNAL_FUTURE_PERF_COL

logger = logging.getLogger("calculations")


worker_ctx: WorkerContext


def _init_worker(ctx: WorkerContext):
    global worker_ctx
    worker_ctx = ctx


def _calc_workers(n_factors: int, n_rows: int) -> int:
    # overhead of creating the processes is high. this formula adjusts the amount of workers launched based on row count and amount of factors
    if n_factors * n_rows < 1_000_000:
        return 0

    max_cpus = min(16, cpu_count() or 4)

    # at least 2 factors per worker
    by_factors = n_factors // 2
    # add a worker per 25k rows. for reference, 1 year of weekly data with sp500 as universe, is 26k rows
    by_rows = max(1, n_rows // 25000)

    workers = min(by_factors, by_rows, max_cpus)
    return max(1, workers)


def _process_factor_per_date(factor_valid: np.ndarray, perf_valid: np.ndarray, high_quantile: float, low_quantile: float):
    if high_quantile <= 0:
        raise ValueError("High quantile must be greater than 0")

    ic = weighted_ic(factor_valid, perf_valid)

    total_stocks = len(factor_valid)

    high_quantile_amount = int(total_stocks * (high_quantile / 100))
    low_quantile_amount = int(total_stocks * (low_quantile / 100))

    if high_quantile_amount == 0:
        raise ValueError("No stocks were found in the high quantile")

    if low_quantile > 0:
        if low_quantile_amount == 0:
            raise ValueError("No stocks were found in the low quantile")

        sort_by_factor = np.argpartition(factor_valid, [high_quantile_amount, -low_quantile_amount])
        low_quantile_ret = float(np.take(perf_valid, sort_by_factor[-low_quantile_amount:]).mean())
    else:
        sort_by_factor = np.argpartition(factor_valid, high_quantile_amount)
        low_quantile_ret = 0.0

    high_quantile_ret = float(np.take(perf_valid, sort_by_factor[:high_quantile_amount]).mean())

    return ic, high_quantile_ret, low_quantile_ret


def _process_factor(factor_arr: np.ndarray, ascending: bool) -> tuple[ProcessFactorResult, np.ndarray]:
    ctx = worker_ctx
    if not ascending:
        np.negative(factor_arr, out=factor_arr)

    valid_mask = ctx.perf_mask & np.isfinite(factor_arr)

    factor_stats_per_date = np.empty((len(ctx.unique_dates), 3), dtype=np.float32)
    for i, date_group in enumerate(ctx.date_indices):
        valid_rows_from_group = date_group[valid_mask[date_group]]
        if valid_rows_from_group.size == 0:
            factor_stats_per_date[i] = np.nan
            continue
        factor_stats_per_date[i] = _process_factor_per_date(
            factor_arr[valid_rows_from_group],
            ctx.perf_arr[valid_rows_from_group],
            high_quantile=ctx.params.high_quantile,
            low_quantile=ctx.params.low_quantile,
        )

    valid = np.isfinite(ctx.benchmark_returns)
    valid &= np.all(np.isfinite(factor_stats_per_date[:, 1:3]), axis=1)

    ic_per_date = factor_stats_per_date[:, 0]
    high_quantile_rets = np.compress(valid, factor_stats_per_date[:, 1])
    low_quantile_rets = np.compress(valid, factor_stats_per_date[:, 2])
    benchmark_returns_valid = np.compress(valid, ctx.benchmark_returns)

    factor_metrics = calculate_factor_metric(high_quantile_rets, benchmark_returns_valid, ctx.periods_per_year)

    ic_valid = np.compress(np.isfinite(ic_per_date), ic_per_date)
    ic_t_stat: Any = ttest_1samp(ic_valid, popmean=0)[0]

    result: ProcessFactorResult = {
        "na_pct": round(calculate_na_pct(factor_arr), 2),
        "ic": float(np.nanmean(ic_per_date)),
        "ic_t_stat": float(ic_t_stat),
        "annualized_high_quantile_pct": annualize_return(high_quantile_rets, worker_ctx.periods_per_year) * 100,
        "annualized_low_quantile_pct": (
            annualize_return(low_quantile_rets, worker_ctx.periods_per_year) * 100 if worker_ctx.params.low_quantile > 0 else 0
        ),
        "asc": ascending,
        "returns": high_quantile_rets - low_quantile_rets,
        **factor_metrics,
    }

    return result, factor_stats_per_date


def analyze_factors(
    df: pl.DataFrame,
    benchmark_returns: np.ndarray,
    dataset_svc: DatasetService,
    factor_columns: list[str],
    params: AnalysisParams,
    periods_per_year: float,
    on_progress: Callable[[int, int], None],
) -> dict[str, ProcessFactorResult]:
    perf_arr = df[INTERNAL_FUTURE_PERF_COL].to_numpy()
    perf_mask = np.isfinite(perf_arr) & (perf_arr <= (params.max_return_pct / 100))

    unique_dates, date_index_by_row = np.unique(df["Date"].to_numpy(), return_inverse=True)
    date_indices = [np.where(date_index_by_row == i)[0] for i in range(len(unique_dates))]

    ctx = WorkerContext(
        perf_arr=perf_arr,
        perf_mask=perf_mask,
        date_indices=date_indices,
        benchmark_returns=benchmark_returns,
        unique_dates=unique_dates,
        params=params,
        periods_per_year=periods_per_year,
    )

    n_workers = _calc_workers(len(factor_columns), len(df))
    logger.info("Processes launched with multi-processing: ")
    logger.info(n_workers)
    if n_workers <= 1:
        return _analyze_single_process(ctx, dataset_svc, factor_columns, params, on_progress)

    return _analyze_multiprocess(ctx, dataset_svc, factor_columns, params, on_progress, n_workers)


def _analyze_single_process(
    ctx: WorkerContext,
    dataset_svc: DatasetService,
    factor_columns: list[str],
    params: AnalysisParams,
    on_progress: Callable[[int, int], None],
) -> dict[str, ProcessFactorResult]:
    global worker_ctx
    worker_ctx = ctx

    factor_stats_dict: dict[str, ProcessFactorResult] = {}
    logged_first = False

    for i, name in enumerate(factor_columns):
        try:
            arr = dataset_svc.read_column_pa(name).to_numpy().astype(np.float32)
            result, factor_stats_per_date = _process_factor(arr, name in params.asc_factors)
            factor_stats_dict[name] = result

            if not logged_first:
                _log_first_factor(name, result["annualized_alpha_pct"], factor_stats_per_date, arr, ctx)
                logged_first = True
        except Exception:
            logger.error(f"ANALYSIS FAILED - Factor {name}\n{traceback.format_exc()}")

        on_progress(i + 1, len(factor_columns))

    return factor_stats_dict


def _analyze_multiprocess(
    ctx: WorkerContext,
    dataset_svc: DatasetService,
    factor_columns: list[str],
    params: AnalysisParams,
    on_progress: Callable[[int, int], None],
    n_workers: int,
) -> dict[str, ProcessFactorResult]:
    with ProcessPoolExecutor(
        max_workers=n_workers,
        initializer=_init_worker,
        initargs=(ctx,),
    ) as executor:
        pending_factors = {}
        factors = iter(factor_columns)
        completed_count = 0
        logged_first = False
        factor_stats_dict: dict[str, ProcessFactorResult] = {}

        def queue_factor():
            try:
                name = next(factors)
                arr = dataset_svc.read_column_pa(name).to_numpy().astype(np.float32)
                future = executor.submit(_process_factor, arr, name in params.asc_factors)
                keep_arr = arr if not logged_first else None
                pending_factors[future] = (name, keep_arr)
                return True
            except StopIteration:
                return False

        for _ in range(min(n_workers * 3, len(factor_columns))):
            if not queue_factor():
                break

        while pending_factors:
            done, _ = wait(pending_factors.keys(), return_when=FIRST_COMPLETED)

            for future in done:
                factor, original_arr = pending_factors.pop(future)
                try:
                    result, factor_stats_per_date = future.result()
                    factor_stats_dict[factor] = result

                    if not logged_first:
                        _log_first_factor(factor, result["annualized_alpha_pct"], factor_stats_per_date, original_arr, ctx)
                        logged_first = True
                except Exception:
                    logger.error(f"ANALYSIS FAILED - Factor {factor}\n{traceback.format_exc()}")

                queue_factor()
                completed_count += 1
                on_progress(completed_count, len(factor_columns))

        return factor_stats_dict


def _log_first_factor(factor: str, annualized_alpha_pct: float, stats: np.ndarray, first_factor_data: np.ndarray, ctx: WorkerContext):
    total_alpha_pct = 100 * ((1 + annualized_alpha_pct / 100) ** (len(ctx.unique_dates) / ctx.periods_per_year) - 1)

    lines = []
    for i, date in enumerate(ctx.unique_dates):
        idx = ctx.date_indices[i]

        lines.append(
            f"  {date} | "
            f"High Q: {stats[i, 1]*100:6.2f}% | "
            f"Low Q: {stats[i, 2]*100:6.2f}% | "
            f"Factor Mean: {np.nanmean(first_factor_data[idx]):8.2f} | "
            f"Perf Mean: {np.nanmean(ctx.perf_arr[idx]) * 100:6.2f}%"
        )

    detailed_report = "\n".join(lines)
    logger.info(
        f"\n{'='*60}\n"
        f"FIRST FACTOR BREAKDOWN: [{factor}]\n"
        f"Total Alpha: {total_alpha_pct:.2f}%\n"
        f"Annualized Alpha: {annualized_alpha_pct:.2f}%\n"
        f"{detailed_report}\n"
    )
