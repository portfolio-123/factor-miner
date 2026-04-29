from collections.abc import Callable
from concurrent.futures import Future, ProcessPoolExecutor, as_completed
import logging
import math
from os import cpu_count
from typing import ParamSpec, TypeVar
import numpy as np
import polars as pl
from scipy.stats import ttest_1samp
import traceback
from contextlib import ExitStack, contextmanager

from src.core.config.constants import FUTURE_PERF_COLUMN
from src.core.calculations.utils import annualize_return, calculate_factor_metric, calculate_na_pct, weighted_ic
from src.core.types.models import AnalysisParams, DatasetDetails, LocalArray, ProcessFactorResult, SharedArray, WorkerContext
from src.services.dataset_service import DatasetService

T = TypeVar("T")
P = ParamSpec("P")

logger = logging.getLogger("calculations")

worker_ctx: WorkerContext | None = None


def _set_worker(ctx: WorkerContext | None):
    global worker_ctx

    worker_ctx = ctx


def allocate_shared_array(arr: np.ndarray, stack: ExitStack) -> SharedArray:
    shared = SharedArray.alloc(arr)
    stack.callback(SharedArray.dealloc, shared)
    return shared


def run_synchronously(func: Callable[P, T], *args: P.args, **kwargs: P.kwargs):
    future = Future()
    future.set_result(func(*args, **kwargs))
    return future


@contextmanager
def get_executor(
    n_workers: int,
    perf_arr: np.ndarray,
    perf_mask: np.ndarray,
    benchmark_returns: np.ndarray,
    offsets: np.ndarray,
    params: AnalysisParams,
    dataset_details: DatasetDetails,
    periods_per_year: float,
):
    if n_workers > 1:
        with ExitStack() as stack:
            ctx = WorkerContext(
                params=params,
                dataset_details=dataset_details,
                periods_per_year=periods_per_year,
                perf_arr=allocate_shared_array(perf_arr, stack),
                perf_mask=allocate_shared_array(perf_mask, stack),
                benchmark_returns=allocate_shared_array(benchmark_returns, stack),
                offsets=allocate_shared_array(offsets, stack),
            )
            executor = ProcessPoolExecutor(max_workers=n_workers, initializer=_set_worker, initargs=(ctx,))

            stack.push(executor)
            yield executor.submit

        return

    ctx = WorkerContext(
        params=params,
        dataset_details=dataset_details,
        periods_per_year=periods_per_year,
        perf_arr=LocalArray(perf_arr),
        perf_mask=LocalArray(perf_mask),
        benchmark_returns=LocalArray(benchmark_returns),
        offsets=LocalArray(offsets),
    )
    _set_worker(ctx)
    yield run_synchronously
    _set_worker(None)


def _process_factor_per_date(factor_valid: np.ndarray, perf_valid: np.ndarray, high_quantile: float, low_quantile: float, ascending: bool):
    total_stocks = len(factor_valid)

    high_quantile_cut = max(int(total_stocks * (high_quantile / 100)), 1) if high_quantile > 0 else 0
    low_quantile_cut = max(int(total_stocks * (low_quantile / 100)), 1) if low_quantile > 0 else 0

    k = []
    if low_quantile_cut > 0:
        k.append(low_quantile_cut)
    if high_quantile_cut > 0:
        k.append(total_stocks - high_quantile_cut)
    sorted_partitions = np.argpartition(factor_valid, k)

    if ascending:
        high_ret = np.mean(perf_valid[sorted_partitions[:high_quantile_cut]]) if high_quantile_cut > 0 else 0.0
        low_ret = np.mean(perf_valid[sorted_partitions[-low_quantile_cut:]]) if low_quantile_cut > 0 else 0.0
    else:
        high_ret = np.mean(perf_valid[sorted_partitions[-high_quantile_cut:]]) if high_quantile_cut > 0 else 0.0
        low_ret = np.mean(perf_valid[sorted_partitions[:low_quantile_cut]]) if low_quantile_cut > 0 else 0.0

    return high_ret, low_ret


def _process_factor(factor: str, ascending: bool) -> tuple[ProcessFactorResult, np.ndarray]:
    if not worker_ctx:
        raise RuntimeError("Worker state not initialized")

    with DatasetService(worker_ctx.dataset_details) as dataset_svc:
        factor_arr = dataset_svc.read_column_pa(factor).to_numpy().astype(np.float32)

    perf_arr = worker_ctx.perf_arr.array
    perf_mask = worker_ctx.perf_mask.array
    offsets = worker_ctx.offsets.array
    params = worker_ctx.params
    num_dates = len(offsets)

    ic_valid = np.empty(num_dates, dtype=np.float32)
    ic_valid_count = 0

    masked = []

    for i in range(num_dates):
        date_slice = slice(offsets[i, 0], offsets[i, 1])
        f_group = factor_arr[date_slice]
        p_group = perf_arr[date_slice]
        m_group = perf_mask[date_slice]
        mask = m_group & np.isfinite(f_group)

        if np.count_nonzero(mask) < 2:
            continue

        factor_valid = f_group[mask]
        perf_valid = p_group[mask]
        ic_valid[ic_valid_count] = weighted_ic(factor_valid, perf_valid)
        ic_valid_count += 1

        masked.append((i, factor_valid, perf_valid))

    if ic_valid_count > 0:
        ic = float(np.nanmean(ic_valid[:ic_valid_count]))
        if params.auto_detect_direction:
            ascending = ic < 0

        if ascending:
            ic = -ic
            ic_valid[:ic_valid_count] = -ic_valid[:ic_valid_count]
    else:
        ic = math.nan

    quantile_perf_per_date = np.empty((num_dates, 2), dtype=np.float32)
    for i, factor_valid, perf_valid in masked:
        quantile_perf_per_date[i, :] = _process_factor_per_date(
            factor_valid, perf_valid, params.high_quantile, params.low_quantile, ascending
        )

    benchmark_returns = worker_ctx.benchmark_returns.array
    valid = np.isfinite(benchmark_returns) & np.all(np.isfinite(quantile_perf_per_date), axis=1)

    high_quantile_rets = quantile_perf_per_date[valid, 0]
    low_quantile_rets = quantile_perf_per_date[valid, 1]
    benchmark_returns_valid = benchmark_returns[valid]

    combined_returns = high_quantile_rets - low_quantile_rets if params.high_quantile > 0 else low_quantile_rets

    aligned_returns = np.full(num_dates, np.nan, dtype=np.float32)
    aligned_returns[valid] = np.where(combined_returns <= -1.0, np.nan, combined_returns)

    factor_metrics = calculate_factor_metric(aligned_returns[valid], benchmark_returns_valid, worker_ctx.periods_per_year)
    ic_t_stat = float(ttest_1samp(ic_valid[:ic_valid_count], popmean=0)[0]) if ic_valid_count > 0 else math.nan

    result: ProcessFactorResult = {
        "na_pct": round(calculate_na_pct(factor_arr), 2),
        "ic": ic,
        "ic_t_stat": ic_t_stat,
        "annualized_high_quantile_pct": annualize_return(high_quantile_rets, worker_ctx.periods_per_year) * 100,
        "annualized_low_quantile_pct": (
            annualize_return(low_quantile_rets, worker_ctx.periods_per_year) * 100 if params.low_quantile > 0 else 0
        ),
        "asc": ascending,
        "returns": aligned_returns,
        **factor_metrics,
    }

    return result, quantile_perf_per_date


def analyze_factors(
    df: pl.DataFrame,
    benchmark_returns: np.ndarray,
    dataset_details: DatasetDetails,
    factor_columns: list[str],
    params: AnalysisParams,
    periods_per_year: float,
    on_progress: Callable[[int, int], None],
) -> dict[str, ProcessFactorResult]:
    perf_arr = df[FUTURE_PERF_COLUMN].to_numpy().astype(np.float32)
    perf_mask = np.isfinite(perf_arr) & (perf_arr <= (params.max_return_pct / 100))

    counts, unique_dates = df.lazy().select(pl.col("Date").rle().struct.unnest()).select("len", "value").collect().to_numpy().T

    ends = np.cumsum(counts)
    starts = np.zeros_like(ends)
    starts[1:] = ends[:-1]
    offsets = np.column_stack((starts, ends))

    n_workers = _calc_workers(len(factor_columns), len(df))
    results: dict[str, ProcessFactorResult] = {}

    logger.info(f"Workers launched: {n_workers}")

    logged_first = False

    with get_executor(n_workers, perf_arr, perf_mask, benchmark_returns, offsets, params, dataset_details, periods_per_year) as submit:
        future_to_name = {submit(_process_factor, name, name in params.asc_factors): name for name in factor_columns}

        for i, f in enumerate(as_completed(future_to_name), start=1):
            name = future_to_name[f]
            res = f.result()
            try:
                results[name] = res[0]
                if not logged_first:
                    with DatasetService(dataset_details) as dataset_svc:
                        arr = dataset_svc.read_column_pa(name).to_numpy().astype(np.float32)
                    _log_first_factor(name, res[0]["annualized_alpha_pct"], res[1], arr, unique_dates, periods_per_year, perf_arr, offsets)
                    logged_first = True
            except Exception:
                logger.error(f"ANALYSIS FAILED - Factor {name}: {traceback.format_exc()}")

            on_progress(i, len(factor_columns))

    return results


def _calc_workers(n_factors: int, n_rows: int) -> int:
    if n_factors * n_rows < 1_000_000:
        return 1
    max_cpus = min(16, cpu_count() or 4)
    return max(1, min(n_factors // 2, n_rows // 10_000, max_cpus))


def _log_first_factor(
    factor: str,
    annualized_alpha_pct: float,
    quantile_perf: np.ndarray,
    first_factor_data: np.ndarray,
    unique_dates: np.ndarray,
    periods_per_year: float,
    perf_arr: np.ndarray,
    offsets: np.ndarray,
):
    total_alpha_pct = 100 * ((1 + annualized_alpha_pct / 100) ** (len(unique_dates) / periods_per_year) - 1)

    lines = []
    for i, date in enumerate(unique_dates):
        start, end = offsets[i]
        if start is not None and end is not None:
            f_mean = f"{np.nanmean(first_factor_data[start:end]):8.2f}"
            p_mean = f"{np.nanmean(perf_arr[start:end]) * 100:6.2f}%"
        else:
            f_mean = f"{'NaN':>8}"
            p_mean = f"{'NaN':>7}"

        lines.append(
            f"  {date} | "
            f"High Q: {quantile_perf[i, 0]*100:6.2f}% | "
            f"Low Q: {quantile_perf[i, 1]*100:6.2f}% | "
            f"Factor Mean: {f_mean} | "
            f"Perf Mean: {p_mean}"
        )

    detailed_report = "\n".join(lines)
    logger.info(
        f"\n{'='*60}\n"
        f"FIRST FACTOR BREAKDOWN: [{factor}]\n"
        f"Total Alpha: {total_alpha_pct:.2f}%\n"
        f"Annualized Alpha: {annualized_alpha_pct:.2f}%\n"
        f"{detailed_report}\n"
    )
