from collections.abc import Callable
from concurrent.futures import ProcessPoolExecutor, as_completed
import logging
from os import cpu_count
from typing import Any
import numpy as np
import polars as pl
from scipy.stats import ttest_1samp
import traceback
from multiprocessing import shared_memory
from contextlib import ExitStack, contextmanager

from src.core.calculations.utils import annualize_return, calculate_factor_metric, calculate_na_pct, weighted_ic
from src.core.types.models import AnalysisParams, DatasetDetails, ProcessFactorResult, SharedArrayMetadata, WorkerContext
from src.services.dataset_service import DatasetService
from src.core.config.constants import INTERNAL_FUTURE_PERF_COL

logger = logging.getLogger("calculations")

worker_ctx: WorkerContext | None = None
worker_dataset_svc: DatasetService
worker_shm_handles: list[shared_memory.SharedMemory] = []


def _init_worker(
    metadata: dict[str, SharedArrayMetadata] | None,
    params: AnalysisParams,
    dataset_details: DatasetDetails,
    periods_per_year: float,
    local_arrays: dict[str, np.ndarray] | None = None,
):
    global worker_ctx, worker_dataset_svc, worker_shm_handles

    for shm in worker_shm_handles:
        try:
            shm.close()
        except Exception:
            pass
    worker_shm_handles.clear()

    worker_dataset_svc = DatasetService(dataset_details)

    arrays = reconstruct_shared_arrays(metadata) if metadata else local_arrays

    assert arrays is not None, "Worker failed to initialize data"

    worker_ctx = WorkerContext(params=params, dataset_details=dataset_details, arrays=arrays, periods_per_year=periods_per_year)


@contextmanager
def get_executor(n_workers, arrays, params, dataset_details, periods_per_year):
    with ExitStack() as stack:
        if n_workers > 1:
            active_shms, metadata = share_arrays(arrays)

            def cleanup_shm():
                for shm in active_shms:
                    try:
                        shm.close()
                        shm.unlink()
                    except FileNotFoundError:
                        pass

            stack.callback(cleanup_shm)

            executor = ProcessPoolExecutor(
                max_workers=n_workers, initializer=_init_worker, initargs=(metadata, params, dataset_details, periods_per_year)
            )

            stack.callback(executor.shutdown, wait=True)
            yield executor

        else:
            _init_worker(None, params, dataset_details, periods_per_year, arrays)
            yield None


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


def _process_factor(factor: str, ascending: bool) -> tuple[ProcessFactorResult, np.ndarray]:
    if not worker_ctx:
        raise RuntimeError("Worker state not initialized")

    with worker_dataset_svc:
        factor_arr = worker_dataset_svc.read_column_pa(factor).to_numpy().astype(np.float32)

    if not ascending:
        np.negative(factor_arr, out=factor_arr)

    benchmark_returns = worker_ctx.arrays["benchmark_returns"]
    offsets = worker_ctx.arrays["offsets"]

    total_dates = len(offsets)
    aligned_returns = np.full(total_dates, np.nan, dtype=np.float32)

    factor_stats_per_date = np.empty((len(offsets), 3), dtype=np.float32)

    for i, (start, end) in enumerate(offsets):
        f_group, p_group, m_group = (
            factor_arr[start:end],
            worker_ctx.arrays["perf_arr"][start:end],
            worker_ctx.arrays["perf_mask"][start:end],
        )
        valid_mask = m_group & np.isfinite(f_group)

        if not np.any(valid_mask):
            factor_stats_per_date[i] = np.nan
            continue

        factor_stats_per_date[i] = _process_factor_per_date(
            f_group[valid_mask], p_group[valid_mask], worker_ctx.params.high_quantile, worker_ctx.params.low_quantile
        )

    ic_per_date = factor_stats_per_date[:, 0]
    valid = np.isfinite(benchmark_returns) & np.all(np.isfinite(factor_stats_per_date[:, 1:3]), axis=1)

    high_quantile_rets = factor_stats_per_date[valid, 1]
    low_quantile_rets = factor_stats_per_date[valid, 2]
    benchmark_returns_valid = benchmark_returns[valid]

    factor_metrics = calculate_factor_metric(high_quantile_rets, benchmark_returns_valid, worker_ctx.periods_per_year)
    ic_valid = ic_per_date[np.isfinite(ic_per_date)]
    ic_t_stat: Any = ttest_1samp(ic_valid, popmean=0)[0]

    aligned_returns[valid] = high_quantile_rets - low_quantile_rets

    result: ProcessFactorResult = {
        "na_pct": round(calculate_na_pct(factor_arr), 2),
        "ic": float(np.nanmean(ic_per_date)),
        "ic_t_stat": float(ic_t_stat),
        "annualized_high_quantile_pct": annualize_return(high_quantile_rets, worker_ctx.periods_per_year) * 100,
        "annualized_low_quantile_pct": (
            annualize_return(low_quantile_rets, worker_ctx.periods_per_year) * 100 if worker_ctx.params.low_quantile > 0 else 0
        ),
        "asc": ascending,
        "returns": aligned_returns,
        **factor_metrics,
    }

    return result, factor_stats_per_date


def analyze_factors(
    df: pl.DataFrame,
    benchmark_returns: np.ndarray,
    dataset_details: DatasetDetails,
    factor_columns: list[str],
    params: AnalysisParams,
    periods_per_year: float,
    on_progress: Callable[[int, int], None],
) -> dict[str, ProcessFactorResult]:
    df = df.sort("Date")
    perf_arr = df[INTERNAL_FUTURE_PERF_COL].to_numpy().astype(np.float32)
    perf_mask = np.isfinite(perf_arr) & (perf_arr <= (params.max_return_pct / 100))

    unique_dates, counts = np.unique(df["Date"].to_numpy(), return_counts=True)
    ends = np.cumsum(counts)
    starts = np.zeros_like(ends)
    starts[1:] = ends[:-1]
    offsets = np.column_stack((starts, ends))

    n_workers = _calc_workers(len(factor_columns), len(df))
    results: dict[str, ProcessFactorResult] = {}

    logger.info(f"Workers launched: {n_workers}")

    arrays = {"perf_arr": perf_arr, "perf_mask": perf_mask, "benchmark_returns": benchmark_returns, "offsets": offsets}

    logged_first = False

    with get_executor(n_workers, arrays, params, dataset_details, periods_per_year) as executor:

        for i, (name, res) in enumerate(_get_results(executor, factor_columns, params)):
            try:
                results[name] = res[0]
                if not logged_first:
                    with DatasetService(dataset_details) as dataset_svc:
                        arr = dataset_svc.read_column_pa(name).to_numpy().astype(np.float32)
                    _log_first_factor(name, res[0]["annualized_alpha_pct"], res[1], arr, unique_dates, periods_per_year, perf_arr, offsets)
                    logged_first = True
            except Exception:
                logger.error(f"ANALYSIS FAILED - Factor {name}: {traceback.format_exc()}")

            on_progress(i + 1, len(factor_columns))

    return results


def _get_results(executor: ProcessPoolExecutor | None, factor_columns: list[str], params: AnalysisParams):
    if executor:
        future_to_name = {executor.submit(_process_factor, name, name in params.asc_factors): name for name in factor_columns}
        for f in as_completed(future_to_name):
            yield future_to_name[f], f.result()
    else:
        for n in factor_columns:
            yield n, _process_factor(n, n in params.asc_factors)


def reconstruct_shared_arrays(arrays_metadata: dict[str, SharedArrayMetadata]) -> dict[str, np.ndarray]:
    global worker_shm_handles
    reconstructed_arrs = {}

    for key, info in arrays_metadata.items():
        shm = shared_memory.SharedMemory(name=info["name"])

        worker_shm_handles.append(shm)

        arr = np.ndarray(shape=info["shape"], dtype=info["dtype"], buffer=shm.buf)

        reconstructed_arrs[key] = arr

    return reconstructed_arrs


def share_arrays(arrays: dict[str, np.ndarray]) -> tuple[list[shared_memory.SharedMemory], dict[str, SharedArrayMetadata]]:
    shm_objs = []
    metadata: dict[str, SharedArrayMetadata] = {}
    for key, arr in arrays.items():
        arr = np.ascontiguousarray(arr)
        shm = shared_memory.SharedMemory(create=True, size=arr.nbytes)
        shared_arr = np.ndarray(arr.shape, dtype=arr.dtype, buffer=shm.buf)
        shared_arr[:] = arr[:]
        shm_objs.append(shm)
        metadata[key] = {"name": shm.name, "shape": arr.shape, "dtype": str(arr.dtype)}
    return shm_objs, metadata


def _calc_workers(n_factors: int, n_rows: int) -> int:
    if n_factors * n_rows < 1_000_000:
        return 1
    max_cpus = min(16, cpu_count() or 4)
    return max(1, min(n_factors // 2, n_rows // 10_000, max_cpus))


def _log_first_factor(
    factor: str,
    annualized_alpha_pct: float,
    stats: np.ndarray,
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

        lines.append(
            f"  {date} | "
            f"High Q: {stats[i, 1]*100:6.2f}% | "
            f"Low Q: {stats[i, 2]*100:6.2f}% | "
            f"Factor Mean: {np.nanmean(first_factor_data[start:end]):8.2f} | "
            f"Perf Mean: {np.nanmean(perf_arr[start:end]) * 100:6.2f}%"
        )

    detailed_report = "\n".join(lines)
    logger.info(
        f"\n{'='*60}\n"
        f"FIRST FACTOR BREAKDOWN: [{factor}]\n"
        f"Total Alpha: {total_alpha_pct:.2f}%\n"
        f"Annualized Alpha: {annualized_alpha_pct:.2f}%\n"
        f"{detailed_report}\n"
    )
