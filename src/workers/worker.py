from collections.abc import Callable
import logging
import sys
import traceback
from datetime import date, datetime, timedelta
from typing import TypedDict

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stderr,
)
stderr_logger = logging.getLogger("worker")

import polars.selectors as cs

from src.core.config.environment import INTERNAL_MODE
from src.core.config.constants import PRICE_COLUMN_NAMES, BASE_REQUIRED_COLUMNS
from src.core.types.models import (
    AnalysisParams,
    AnalysisStatus,
    AnalysisProgress,
    AnalysisResults,
    AnalysisUpdate,
    DatasetType,
)
from src.core.utils.common import (
    serialize_dataframe,
    find_price_column,
    extract_benchmark_ticker,
)
from src.workers.analysis_service import AnalysisService
from src.services.dataset_service import DatasetService
from src.internal.p123_client import fetch_benchmark_data
from src.services.benchmark_service import fetch_benchmark_external
from src.core.calculations.forward_returns import (
    calculate_benchmark_returns,
    calculate_future_performance,
)
from src.core.calculations.factor_analysis import (
    analyze_factors,
    calculate_factor_metrics,
)
from src.core.calculations.feature_selection import (
    calculate_correlation_matrix,
    select_best_features,
)


class AnalysisRunResult(TypedDict):
    all_metrics: str
    all_corr_matrix: str
    avg_abs_alpha: float
    best_feature_names: list[str]
    factor_classifications: dict[str, str]


def run_analysis(
    update: Callable[[AnalysisUpdate], None],
    logger: logging.Logger,
    access_token: str | None,
    params: AnalysisParams,
    dataset_svc: DatasetService,
) -> AnalysisRunResult:

    dataset_info = dataset_svc.get_metadata()

    if dataset_info.type == DatasetType.DATE:
        raise ValueError("[single-date]")

    price_column = find_price_column(dataset_svc.column_names, PRICE_COLUMN_NAMES)
    # Exclude all price column names from analysis (never analyze them)
    required_columns = BASE_REQUIRED_COLUMNS + PRICE_COLUMN_NAMES

    start_dt = datetime.strptime(dataset_info.startDt[:10], "%Y-%m-%d")
    # extend by full rebalance period + 7 days buffer
    forward_days = dataset_info.frequency.weeks * 7 + 7
    end_dt_raw = datetime.strptime(dataset_info.endDt[:10], "%Y-%m-%d") + timedelta(
        days=forward_days
    )
    end_dt = min(end_dt_raw, date.today())
    benchmark_ticker = extract_benchmark_ticker(dataset_info.benchmark)

    logger.info(f"Fetching benchmark data for {benchmark_ticker}...")
    date_range = (start_dt.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d"))
    if INTERNAL_MODE:  # internal
        benchmark_data = fetch_benchmark_data(
            benchmark_ticker=benchmark_ticker,
            access_token=access_token,
            start_date=date_range[0],
            end_date=date_range[1],
        )
    else:  # external
        benchmark_data = fetch_benchmark_external(
            ticker=benchmark_ticker,
            start_date=date_range[0],
            end_date=date_range[1],
        )

    logger.info("Benchmark data fetched successfully")

    def on_progress(completed: int, total: int) -> None:
        percent = (completed * 100) // total
        prev_percent = ((completed - 1) * 100) // total if completed > 1 else -1
        if percent // 10 > prev_percent // 10 or completed == total:
            logger.info(f"Progress: {percent}% ({completed}/{total} factors)")
        update({"progress": AnalysisProgress(completed=completed, total=total)})

    factor_columns = [
        col for col in dataset_svc.column_names if col not in required_columns
    ]
    logger.info(f"Found {len(factor_columns)} factor columns")

    update({"progress": AnalysisProgress(completed=0, total=len(factor_columns))})

    logger.info("Calculating future performance...")
    core_df = dataset_svc.read_columns(["Date", "Ticker", price_column])
    future_perf_df = calculate_future_performance(core_df, price_column)

    logger.info("Analyzing factors...")
    results_df, factor_stats = analyze_factors(
        future_perf_df,
        dataset_svc,
        core_df=core_df,
        factor_columns=factor_columns,
        top_pct=params.top_pct,
        bottom_pct=params.bottom_pct,
        on_progress=on_progress,
    )

    raw_data = core_df.select(["Date"])

    logger.info("Calculating benchmark returns...")
    raw_data = calculate_benchmark_returns(
        raw_data,
        benchmark_data,
        frequency=dataset_info.frequency,
    )
    if results_df.is_empty():
        raise ValueError("No results from factor analysis")

    logger.info("Calculating factor metrics...")
    metrics_df = calculate_factor_metrics(
        results_df,
        raw_data,
        periods_per_year=dataset_info.frequency.periods_per_year,
        factor_stats=factor_stats,
    )

    logger.info("Calculating correlation matrix...")
    corr_matrix = calculate_correlation_matrix(results_df)

    metrics_df = metrics_df.with_columns(cs.numeric().round(4))
    corr_matrix = corr_matrix.with_columns(cs.numeric().round(4))

    avg_abs_alpha = float(metrics_df["annualized alpha %"].abs().mean())
    logger.info(f"Average absolute alpha: {avg_abs_alpha:.2f}%")

    best_feature_names, factor_classifications = select_best_features(
        metrics_df=metrics_df,
        correlation_matrix=corr_matrix,
        n=params.n_factors,
        correlation_threshold=params.correlation_threshold,
        a_min=params.min_alpha,
        max_na_pct=params.max_na_pct,
        min_ic=params.min_ic,
        rank_by=params.rank_by,
    )
    best_factors_count = len(best_feature_names)
    logger.info(f"Best factors: {best_factors_count}/{len(metrics_df)}")

    logger.info("Analysis complete!")

    return {
        "all_metrics": serialize_dataframe(metrics_df),
        "all_corr_matrix": serialize_dataframe(corr_matrix),
        "avg_abs_alpha": avg_abs_alpha,
        "best_feature_names": best_feature_names,
        "factor_classifications": factor_classifications,
    }


def save_results(update: Callable[[AnalysisUpdate], None], results: AnalysisRunResult):
    update(
        {
            "status": AnalysisStatus.SUCCESS,
            "results": AnalysisResults(
                all_metrics=results["all_metrics"],
                all_corr_matrix=results["all_corr_matrix"],
                best_feature_names=results["best_feature_names"],
                factor_classifications=results["factor_classifications"],
            ),
            "avg_abs_alpha": results["avg_abs_alpha"],
            "best_factors_count": len(results["best_feature_names"]),
        }
    )


def main(fl_id: str, analysis_id: str, user_uid: str | None, access_token: str | None):
    if user_uid == "":
        user_uid = None
    if access_token == "":
        access_token = None

    service = AnalysisService(user_uid)
    analysis = service.get(fl_id, analysis_id)
    if analysis is None:
        stderr_logger.error(f"Analysis {fl_id}/{analysis_id} not found")
        sys.exit(1)

    def update(updates: AnalysisUpdate):
        nonlocal analysis
        assert analysis is not None
        analysis = service.save(analysis, updates)

    try:
        stderr_logger.info(f"Worker started for {fl_id}/{analysis_id}")
        update({"status": AnalysisStatus.RUNNING})
        stderr_logger.info("Starting analysis...")
        stderr_logger.info(f"Processing dataset: {fl_id}")
        with DatasetService(fl_id) as dataset_svc:
            results = run_analysis(
                update, stderr_logger, access_token, analysis.params, dataset_svc
            )
        save_results(update, results)
        stderr_logger.info("Analysis completed successfully")
    except Exception as e:
        error_msg = f"{str(e)}\n\n{traceback.format_exc()}"
        stderr_logger.error(f"EXCEPTION: {error_msg}")
        update({"status": AnalysisStatus.FAILED, "error": error_msg})
        sys.exit(1)


if __name__ == "__main__":
    main(*sys.argv)
