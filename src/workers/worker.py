from collections.abc import Callable
import logging
import polars as pl
import numpy as np
import sys
import traceback
from time import monotonic


from src.core.config.constants import (
    INTERNAL_BENCHMARK_COL,
    BASE_REQUIRED_COLUMNS,
    INTERNAL_FUTURE_PERF_COL,
    PRICE_COLUMN,
)
from src.core.types.models import (
    AnalysisParams,
    AnalysisStatus,
    AnalysisProgress,
    AnalysisResults,
    AnalysisUpdate,
    DatasetDetails,
    DatasetType,
    process_factor_result_scalars,
)
from src.core.utils.common import serialize_dataframe, extract_benchmark_ticker
from src.workers.analysis_service import AnalysisService
from src.services.dataset_service import DatasetService
from src.core.calculations.forward_returns import (
    add_future_performance_column,
    calculate_benchmark_returns,
)
from src.core.calculations.factor_analysis import analyze_factors
from src.core.calculations.feature_selection import (
    calculate_correlation_matrix,
    select_best_factors,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stderr,
)
stderr_logger = logging.getLogger("worker")


def run_analysis(
    update: Callable[[AnalysisUpdate], None],
    logger: logging.Logger,
    access_token: str | None,
    params: AnalysisParams,
    dataset_svc: DatasetService,
) -> AnalysisResults:
    dataset_info = dataset_svc.get_metadata()
    periods_per_year = dataset_info.frequency.periods_per_year

    if dataset_info.type == DatasetType.DATE:
        raise ValueError("[single-date]")

    # find the price column, the base columns (date, ticker) and exclude them from being analyzed
    column_names = dataset_svc.column_names

    factor_columns = [
        col for col in column_names if col not in {*BASE_REQUIRED_COLUMNS, PRICE_COLUMN}
    ]

    update({"progress": AnalysisProgress(completed=0, total=len(factor_columns))})

    logger.info("Calculating future performance...")

    core_df = (
        dataset_svc.read_columns_pl(["Date", "Ticker", PRICE_COLUMN])
        .cast({PRICE_COLUMN: pl.Float32})
        .with_columns(pl.col("Date").str.to_date("%Y-%m-%d"))
    )

    core_df = add_future_performance_column(core_df, PRICE_COLUMN).drop(PRICE_COLUMN)
    core_df = core_df.with_row_index("_orig_idx").filter(
        pl.col(INTERNAL_FUTURE_PERF_COL).is_not_null()
    )

    logger.info("Calculating benchmark returns...")

    benchmark_prices = DatasetService.fetch_benchmark(
        ticker=extract_benchmark_ticker(dataset_info.benchName),
        start_date=dataset_info.startDt[:10],
        end_date=dataset_info.endDt[:10],
        access_token=access_token,
    )

    benchmark_df = calculate_benchmark_returns(
        core_df.select("Date").unique().sort("Date"), benchmark_prices
    )

    logger.info("Benchmark data fetched successfully")

    logger.info("Analyzing factors...")
    factor_stats = analyze_factors(
        core_df,
        benchmark_df[INTERNAL_BENCHMARK_COL].to_numpy(),
        dataset_svc,
        factor_columns,
        params,
        periods_per_year,
        on_progress=lambda completed, total: update(
            {"progress": AnalysisProgress(completed=completed, total=total)}
        ),
    )

    if not factor_stats:
        raise ValueError("No results from factor analysis")

    logger.info("Calculating factor metrics...")
    wide_data: dict[str, np.ndarray] = {}
    results: list[dict[str, str | float]] = []
    for factor, data in factor_stats.items():
        data["column"] = factor  # type: ignore
        wide_data[factor] = data.pop("returns")  # type: ignore
        results.append(data)  # type: ignore

    factor_returns_wide = pl.DataFrame(
        wide_data, schema=[(f, pl.Float32) for f in factor_columns]
    )
    metrics_df = pl.DataFrame(
        results,
        schema=[
            ("column", pl.Utf8),
            *((col, pl.Float32) for col in process_factor_result_scalars),
        ],
    )

    logger.info("Calculating correlation matrix...")
    corr_matrix = calculate_correlation_matrix(factor_returns_wide)

    best_factors, factor_classifications = select_best_factors(
        metrics_df,
        corr_matrix,
        params,
    )
    logger.info(f"Best factors: {len(best_factors)}/{len(metrics_df)}")

    logger.info("Analysis complete")

    return AnalysisResults(
        all_metrics=serialize_dataframe(metrics_df),
        all_corr_matrix=serialize_dataframe(corr_matrix),
        best_feature_names=best_factors,
        factor_classifications=factor_classifications,
        avg_abs_alpha=float(metrics_df["annualized_alpha_pct"].abs().mean()),
    )


def save_results(update: Callable[[AnalysisUpdate], None], results: AnalysisResults):
    update(
        {
            "status": AnalysisStatus.SUCCESS,
            "results": results,
            "avg_abs_alpha": results.avg_abs_alpha,
            "best_factors_count": len(results.best_feature_names),
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

    last_progress_write_at = monotonic()

    def update(updates: AnalysisUpdate):
        nonlocal analysis, last_progress_write_at
        assert analysis is not None

        progress = updates.get("progress")
        if progress is not None:
            now = monotonic()
            if (
                progress.completed != progress.total
            ) and now - last_progress_write_at < 3:
                return
            last_progress_write_at = now

        analysis = service.save(analysis, updates)

    try:
        stderr_logger.info(f"Worker started for {fl_id}/{analysis_id}")
        update({"status": AnalysisStatus.RUNNING})
        stderr_logger.info("Starting analysis...")
        stderr_logger.info(f"Processing dataset: {fl_id}")
        with DatasetService(
            DatasetDetails(fl_id=fl_id, user_uid=user_uid)
        ) as dataset_svc:
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
    main(*sys.argv[1:])
