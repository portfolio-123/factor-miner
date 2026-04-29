from collections.abc import Callable
import logging
import polars as pl
import sys
import traceback
from time import monotonic
import numpy as np

from src.core.calculations.utils import annualize_return, cumulative_return
from src.core.config.constants import FUTURE_PERF_COLUMN, INTERNAL_BENCHMARK_COL, REQUIRED_COLUMNS, SPECIAL_COLUMNS
from src.core.types.models import (
    APICredentials,
    AnalysisError,
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
from src.core.calculations.forward_returns import calculate_benchmark_returns
from src.core.calculations.factor_analysis import analyze_factors
from src.core.calculations.feature_selection import calculate_correlation_matrix, select_best_factors

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", stream=sys.stderr)
stderr_logger = logging.getLogger("worker")


def run_analysis(
    update: Callable[[AnalysisUpdate], None],
    logger: logging.Logger,
    api_credentials: APICredentials | None,
    params: AnalysisParams,
    dataset_details: DatasetDetails,
) -> AnalysisResults:
    with DatasetService(dataset_details) as dataset_svc:
        column_names = set(dataset_svc.column_names)
        if missing := REQUIRED_COLUMNS - column_names:
            raise AnalysisError(f"Dataset is missing required columns: {missing}", error_type="missing-column")

        dataset_lf = dataset_svc.scan()
        core_df = (
            dataset_lf.select(REQUIRED_COLUMNS)
            .with_columns(pl.col("Date").str.strptime(pl.Date), (pl.col(FUTURE_PERF_COLUMN) / 100))
            .collect()
        )
        if not core_df.get_column("Date").is_sorted():
            raise ValueError("DataFrame must be sorted by 'Date' ascending.")

        factor_columns = list(column_names - SPECIAL_COLUMNS)
        dataset_info = dataset_svc.get_metadata()

    periods_per_year = dataset_info.frequency.periods_per_year

    if dataset_info.type == DatasetType.DATE:
        raise AnalysisError("Single-date datasets are not supported", error_type="single-date")

    update({"progress": AnalysisProgress(completed=0, total=len(factor_columns))})

    logger.info("Calculating benchmark returns...")

    benchmark_prices = DatasetService.fetch_benchmark(
        ticker=extract_benchmark_ticker(dataset_info.benchName),
        start_date=dataset_info.startDt[:10],
        end_date=dataset_info.endDt[:10],
        api_credentials=api_credentials,
    )

    dates = core_df.lazy().select(pl.col("Date").rle().struct.field("value").alias("Date"))
    benchmark_df = (
        calculate_benchmark_returns(dates, benchmark_prices.lazy())
        .select(pl.col("Date"), pl.col("Return").alias(INTERNAL_BENCHMARK_COL))
        .collect()
    )

    valid_benchmark = benchmark_df[INTERNAL_BENCHMARK_COL].drop_nulls().to_numpy()

    total_benchmark_return = cumulative_return(valid_benchmark) * 100
    annualized_benchmark_return = annualize_return(valid_benchmark, periods_per_year) * 100
    logger.info("Benchmark: cumulative %+.2f%%, annualized %+.2f%%", total_benchmark_return, annualized_benchmark_return)

    # log all the benchmark prices by date
    lines = "\n".join(f"{date}  {'N/A' if ret is None else f'{ret * 100:+.2f}%'}" for date, ret in benchmark_df.iter_rows())
    logger.info("Benchmark returns by period:\n%s", lines)

    logger.info("Analyzing factors...")
    factor_stats = analyze_factors(
        core_df,
        benchmark_df[INTERNAL_BENCHMARK_COL].to_numpy(),
        dataset_details,
        factor_columns,
        params,
        periods_per_year,
        on_progress=lambda completed, total: update({"progress": AnalysisProgress(completed=completed, total=total)}),
    )

    if not factor_stats:
        raise AnalysisError("No results from factor analysis")

    logger.info("Calculating factor metrics...")
    results: list[dict[str, str | float]] = []
    for factor, data in factor_stats.items():
        data["column"] = factor  # type: ignore
        results.append(data)  # type: ignore

    metrics_df = pl.DataFrame(results, schema=[("column", pl.Utf8), *((col, pl.Float32) for col in process_factor_result_scalars)])

    logger.info("Calculating correlation matrix...")

    best_factors, factor_classifications, corr_matrix = select_best_factors(dataset_lf, metrics_df, params)
    logger.info(f"Best factors: {len(best_factors)}/{len(metrics_df)}")

    logger.info("Analysis complete")

    return AnalysisResults(
        all_metrics=serialize_dataframe(metrics_df),
        all_corr_matrix=serialize_dataframe(corr_matrix),
        best_feature_names=best_factors,
        factor_classifications=factor_classifications,
        avg_alpha=float(np.nanmean(metrics_df.get_column("annualized_alpha_pct"))),  # type: ignore[arg-type]
        benchmark={"total_benchmark_return": float(total_benchmark_return), "annualized_benchmark_return": annualized_benchmark_return},
    )


def save_results(update: Callable[[AnalysisUpdate], None], results: AnalysisResults):
    update(
        {
            "status": AnalysisStatus.SUCCESS,
            "results": results,
            "avg_alpha": results.avg_alpha,
            "best_factors_count": len(results.best_feature_names),
        }
    )


def main(fl_id: str, analysis_id: str, user_uid: str | None, api_id: str | None, api_key: str | None):
    if user_uid == "":
        user_uid = None
    if api_id == "":
        api_id = None
    if api_key == "":
        api_key = None

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
            if (progress.completed != progress.total) and now - last_progress_write_at < 3:
                return
            last_progress_write_at = now

        analysis = service.save(analysis, updates)

    try:
        stderr_logger.info(f"Worker started for {fl_id}/{analysis_id}")
        update({"status": AnalysisStatus.RUNNING})
        stderr_logger.info("Starting analysis...")
        stderr_logger.info(f"Processing dataset: {fl_id}")

        results = run_analysis(
            update,
            stderr_logger,
            APICredentials(api_id=api_id, api_key=api_key) if api_id and api_key else None,
            analysis.params,
            DatasetDetails(fl_id=fl_id, user_uid=user_uid),
        )
        save_results(update, results)
        stderr_logger.info("Analysis completed successfully")
    except AnalysisError as e:
        error_msg = str(e)
        error_type = e.error_type
        update({"status": AnalysisStatus.FAILED, "error": error_msg, "error_type": error_type})
        sys.exit(1)
    except Exception as e:
        error_msg = f"{str(e)}\n\n{traceback.format_exc()}"
        stderr_logger.error(f"EXCEPTION: {error_msg}")
        update({"status": AnalysisStatus.FAILED, "error": error_msg})
        sys.exit(1)


if __name__ == "__main__":
    main(*sys.argv[1:])
