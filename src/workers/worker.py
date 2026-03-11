import logging
import sys
import traceback
from datetime import datetime, timedelta

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
    Analysis,
    AnalysisStatus,
    AnalysisProgress,
    AnalysisResults,
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


class AnalysisRunner:
    def __init__(self, fl_id: str, analysis_id: str, user_uid: str):
        self.fl_id = fl_id
        self.analysis_id = analysis_id
        self.user_uid = user_uid
        self.service = AnalysisService(user_uid)
        self.analysis: Analysis | None = None

    def log(self, message: str) -> None:
        stderr_logger.info(message)

    def update(self, **updates) -> None:
        if self.analysis:
            self.analysis = self.service.save(self.analysis, **updates)

    def run(self) -> dict:
        self.log("Starting analysis...")

        params = self.analysis.params
        self.log(f"Processing dataset: {self.fl_id}")

        with DatasetService(self.fl_id, self.user_uid) as dataset_svc:
            dataset_info = dataset_svc.get_metadata()

            if dataset_info.type == DatasetType.DATE:
                raise ValueError("[single-date]")

            price_column = find_price_column(dataset_svc.column_names, PRICE_COLUMN_NAMES)
            # Exclude all price column names from analysis (never analyze them)
            required_columns = BASE_REQUIRED_COLUMNS + PRICE_COLUMN_NAMES

            start_dt = datetime.strptime(dataset_info.startDt[:10], "%Y-%m-%d")
            # extend by full rebalance period + 7 days buffer
            forward_days = dataset_info.frequency.weeks * 7 + 7
            end_dt_raw = datetime.strptime(dataset_info.endDt[:10], "%Y-%m-%d") + timedelta(days=forward_days)
            end_dt = min(end_dt_raw, datetime.today().replace(hour=0, minute=0, second=0, microsecond=0))
            benchmark_ticker = extract_benchmark_ticker(dataset_info.benchmark)

            self.log(f"Fetching benchmark data for {benchmark_ticker}...")
            try:
                date_range = (start_dt.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d"))
                if INTERNAL_MODE:  # internal
                    benchmark_data = fetch_benchmark_data(
                        benchmark_ticker=benchmark_ticker,
                        access_token=params.access_token,
                        start_date=date_range[0],
                        end_date=date_range[1],
                    )
                else:  # external
                    benchmark_data = fetch_benchmark_external(
                        ticker=benchmark_ticker,
                        start_date=date_range[0],
                        end_date=date_range[1],
                    )
            finally:
                self.analysis = self.service.clear_credentials(self.analysis)

            self.log("Benchmark data fetched successfully")

            def on_progress(completed: int, total: int) -> None:
                percent = (completed * 100) // total
                prev_percent = ((completed - 1) * 100) // total if completed > 1 else -1
                if percent // 10 > prev_percent // 10 or completed == total:
                    self.log(f"Progress: {percent}% ({completed}/{total} factors)")
                self.update(
                    status=AnalysisStatus.RUNNING,
                    progress=AnalysisProgress(
                        completed=completed,
                        total=total,
                    ),
                )

            factor_columns = [
                col for col in dataset_svc.column_names if col not in required_columns
            ]
            self.log(f"Found {len(factor_columns)} factor columns")

            self.update(
                status=AnalysisStatus.RUNNING,
                progress=AnalysisProgress(completed=0, total=len(factor_columns)),
            )

            self.log("Calculating future performance...")
            perf_core = dataset_svc.read_columns(["Date", "Ticker", price_column])
            future_perf_df = calculate_future_performance(perf_core, price_column)

            self.log("Analyzing factors...")
            results_df, factor_stats = analyze_factors(
                future_perf_df,
                dataset_svc,
                factor_columns=factor_columns,
                top_pct=params.top_pct,
                bottom_pct=params.bottom_pct,
                on_progress=on_progress,
            )

            raw_data = dataset_svc.read_columns(["Date"])

            self.log("Calculating benchmark returns...")
            raw_data = calculate_benchmark_returns(
                raw_data,
                benchmark_data,
                frequency=dataset_info.frequency,
            )
            if results_df.is_empty():
                raise ValueError("No results from factor analysis")

            self.log("Calculating factor metrics...")
            metrics_df = calculate_factor_metrics(
                results_df,
                raw_data,
                periods_per_year=dataset_info.frequency.periods_per_year,
                factor_stats=factor_stats,
            )

            self.log("Calculating correlation matrix...")
            corr_matrix = calculate_correlation_matrix(results_df)

            metrics_df = metrics_df.with_columns(cs.numeric().round(4))
            corr_matrix = corr_matrix.with_columns(cs.numeric().round(4))

            avg_abs_alpha = float(metrics_df["annualized alpha %"].abs().mean())
            self.log(f"Average absolute alpha: {avg_abs_alpha:.2f}%")

            best_feature_names, factor_classifications = select_best_features(
                metrics_df=metrics_df,
                correlation_matrix=corr_matrix,
                N=params.n_factors,
                correlation_threshold=params.correlation_threshold,
                a_min=params.min_alpha,
                max_na_pct=params.max_na_pct,
                min_ic=params.min_ic,
                rank_by=getattr(params, "rank_by", "Alpha"),
            )
            best_factors_count = len(best_feature_names)
            self.log(f"Best factors: {best_factors_count}/{len(metrics_df)}")

            self.log("Analysis complete!")

            return {
                "all_metrics": serialize_dataframe(metrics_df),
                "all_corr_matrix": serialize_dataframe(corr_matrix),
                "avg_abs_alpha": avg_abs_alpha,
                "best_feature_names": best_feature_names,
                "factor_classifications": factor_classifications,
            }

    def execute(self) -> None:
        self.log(f"Worker started for {self.fl_id}/{self.analysis_id}")

        self.analysis = self.service.get(self.fl_id, self.analysis_id)
        if self.analysis is None:
            stderr_logger.error(f"Analysis {self.fl_id}/{self.analysis_id} not found")
            sys.exit(1)

        try:
            self.update(status=AnalysisStatus.RUNNING)

            results = self.run()

            self.update(
                status=AnalysisStatus.SUCCESS,
                results=AnalysisResults(
                    all_metrics=results["all_metrics"],
                    all_corr_matrix=results["all_corr_matrix"],
                    best_feature_names=results["best_feature_names"],
                    factor_classifications=results["factor_classifications"],
                ),
                avg_abs_alpha=results["avg_abs_alpha"],
                best_factors_count=len(results["best_feature_names"]),
            )
            self.log("Analysis completed successfully")

        except Exception as e:
            error_msg = f"{str(e)}\n\n{traceback.format_exc()}"
            stderr_logger.error(f"EXCEPTION: {error_msg}")
            self.update(status=AnalysisStatus.FAILED, error=error_msg)
            sys.exit(1)


def main():
    fl_id = sys.argv[1]
    analysis_id = sys.argv[2]
    user_uid = sys.argv[3] or None
    runner = AnalysisRunner(fl_id, analysis_id, user_uid)
    runner.execute()


if __name__ == "__main__":
    main()
