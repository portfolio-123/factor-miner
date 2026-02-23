import logging
import sys
import traceback
from datetime import datetime

import pandas as pd

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stderr,
)
stderr_logger = logging.getLogger("worker")


from dotenv import load_dotenv

load_dotenv()

from src.core.config.constants import PRICE_FORMULA, PRICE_FORMULA_FRIDAY, BASE_REQUIRED_COLUMNS
from src.core.types.models import (
    Analysis,
    AnalysisStatus,
    AnalysisProgress,
    AnalysisResults,
    DatasetType,
)
from src.core.utils.common import serialize_dataframe, find_column_by_formula, extract_benchmark_ticker
from src.workers.analysis_service import AnalysisService
from src.services.dataset_service import DatasetService
from src.services.p123_client import fetch_benchmark_data
from src.core.calculations import (
    calculate_benchmark_returns,
    calculate_future_performance,
    analyze_factors,
    calculate_factor_metrics,
    calculate_correlation_matrix,
    select_best_features,
)


class AnalysisRunner:
    def __init__(self, fl_id: str, analysis_id: str):
        self.fl_id = fl_id
        self.analysis_id = analysis_id
        self.service = AnalysisService()
        self.analysis: Analysis | None = None

    def log(self, message: str) -> None:
        timestamp = datetime.now().strftime("%H:%M:%S")
        formatted = f"[{timestamp}] {message}"

        if self.analysis:
            self.analysis = self.service.append_log(self.analysis, formatted)

    def update(self, **updates) -> None:
        if self.analysis:
            self.analysis = self.service.save(self.analysis, **updates)

    def run(self) -> dict:
        self.log("Starting analysis...")
        stderr_logger.info("Starting analysis")

        params = self.analysis.params
        self.log(f"Processing dataset: {self.fl_id}")

        stderr_logger.info("Opening DatasetService")
        with DatasetService(self.fl_id) as dataset_svc:
            stderr_logger.info("Getting metadata")
            dataset_info = dataset_svc.get_metadata()

            if dataset_info.type == DatasetType.DATE:
                raise ValueError("single-date")

            price_column = find_column_by_formula(dataset_info.formulas, PRICE_FORMULA)
            price_column_friday = find_column_by_formula(dataset_info.formulas, PRICE_FORMULA_FRIDAY)
            required_columns = BASE_REQUIRED_COLUMNS + [price_column, price_column_friday]

            start_dt = pd.to_datetime(dataset_info.startDt) - pd.Timedelta(days=7)
            end_dt = dataset_info.endDt
            benchmark_ticker = extract_benchmark_ticker(dataset_info.benchmark)

            self.log(f"Fetching benchmark data for {benchmark_ticker}...")
            stderr_logger.info(f"Fetching benchmark data for {benchmark_ticker}")
            try:
                benchmark_data = fetch_benchmark_data(
                    benchmark_ticker=benchmark_ticker,
                    access_token=params.access_token,
                    start_date=start_dt.strftime("%Y-%m-%d"),
                    end_date=end_dt,
                )
            finally:
                self.analysis = self.service.clear_credentials(self.analysis)

            self.log("Benchmark data fetched successfully")
            stderr_logger.info("Benchmark data fetched")

            def on_progress(completed: int, total: int) -> None:
                percent = (completed * 100) // total
                prev_percent = ((completed - 1) * 100) // total if completed > 1 else -1
                if percent // 10 > prev_percent // 10 or completed == total:
                    self.log(f"Progress: {percent}% ({completed}/{total} factors)")
                    stderr_logger.info(f"PROGRESS: {percent}% ({completed}/{total} factors)")
                self.update(
                    status=AnalysisStatus.RUNNING,
                    progress=AnalysisProgress(
                        completed=completed,
                        total=total,
                    ),
                )

            factor_columns = [
                col
                for col in dataset_svc.column_names
                if col not in required_columns
            ]
            stderr_logger.info(f"Found {len(factor_columns)} factor columns")

            self.update(
                status=AnalysisStatus.RUNNING,
                progress=AnalysisProgress(completed=0, total=len(factor_columns)),
            )

            self.log("Calculating future performance...")
            stderr_logger.info("Reading price columns for future performance")
            perf_core = dataset_svc.read_columns(["Date", "Ticker", price_column])
            stderr_logger.info(" Calculating future performance")
            future_perf_df = calculate_future_performance(perf_core, price_column)

            self.log("Analyzing factors...")
            stderr_logger.info("Starting analyze_factors")
            results_df, factor_stats = analyze_factors(
                future_perf_df,
                dataset_svc,
                factor_columns=factor_columns,
                top_pct=params.top_pct,
                bottom_pct=params.bottom_pct,
                progress_fn=on_progress,
            )
            stderr_logger.info(f"analyze_factors complete, {len(results_df)} results")

            stderr_logger.info("Reading Date column")
            raw_data = dataset_svc.read_columns(["Date"])

            self.log("Calculating benchmark returns...")
            stderr_logger.info("Calculating benchmark returns")
            raw_data = calculate_benchmark_returns(
                raw_data,
                benchmark_data,
                frequency=dataset_info.frequency,
            )
            if results_df.empty:
                raise ValueError("No results from factor analysis")

            self.log("Calculating factor metrics...")
            stderr_logger.info("Calculating factor metrics")
            metrics_df = calculate_factor_metrics(
                results_df,
                raw_data,
                periods_per_year=dataset_info.frequency.periods_per_year,
                factor_stats=factor_stats,
            )
            stderr_logger.info(f"factor metrics complete, {len(metrics_df)} factors")

            self.log("Calculating correlation matrix...")
            stderr_logger.info("Calculating correlation matrix")
            corr_matrix = calculate_correlation_matrix(results_df)
            stderr_logger.info("Correlation matrix complete")

            metrics_df = metrics_df.round(4)
            corr_matrix = corr_matrix.round(4)

            avg_abs_alpha = float(metrics_df["annualized alpha %"].abs().mean())
            self.log(f"Average absolute alpha: {avg_abs_alpha:.2f}%")

            stderr_logger.info("Selecting best features")
            best_feature_names, factor_classifications = select_best_features(
                metrics_df=metrics_df,
                correlation_matrix=corr_matrix,
                N=params.n_factors,
                correlation_threshold=params.correlation_threshold,
                a_min=params.min_alpha,
                max_na_pct=params.max_na_pct,
                min_ic=params.min_ic,
            )
            best_factors_count = len(best_feature_names)
            self.log(f"Best factors: {best_factors_count}/{len(metrics_df)}")
            stderr_logger.info(f"Selected {best_factors_count} best features")

            self.log("Analysis complete!")
            stderr_logger.info("Analysis complete")

            return {
                "all_metrics": serialize_dataframe(metrics_df),
                "all_corr_matrix": serialize_dataframe(corr_matrix),
                "avg_abs_alpha": avg_abs_alpha,
                "best_feature_names": best_feature_names,
                "factor_classifications": factor_classifications,
            }

    def execute(self) -> None:
        stderr_logger.info(f"Worker started for {self.fl_id}/{self.analysis_id}")
        self.log(f"Worker started for analysis: {self.fl_id}/{self.analysis_id}")

        self.analysis = self.service.get(self.fl_id, self.analysis_id)
        if self.analysis is None:
            stderr_logger.error(f"Analysis {self.fl_id}/{self.analysis_id} not found")
            self.log(f"Analysis {self.fl_id}/{self.analysis_id} not found")
            sys.exit(1)

        try:
            self.update(status=AnalysisStatus.RUNNING)
            self.log("Status updated to running")

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
            stderr_logger.info("SUCCESS: Analysis completed")

        except Exception as e:
            error_msg = f"{str(e)}\n\n{traceback.format_exc()}"
            stderr_logger.error(f"EXCEPTION: {error_msg}")
            self.log(f"Error: {error_msg}")
            self.update(status=AnalysisStatus.FAILED, error=error_msg)
            sys.exit(1)


def main():
    fl_id = sys.argv[1]
    analysis_id = sys.argv[2]
    runner = AnalysisRunner(fl_id, analysis_id)
    runner.execute()


if __name__ == "__main__":
    main()
