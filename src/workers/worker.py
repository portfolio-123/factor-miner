import sys
import traceback
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

from src.core.config.constants import PRICE_COLUMN, REQUIRED_COLUMNS
from src.core.types.models import (
    Analysis,
    AnalysisStatus,
    AnalysisProgress,
    AnalysisResults,
    DatasetType,
)
from src.core.utils.common import serialize_dataframe
from src.workers.analysis_service import AnalysisService
from src.services.dataset_service import DatasetService
from src.services.p123_client import fetch_benchmark_data
from src.core.calculations import (
    calculate_benchmark_returns,
    calculate_future_performance,
    analyze_factors,
    calculate_factor_metrics,
    calculate_correlation_matrix,
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

        params = self.analysis.params
        self.log(f"Processing dataset: {self.fl_id}")

        with DatasetService(self.fl_id) as dataset_svc:
            dataset_info = dataset_svc.get_metadata()

            is_date_type = dataset_info.type == DatasetType.DATE
            base_dt = dataset_info.asOfDt if is_date_type else dataset_info.startDt
            end_dt = dataset_info.asOfDt if is_date_type else dataset_info.endDt
            start_dt = pd.to_datetime(base_dt) - pd.Timedelta(days=7)

            self.log(f"Fetching benchmark data for {params.benchmark_ticker}...")
            try:
                benchmark_data = fetch_benchmark_data(
                    benchmark_ticker=params.benchmark_ticker,
                    access_token=params.access_token,
                    start_date=start_dt.strftime("%Y-%m-%d"),
                    end_date=end_dt,
                )
            finally:
                self.analysis = self.service.clear_credentials(self.analysis)

            self.log("Benchmark data fetched successfully")

            def on_progress(completed: int, total: int, current_factor: str = "") -> None:
                percent = (completed * 100) // total
                prev_percent = ((completed - 1) * 100) // total if completed > 1 else -1
                if percent // 10 > prev_percent // 10 or completed == total:
                    self.log(f"Progress: {percent}% ({completed}/{total} factors)")
                self.update(
                    status=AnalysisStatus.RUNNING,
                    progress=AnalysisProgress(
                        completed=completed,
                        total=total,
                        current_factor=current_factor,
                    ),
                )

            factor_columns = [
                col
                for col in dataset_svc.column_names
                if col not in REQUIRED_COLUMNS
            ]

            self.update(
                status=AnalysisStatus.RUNNING,
                progress=AnalysisProgress(completed=0, total=len(factor_columns)),
            )

            self.log("Calculating future performance...")
            perf_core = dataset_svc.read_columns(["Date", "Ticker", PRICE_COLUMN])
            future_perf_df = calculate_future_performance(perf_core, PRICE_COLUMN)
            self.log("Analyzing factors...")
            results_df = analyze_factors(
                future_perf_df,
                dataset_svc,
                factor_columns=factor_columns,
                top_pct=params.top_pct,
                bottom_pct=params.bottom_pct,
                progress_fn=on_progress,
            )
            raw_data = dataset_svc.read_columns(["Date"])
            self.log("Calculating benchmark returns...")
            raw_data = calculate_benchmark_returns(
                raw_data,
                benchmark_data,
                frequency=dataset_info.frequency,
            )
            if results_df.empty:
                raise ValueError("No results from factor analysis")

            self.log("Calculating factor metrics...")
            metrics_df = calculate_factor_metrics(
                results_df,
                raw_data,
                periods_per_year=dataset_info.frequency.periods_per_year,
            )

            self.log("Calculating correlation matrix...")
            corr_matrix = calculate_correlation_matrix(results_df)

            metrics_df = metrics_df.round(4)
            corr_matrix = corr_matrix.round(4)

            avg_abs_alpha = float(metrics_df["annualized alpha %"].abs().mean())
            self.log(f"Average absolute alpha: {avg_abs_alpha:.2f}%")

            self.log("Analysis complete!")

            return {
                "all_metrics": serialize_dataframe(metrics_df),
                "all_corr_matrix": serialize_dataframe(corr_matrix),
                "avg_abs_alpha": avg_abs_alpha,
            }

    def execute(self) -> None:
        self.log(f"Worker started for analysis: {self.fl_id}/{self.analysis_id}")

        self.analysis = self.service.get(self.fl_id, self.analysis_id)
        if self.analysis is None:
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
                ),
                avg_abs_alpha=results["avg_abs_alpha"],
            )
            self.log("Analysis completed successfully")

        except Exception as e:
            error_msg = f"{str(e)}\n\n{traceback.format_exc()}"
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
