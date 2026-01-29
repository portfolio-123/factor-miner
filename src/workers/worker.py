import sys
import traceback
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv

load_dotenv()

from src.core.constants import PRICE_COLUMN, REQUIRED_COLUMNS
from src.core.environment import FACTOR_LIST_DIR, FACTORMINER_DIR
from src.core.types import Analysis, AnalysisStatus, AnalysisProgress, AnalysisResults
from src.core.utils import serialize_dataframe
from src.workers.analysis_service import AnalysisService
from src.services.readers import ParquetDataReader
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
        self.service = AnalysisService(FACTORMINER_DIR)
        self.analysis: Analysis | None = None

    def log(self, message: str) -> None:
        timestamp = datetime.now().strftime("%H:%M:%S")
        formatted = f"[{timestamp}] [WORKER] {message}"

        if self.analysis:
            self.analysis = self.service.append_log(self.analysis, formatted)

    def update(self, **updates) -> None:
        if self.analysis:
            self.analysis = self.service.save(self.analysis, **updates)

    def run(self) -> dict:
        self.log("Starting analysis...")

        params = self.analysis.params
        dataset_path = str(FACTOR_LIST_DIR / self.fl_id)
        self.log(f"Processing dataset: {dataset_path}")

        reader = ParquetDataReader(dataset_path)

        dataset_info = reader.get_dataset_info()
        start_dt = pd.to_datetime(dataset_info.startDt) - pd.Timedelta(days=7)

        self.log(f"Fetching benchmark data for {params.benchmark_ticker}...")
        try:
            benchmark_data = fetch_benchmark_data(
                benchmark_ticker=params.benchmark_ticker,
                access_token=params.access_token,
                start_date=start_dt.strftime("%Y-%m-%d"),
                end_date=dataset_info.endDt,
            )
        finally:
            self.analysis = self.service.clear_credentials(self.analysis)

        self.log("Benchmark data fetched successfully")

        def on_progress(completed: int, total: int, current_factor: str = "") -> None:
            self.log(f"Progress: {completed}/{total} factors - {current_factor}")
            self.update(
                status=AnalysisStatus.RUNNING,
                progress=AnalysisProgress(
                    completed=completed,
                    total=total,
                    current_factor=current_factor,
                ),
            )

        factor_columns = [
            col for col in reader.column_names
            if col not in REQUIRED_COLUMNS + ["benchmark", "Future Perf"]
        ]

        self.update(
            status=AnalysisStatus.RUNNING,
            progress=AnalysisProgress(completed=0, total=len(factor_columns)),
        )

        self.log("Calculating future performance...")

        perf_core = reader.read_columns(["Date", "Ticker", PRICE_COLUMN])
        future_perf_df = calculate_future_performance(perf_core, PRICE_COLUMN)

        self.log("Analyzing factors...")
        results_df = analyze_factors(
            future_perf_df,
            reader,
            factor_columns=factor_columns,
            top_pct=params.top_pct,
            bottom_pct=params.bottom_pct,
            progress_fn=on_progress,
        )
        raw_data = reader.read_columns(["Date"])

        self.log("Calculating benchmark returns...")
        raw_data = calculate_benchmark_returns(raw_data, benchmark_data)

        if results_df.empty:
            raise ValueError("No results from factor analysis")

        self.log("Calculating factor metrics...")
        metrics_df = calculate_factor_metrics(
            results_df, raw_data, periods_per_year=dataset_info.frequency.periods_per_year
        )

        self.log("Calculating correlation matrix...")
        corr_matrix = calculate_correlation_matrix(results_df)

        # Round to 4 decimals for storage efficiency
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
