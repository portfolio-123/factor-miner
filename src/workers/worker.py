import sys
import traceback
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
from src.core.constants import PRICE_COLUMN
from src.core.constants import REQUIRED_COLUMNS
from src.core.constants import AnalysisStatus
from src.core.types import AnalysisParams

from src.core.utils import serialize_dataframe
from src.workers.manager import read_analysis, update_analysis, append_analysis_log, clear_analysis_credentials
from src.services.readers import ParquetDataReader
from src.services.p123_client import fetch_benchmark_data
from src.core.calculations import (
    get_dataset_date_range,
    calculate_benchmark_returns,
    calculate_future_performance,
    analyze_factors,
    calculate_factor_metrics,
    calculate_correlation_matrix,
)

_analysis_id: str | None = None


def log(message: str) -> None:
    timestamp = datetime.now().strftime('%H:%M:%S')
    formatted = f"[{timestamp}] [WORKER] {message}"

    if _analysis_id:
        append_analysis_log(_analysis_id, formatted)


def run_analysis(analysis_id: str, params: AnalysisParams) -> dict:
    log("Starting analysis...")

    log(f"Processing dataset: {params.dataset_path}")

    reader = ParquetDataReader(params.dataset_path)

    date_df = reader.read_columns(["Date"])
    start_date, end_date = get_dataset_date_range(date_df)

    log(f"Fetching benchmark data for {params.benchmark_ticker}...")
    try:
        benchmark_data = fetch_benchmark_data(
            benchmark_ticker=params.benchmark_ticker,
            access_token=params.access_token,
            start_date=start_date,
            end_date=end_date,
        )
    finally:
        clear_analysis_credentials(analysis_id)

    log("Benchmark data fetched successfully")

    # Progress callback to update analysis file
    def on_progress(completed: int, total: int, current_factor: str = "") -> None:
        log(f"Progress: {completed}/{total} factors - {current_factor}")
        update_analysis(analysis_id, status=AnalysisStatus.RUNNING, progress={
            "completed": completed,
            "total": total,
            "current_factor": current_factor
        })

    columns = reader._parquet_file.schema_arrow.names
    excluded_columns = REQUIRED_COLUMNS + ['benchmark', 'Future Perf']
    factor_columns = [col for col in columns if col not in excluded_columns]

    update_analysis(analysis_id, status=AnalysisStatus.RUNNING, progress={
        "completed": 0,
        "total": len(factor_columns),
        "current_factor": ""
    })

    log("Calculating future performance...")

    perf_core = reader.read_columns(['Date', 'Ticker', PRICE_COLUMN])
    future_perf_df = calculate_future_performance(perf_core, PRICE_COLUMN)

    log("Analyzing factors...")
    results_df = analyze_factors(
        future_perf_df,
        reader,
        factor_columns=factor_columns,
        top_pct=params.top_pct,
        bottom_pct=params.bottom_pct,
        progress_fn=on_progress
    )
    raw_data = reader.read_columns(['Date'])

    log("Calculating benchmark returns...")
    raw_data = calculate_benchmark_returns(raw_data, benchmark_data)

    if results_df.empty:
        raise ValueError("No results from factor analysis")

    log("Calculating factor metrics...")
    metrics_df = calculate_factor_metrics(results_df, raw_data)

    log("Calculating correlation matrix...")
    corr_matrix = calculate_correlation_matrix(results_df)

    log("Analysis complete!")

    return {
        'all_metrics': serialize_dataframe(metrics_df),
        'all_corr_matrix': serialize_dataframe(corr_matrix),
    }


def main():
    global _analysis_id
    _analysis_id = sys.argv[1]

    log(f"Worker started for analysis: {_analysis_id}")

    try:
        analysis_data = read_analysis(_analysis_id)
        if analysis_data is None:
            log(f"Analysis {_analysis_id} not found")
            sys.exit(1)

        update_analysis(_analysis_id, status=AnalysisStatus.RUNNING)
        log("Status updated to running")

        params = AnalysisParams(**analysis_data['params'])
        results = run_analysis(_analysis_id, params)

        update_analysis(_analysis_id, status=AnalysisStatus.COMPLETED, results=results)
        log("Analysis completed successfully")

    except Exception as e:
        error_msg = f"{str(e)}\n\n{traceback.format_exc()}"
        log(f"Error: {error_msg}")
        update_analysis(_analysis_id, status=AnalysisStatus.ERROR, error=error_msg)
        sys.exit(1)


if __name__ == "__main__":
    main()


