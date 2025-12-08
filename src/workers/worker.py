"""
Worker script for running factor analysis in background.
Run as: python -m src.workers.worker <job_id>
"""
import sys
import traceback
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
from src.core.constants import PRICE_COLUMN
from src.core.constants import REQUIRED_COLUMNS
from src.core.constants import JobStatus

from src.core.utils import deserialize_dataframe, serialize_dataframe
from src.workers.manager import read_job, update_job, append_job_log
from src.services.readers import ParquetDataReader
from src.core.calculations import (
    calculate_benchmark_returns,
    calculate_future_performance,
    analyze_factors,
    calculate_factor_metrics,
    calculate_correlation_matrix,
)

_job_id: str | None = None


def log(message: str) -> None:
    timestamp = datetime.now().strftime('%H:%M:%S')
    formatted = f"[{timestamp}] [WORKER] {message}"

    if _job_id:
        append_job_log(_job_id, formatted)


def run_analysis(job_id: str, params: dict) -> dict:
    log("Starting analysis...")

    dataset_path = params['dataset_path']  # Already resolved before job was started

    top_pct = params['top_pct']
    bottom_pct = params['bottom_pct']

    benchmark_data = deserialize_dataframe(params['benchmark_data'])

    log(f"Processing dataset: {dataset_path}")

    # Progress callback to update job file
    def on_progress(completed: int, total: int, current_factor: str = "") -> None:
        log(f"Progress: {completed}/{total} factors - {current_factor}")
        update_job(job_id, status=JobStatus.RUNNING, progress={
            "completed": completed,
            "total": total,
            "current_factor": current_factor
        })

    reader = ParquetDataReader(dataset_path)

    columns = reader.get_column_names()
    excluded_columns = REQUIRED_COLUMNS + ['benchmark', 'Future Perf']
    factor_columns = [col for col in columns if col not in excluded_columns]

    update_job(job_id, status=JobStatus.RUNNING, progress={
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
        reader=reader,
        factor_columns=factor_columns,
        top_pct=top_pct,
        bottom_pct=bottom_pct,
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
    global _job_id
    _job_id = sys.argv[1]

    log(f"Worker started for job: {_job_id}")

    try:
        job_data = read_job(_job_id)
        if job_data is None:
            log(f"Job {_job_id} not found")
            sys.exit(1)

        update_job(_job_id, status=JobStatus.RUNNING)
        log("Status updated to running")

        params = job_data['params']
        results = run_analysis(_job_id, params)

        update_job(_job_id, status=JobStatus.COMPLETED, results=results)
        log("Job completed successfully")

    except Exception as e:
        error_msg = f"{str(e)}\n\n{traceback.format_exc()}"
        log(f"Error: {error_msg}")
        update_job(_job_id, status=JobStatus.ERROR, error=error_msg)
        sys.exit(1)


if __name__ == "__main__":
    main()


