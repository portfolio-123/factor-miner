"""
Worker script for running factor analysis in background.
Run as: python -m src.jobs.worker <job_id>
"""
import sys
import traceback
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
from src.core.context import PRICE_COLUMN
from src.core.utils import locate_factor_list_files, deserialize_dataframe, serialize_dataframe
from src.jobs.manager import read_job, update_job
from src.data.readers import get_data_reader
from src.logic.calculations import (
    calculate_benchmark_returns,
    calculate_future_performance,
    analyze_factors,
    calculate_factor_metrics,
    calculate_correlation_matrix,
)


def log(message: str) -> None:
    timestamp = datetime.now().strftime('%H:%M:%S')
    formatted = f"[{timestamp}] [WORKER] {message}"
    print(formatted, flush=True)


def run_analysis(job_id: str, params: dict) -> dict:
    log("Starting analysis...")

    # internal mode: locate by job_id (factor_list_uid)
    # exteral mode: use dataset_path/file_type provided in params
    dataset_path = params.get('dataset_path')
    file_type = params.get('file_type')

    if dataset_path:
        # external mode
        from pathlib import Path as _Path
        dataset_path = _Path(dataset_path)
        if file_type is None:
            # detect if not provided
            from src.core.utils import detect_file_type as _detect_file_type
            file_type = _detect_file_type(dataset_path)
        if not dataset_path.exists():
            raise ValueError(f"Dataset file not found: {dataset_path}")
    else:
        # internal mode
        dataset_path, _, error, file_type = locate_factor_list_files(job_id)
        if error or not dataset_path:
            raise ValueError(f"Could not locate dataset: {error}")

    top_pct = params['top_pct']
    bottom_pct = params['bottom_pct']

    benchmark_data = deserialize_dataframe(params['benchmark_data'])

    log(f"Processing dataset: {dataset_path}")

    # Progress callback to update job file
    def on_progress(completed: int, total: int, current_factor: str = "") -> None:
        log(f"Progress: {completed}/{total} factors - {current_factor}")
        update_job(job_id, status="running", progress={
            "completed": completed,
            "total": total,
            "current_factor": current_factor
        })

    if file_type == 'parquet':
        reader = get_data_reader(dataset_path, file_type=file_type)
        perf_data = reader.read_columns(['Date', 'Ticker', PRICE_COLUMN])
        columns = reader.get_column_names()
        excluded_columns = ['Date', 'Ticker', 'P123 ID', PRICE_COLUMN]
        factor_columns = [col for col in columns if col not in excluded_columns]
    else:
        reader = get_data_reader(dataset_path, file_type=file_type)
        perf_data = reader.read_full()
        factor_columns = None

    log("Calculating future performance...")
    future_perf_df = calculate_future_performance(perf_data, PRICE_COLUMN)

    log("Analyzing factors...")
    if file_type == 'parquet':
        results_df = analyze_factors(
            None,
            future_perf_df,
            parquet_path=dataset_path,
            factor_columns=factor_columns,
            top_pct=top_pct,
            bottom_pct=bottom_pct,
            progress_fn=on_progress
        )
        # Read Date/Ticker for benchmark calculation
        date_ticker_df = reader.read_columns(['Date', 'Ticker'])
        raw_data = date_ticker_df
    else:
        results_df = analyze_factors(
            perf_data,
            future_perf_df,
            top_pct=top_pct,
            bottom_pct=bottom_pct,
            progress_fn=on_progress
        )
        raw_data = perf_data

    log("Calculating benchmark returns...")
    raw_data, _ = calculate_benchmark_returns(raw_data, benchmark_data)

    if results_df.empty:
        raise ValueError("No results from factor analysis")

    log("Calculating factor metrics...")
    metrics_df = calculate_factor_metrics(results_df, raw_data)

    log("Calculating correlation matrix...")
    corr_matrix = calculate_correlation_matrix(results_df)

    log("Analysis complete!")

    # Serialize results for JSON storage
    return {
        'all_metrics': serialize_dataframe(metrics_df),
        'all_corr_matrix': serialize_dataframe(corr_matrix),
    }


def main():
    job_id = sys.argv[1]

    log(f"Worker started for job: {job_id}")

    try:
        # Read job data
        job_data = read_job(job_id)
        if job_data is None:
            log(f"Job {job_id} not found")
            sys.exit(1)

        # Update status to running
        update_job(job_id, status="running")
        log("Status updated to running")

        # Run the analysis
        params = job_data['params']
        results = run_analysis(job_id, params)

        # Update job with results
        update_job(job_id, status="completed", results=results)
        log("Job completed successfully")

    except Exception as e:
        error_msg = f"{str(e)}\n\n{traceback.format_exc()}"
        log(f"Error: {error_msg}")
        update_job(job_id, status="error", error=error_msg)
        sys.exit(1)


if __name__ == "__main__":
    main()
