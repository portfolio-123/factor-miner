"""
Standalone worker script for running factor analysis in background.
This runs as a completely independent process from Streamlit.

Usage: python -m src.jobs.worker <job_id>
"""
import sys
import os
import traceback
import pandas as pd
from pathlib import Path

# Add project root to path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Load environment variables
from dotenv import load_dotenv
load_dotenv(project_root / '.env')

from src.jobs.manager import read_job, update_job, serialize_dataframe


def log(message: str) -> None:
    """Simple logging for worker process."""
    print(f"[WORKER] {message}", flush=True)


def run_analysis(params: dict) -> dict:
    """
    Run the factor analysis with the given parameters.
    Returns a dict with serialized results.
    """
    # Import calculation functions here to avoid circular imports
    from src.data.readers import get_data_reader
    from src.logic.calculations import (
        calculate_benchmark_returns,
        calculate_future_performance,
        calculate_factor_metrics,
        calculate_correlation_matrix,
        select_best_features
    )
    # Import analyze_factors_standalone to avoid add_debug_log dependency
    from src.jobs.analyze import analyze_factors_standalone

    log("Starting analysis...")

    dataset_path = params['dataset_path']
    file_type = params['file_type']
    price_column = params['price_column']
    top_pct = params['top_pct']
    bottom_pct = params['bottom_pct']
    n_features = params['n_features']
    correlation_threshold = params['correlation_threshold']
    min_alpha = params['min_alpha']

    # Deserialize benchmark data
    benchmark_data = pd.read_json(params['benchmark_data'], orient='split')

    log(f"Processing dataset: {dataset_path}")

    # Read data based on file type
    if file_type == 'parquet':
        reader = get_data_reader(dataset_path, file_type=file_type)
        perf_data = reader.read_columns(['Date', 'Ticker', price_column])
        columns = reader.get_column_names()
        excluded_columns = ['Date', 'Ticker', 'P123 ID', price_column]
        factor_columns = [col for col in columns if col not in excluded_columns]
    else:
        reader = get_data_reader(dataset_path, file_type=file_type)
        perf_data = reader.read_full()
        factor_columns = None

    log("Calculating future performance...")
    future_perf_df = calculate_future_performance_standalone(perf_data, price_column)

    log("Analyzing factors...")
    if file_type == 'parquet':
        results_df = analyze_factors_standalone(
            None,
            future_perf_df,
            parquet_path=dataset_path,
            factor_columns=factor_columns,
            top_pct=top_pct,
            bottom_pct=bottom_pct,
            log_fn=log
        )
        # Read Date/Ticker for benchmark calculation
        date_ticker_df = reader.read_columns(['Date', 'Ticker'])
        raw_data = date_ticker_df
    else:
        results_df = analyze_factors_standalone(
            perf_data,
            future_perf_df,
            top_pct=top_pct,
            bottom_pct=bottom_pct,
            log_fn=log
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

    log("Selecting best features...")
    best_features = select_best_features(
        metrics_df,
        corr_matrix,
        N=n_features,
        correlation_threshold=correlation_threshold,
        a_min=min_alpha
    )

    log(f"Analysis complete! Found {len(best_features)} best features")

    # Serialize results for JSON storage
    return {
        'all_metrics': serialize_dataframe(metrics_df),
        'all_corr_matrix': serialize_dataframe(corr_matrix),
        'raw_data': serialize_dataframe(raw_data),
        'best_features': best_features
    }


def calculate_future_performance_standalone(
    raw_data: pd.DataFrame,
    price_column: str,
) -> pd.DataFrame:
    """
    Calculate future performance without using add_debug_log.
    This is a copy of the original function for standalone use.
    """
    df = raw_data[['Date', 'Ticker', price_column]].copy()

    df['Date'] = pd.to_datetime(df['Date'])
    df[price_column] = pd.to_numeric(df[price_column], errors='coerce')

    # sort by Ticker and Date
    df = df.sort_values(['Ticker', 'Date']).reset_index(drop=True)

    # shift to get next week's values for each ticker
    df['Next_Date'] = df.groupby('Ticker')['Date'].shift(-1)
    df['Next_Price'] = df.groupby('Ticker')[price_column].shift(-1)

    # calculate return only where conditions are true
    valid_mask = (
        (df[price_column].notna()) &
        (df[price_column] != 0) &
        (df['Next_Price'].notna())
    )

    df['Future Perf'] = float('nan')
    df.loc[valid_mask, 'Future Perf'] = (
        (df.loc[valid_mask, 'Next_Price'] - df.loc[valid_mask, price_column]) /
        df.loc[valid_mask, price_column]
    )

    # clean up temporary columns
    df = df.drop(columns=['Next_Date', 'Next_Price', price_column])

    return df


def main():
    if len(sys.argv) != 2:
        print("Usage: python -m src.jobs.worker <job_id>", file=sys.stderr)
        sys.exit(1)

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
        results = run_analysis(params)

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
