import os
from datetime import datetime
from typing import Optional

import streamlit as st

from src.core.context import get_state, update_state, add_debug_log
from src.core.types import AnalysisParams
from src.core.utils import get_local_storage, serialize_dataframe
from src.core.validation import validate_inputs
from src.services.p123_client import fetch_benchmark_data
from src.core.calculations import get_dataset_date_range
from src.core.constants import DEFAULT_BENCHMARK
from src.workers.manager import start_analysis_job, get_job_results, delete_job
from src.services.readers import ParquetDataReader


def process_step1() -> bool:
    state = get_state()

    is_valid, error_msg = validate_inputs()
    if not is_valid:
        st.session_state["step1_error"] = error_msg
        return False

    st.session_state["step1_error"] = None

    benchmark_ticker = (
        st.session_state.get("benchmark_ticker", DEFAULT_BENCHMARK).strip()
        or DEFAULT_BENCHMARK
    )
    api_id = str(st.session_state.get("api_id", "")) or None
    api_key = st.session_state.get("api_key", "").strip()
    min_alpha = st.session_state.get("min_alpha", 0.5)
    top_x_pct = st.session_state.get("top_x_pct", 20)
    bottom_x_pct = st.session_state.get("bottom_x_pct", 20)

    add_debug_log(f"Processing Step 1: Benchmark={benchmark_ticker}")
    add_debug_log(
        f"Analysis Parameters - Min Alpha: {min_alpha}%, Top X: {top_x_pct}%, Bottom X: {bottom_x_pct}%"
    )

    try:
        dataset_file = state.dataset_path
        add_debug_log(f"Dataset file: {dataset_file}")

        dataset_reader = ParquetDataReader(dataset_file)

        is_valid, validation_error = dataset_reader.validate()
        if not is_valid:
            st.session_state["step1_error"] = f"Invalid dataset: {validation_error}"
            return False

        formulas_data = dataset_reader.get_formulas_df()
        if formulas_data is None:
            st.session_state["step1_error"] = (
                "Parquet file missing 'features' metadata with formula definitions"
            )
            return False
        add_debug_log(f"Formulas loaded: {len(formulas_data)} formulas")

        add_debug_log("Getting date range from dataset...")

        date_df = dataset_reader.read_columns(["Date"])
        try:
            start_date, end_date = get_dataset_date_range(date_df)
            add_debug_log(f"Date range: {start_date} to {end_date}")
        except ValueError as e:
            st.session_state["step1_error"] = f"Error getting date range: {str(e)}"
            return False

        add_debug_log(f"Fetching benchmark data for {benchmark_ticker}...")
        benchmark_data, error = fetch_benchmark_data(
            benchmark_ticker, api_key, start_date, end_date, api_id
        )

        if error:
            add_debug_log(f"Benchmark fetch failed: {error}")
            st.session_state["step1_error"] = f"Error fetching benchmark data: {error}"
            return False

        add_debug_log("Benchmark data fetched successfully")

        update_state(
            dataset_path=dataset_file,
            formulas_data=formulas_data,
            benchmark_data=benchmark_data,
            benchmark_ticker=benchmark_ticker,
            api_id=api_id,
            api_key=api_key,
            min_alpha=min_alpha,
            top_x_pct=float(top_x_pct),
            bottom_x_pct=float(bottom_x_pct),
        )

        state.completed_steps.add(1)
        state.current_step = 2

        add_debug_log("Step 1 complete - Proceeding to Step 2")
        return True

    except Exception as e:
        add_debug_log(f"ERROR: {str(e)}")
        st.session_state["step1_error"] = f"Error processing data: {str(e)}"
        return False


def start_step2_analysis() -> str:
    state = get_state()

    fl_id = state.factor_list_uid

    dataset_ts = None
    if state.dataset_path and os.path.exists(state.dataset_path):
        try:
            ts = os.path.getmtime(state.dataset_path)
            dataset_ts = str(int(ts))
        except Exception:
            pass

    job_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    # fl_id/dataset_ts/job_ts
    job_id = f"{fl_id}/{dataset_ts}/{job_ts}"

    try:
        params = AnalysisParams(
            top_pct=state.top_x_pct,
            bottom_pct=state.bottom_x_pct,
            min_alpha=state.min_alpha,
            benchmark_data=serialize_dataframe(state.benchmark_data),
            benchmark_ticker=state.benchmark_ticker,
            dataset_path=state.dataset_path,
        )
        start_analysis_job(job_id, params.model_dump())
        update_state(current_job_id=job_id)
        return job_id, None
    except Exception as e:
        return None, f"Error starting analysis: {str(e)}"


def _merge_worker_logs(job_data: dict) -> None:
    """Merge worker logs from job into main debug logs."""
    worker_logs = job_data.get("logs", [])
    for log_entry in worker_logs:
        add_debug_log(log_entry, without_timestamp=True)


def process_step2_completion(job_data: dict) -> Optional[str]:
    state = get_state()

    _merge_worker_logs(job_data)

    try:
        metrics_df, corr_matrix = get_job_results(job_data)

        state.completed_steps.add(2)
        state.completed_steps.add(3)
        update_state(
            all_metrics=metrics_df,
            all_corr_matrix=corr_matrix,
            current_step=3,
        )
        return None
    except Exception as e:
        delete_job(state.current_job_id)
        update_state(current_job_id=None)
        return f"Error loading results: {str(e)}"
