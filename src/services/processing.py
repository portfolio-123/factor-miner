import os
from datetime import datetime
from typing import Optional

import streamlit as st

from src.core.context import get_state, update_state, add_debug_log
from src.core.types import AnalysisParams
from src.core.calculations import get_dataset_date_range
from src.core.constants import DEFAULT_BENCHMARK
from src.workers.manager import start_analysis_job, get_job_results, delete_job
from src.services.readers import ParquetDataReader


def process_config() -> bool:
    state = get_state()

    # sync form values to global state
    update_state(
        benchmark_ticker=st.session_state.get(
            "benchmark_ticker", DEFAULT_BENCHMARK
        ).strip(),
        min_alpha=st.session_state.get("min_alpha", 0.5),
        top_x_pct=st.session_state.get("top_x_pct", 20),
        bottom_x_pct=st.session_state.get("bottom_x_pct", 20),
        config_error=None,
    )

    add_debug_log(
        f"Config: Min Alpha: {state.min_alpha}%, Top X: {state.top_x_pct}%, Bottom X: {state.bottom_x_pct}%"
    )

    try:
        dataset_reader = ParquetDataReader(state.dataset_path)

        formulas_data = dataset_reader.get_formulas_df()
        add_debug_log(f"Formulas loaded: {len(formulas_data)} formulas")

        # Validate date range exists (benchmark will be fetched by worker)
        date_df = dataset_reader.read_columns(["Date"])
        try:
            start_date, end_date = get_dataset_date_range(date_df)
            add_debug_log(f"Date range: {start_date} to {end_date}")
        except ValueError as e:
            update_state(config_error=f"Error getting date range: {str(e)}")
            return False

        update_state(
            formulas_data=formulas_data,
            config_completed=True,
            current_step=2,
        )

        add_debug_log("Settings complete - Proceeding to analysis")
        return True

    except Exception as e:
        add_debug_log(f"ERROR: {str(e)}")
        update_state(config_error=f"Error processing data: {str(e)}")
        return False


def start_step2_analysis() -> None:
    state = get_state()
    update_state(analysis_error=None)

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
            benchmark_ticker=state.benchmark_ticker,
            dataset_path=state.dataset_path,
            api_key=state.user_payload.get("apiKey") if state.user_payload else None,
            api_id=state.user_payload.get("apiId") if state.user_payload else None,
        )
        start_analysis_job(job_id, params.model_dump())
        update_state(current_job_id=job_id)
    except Exception as e:
        update_state(analysis_error=f"Error starting analysis: {str(e)}")


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

        update_state(
            all_metrics=metrics_df,
            all_corr_matrix=corr_matrix,
            page="results",
        )
        return None
    except Exception as e:
        delete_job(state.current_job_id)
        update_state(current_job_id=None)
        return f"Error loading results: {str(e)}"
