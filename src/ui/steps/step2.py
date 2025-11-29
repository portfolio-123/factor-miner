import streamlit as st
import pandas as pd
import subprocess
import sys
import time
import uuid
from pathlib import Path

from src.core.context import get_state, update_state, add_debug_log
from src.core.utils import format_date
from src.ui.components import (
    section_header,
    render_formulas_grid,
    render_dataset_preview
)
from src.data.readers import get_data_reader
from src.jobs.manager import (
    create_job,
    read_job,
    delete_job,
    serialize_dataframe,
    deserialize_dataframe
)


def _set_error(message: str) -> None:
    st.session_state['step2_error'] = message


def _start_background_job() -> str:
    """
    Create a background job and spawn the worker process.
    Returns the job ID.
    """
    state = get_state()

    # Generate unique job ID
    job_id = uuid.uuid4().hex

    # Collect job parameters
    params = {
        'dataset_path': str(state.dataset_path),
        'file_type': state.file_type,
        'price_column': state.PRICE_COLUMN,
        'top_pct': state.top_x_pct,
        'bottom_pct': state.bottom_x_pct,
        'n_features': state.n_features,
        'correlation_threshold': state.correlation_threshold,
        'min_alpha': state.min_alpha,
        'benchmark_data': serialize_dataframe(state.benchmark_data),
    }

    # Create job file
    create_job(job_id, params)
    add_debug_log(f"Created background job: {job_id}")

    # Get project root for running the worker
    project_root = Path(__file__).parent.parent.parent.parent

    # Spawn worker process (detached so it survives browser refresh)
    subprocess.Popen(
        [sys.executable, '-m', 'src.jobs.worker', job_id],
        cwd=str(project_root),
        start_new_session=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    add_debug_log(f"Spawned worker process for job: {job_id}")

    return job_id


def _check_job_status(job_id: str) -> dict:
    """
    Check the status of a background job.
    Returns the job data dict or None if not found.
    """
    return read_job(job_id)


def _load_job_results(job_data: dict) -> bool:
    """
    Load completed job results into the app state.
    Returns True on success, False on failure.
    """
    state = get_state()

    try:
        results = job_data['results']

        # Deserialize DataFrames
        metrics_df = deserialize_dataframe(results['all_metrics'])
        corr_matrix = deserialize_dataframe(results['all_corr_matrix'])
        raw_data = deserialize_dataframe(results['raw_data'])
        best_features = results['best_features']

        # Update state with results
        update_state(
            all_metrics=metrics_df,
            all_corr_matrix=corr_matrix,
            raw_data=raw_data
        )

        add_debug_log(f"Analysis complete! Found {len(best_features)} best features")

        state.completed_steps.add(2)
        state.completed_steps.add(3)
        state.current_step = 3

        return True

    except Exception as e:
        add_debug_log(f"Error loading results: {str(e)}")
        _set_error(f"Error loading results: {str(e)}")
        return False


def render() -> None:
    state = get_state()

    # load preview data if needed
    if state.file_type == 'parquet':
        reader = get_data_reader(state.dataset_path, file_type=state.file_type)
        preview_df = reader.read_preview(num_rows=10)
        metadata = reader.get_metadata()
        actual_row_count = metadata.get('num_rows', len(preview_df))
        unique_dates = metadata.get('unique_dates')
    else:
        preview_df = state.raw_data
        actual_row_count = len(preview_df) if preview_df is not None else 0
        unique_dates = None

    if preview_df is None or preview_df.empty:
        st.error("No data available for preview")
        return

    # Calculate statistics
    num_rows = actual_row_count
    num_columns = len(preview_df.columns)
    dates = pd.to_datetime(preview_df['Date'])
    num_unique_dates = unique_dates if unique_dates is not None else dates.nunique()
    min_date = format_date(dates.min())
    max_date = format_date(dates.max())

    section_header("Dataset Statistics")

    cols = st.columns([1,1,1,2,1], gap="small")
    stat_style = "margin-top: -10px; font-size: 1.25rem; font-weight: 600;"
    stats = [
        ("Rows", num_rows),
        ("Dates", num_unique_dates),
        ("Columns", num_columns),
        ("Period", f"{min_date} - {max_date}"),
        ("Benchmark", state.benchmark_ticker or "N/A"),
    ]
    for col, (label, value) in zip(cols, stats):
        with col:
            st.badge(label)
            st.markdown(f"<p style='{stat_style}'>{value}</p>", unsafe_allow_html=True)

    tab1, tab2 = st.tabs(["Formulas", "Dataset Preview"])

    with tab1:
        if state.formulas_data is not None:
            render_formulas_grid(state.formulas_data)
        else:
            st.info("No formulas data available")

    with tab2:
        render_dataset_preview(preview_df, actual_row_count)

    # display error message above button (if any)
    if st.session_state.get('step2_error'):
        st.error(st.session_state['step2_error'])

    # Check if there's an active job
    job_id = state.current_job_id

    if job_id:
        # Poll for job status
        job_data = _check_job_status(job_id)

        if job_data is None:
            # Job file not found - maybe it was cleaned up
            add_debug_log(f"Job {job_id} not found, resetting state")
            update_state(current_job_id=None)
            st.rerun()

        elif job_data['status'] == 'completed':
            # Job finished - load results
            add_debug_log(f"Job {job_id} completed, loading results")
            if _load_job_results(job_data):
                # Clean up job file
                delete_job(job_id)
                update_state(current_job_id=None)
                st.rerun()
            else:
                # Error loading results
                delete_job(job_id)
                update_state(current_job_id=None)

        elif job_data['status'] == 'error':
            # Job failed
            error_msg = job_data.get('error', 'Unknown error')
            add_debug_log(f"Job {job_id} failed: {error_msg}")
            _set_error(f"Analysis failed: {error_msg.split(chr(10))[0]}")
            delete_job(job_id)
            update_state(current_job_id=None)

        else:
            # Job still running (pending or running) - show spinner and poll
            _, _, col3 = st.columns([2, 1, 1])
            with col3:
                st.markdown('''
                <div class="spinner-button">
                    <div class="spinner"></div>
                    <span>Analyzing</span>
                </div>
                ''', unsafe_allow_html=True)

            # Poll every 2 seconds
            time.sleep(2)
            st.rerun()

    else:
        # No active job - show the Run Analysis button
        _, _, col3 = st.columns([2, 1, 1])
        with col3:
            if st.button("Run Analysis", type="primary", use_container_width=True):
                st.session_state['step2_error'] = None
                try:
                    job_id = _start_background_job()
                    update_state(current_job_id=job_id)
                    st.rerun()
                except Exception as e:
                    add_debug_log(f"Error starting job: {str(e)}")
                    _set_error(f"Error starting analysis: {str(e)}")
