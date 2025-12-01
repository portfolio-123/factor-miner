import streamlit as st
import pandas as pd
import subprocess
import sys
import time
from pathlib import Path

from src.core.context import get_state, update_state
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

# Project root for subprocess cwd
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


def _set_error(message: str) -> None:
    st.session_state['step2_error'] = message


def _start_background_job() -> str:
    """
    Create a background job and spawn the worker process.
    Returns the job ID
    """
    state = get_state()

    # Use factor_list_uid as job ID so we can find it after page refresh
    job_id = state.factor_list_uid
    if not job_id:
        raise ValueError("No factor_list_uid available - cannot create job")

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
        'benchmark_ticker': state.benchmark_ticker,
        'formulas_data': serialize_dataframe(state.formulas_data) if state.formulas_data is not None else None,
    }

    # Create job file
    create_job(job_id, params)

    # Spawn worker process (detached so it survives browser refresh)
    subprocess.Popen(
        [sys.executable, '-m', 'src.jobs.worker', job_id],
        cwd=str(PROJECT_ROOT),
        start_new_session=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

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

        state.completed_steps.add(2)
        state.completed_steps.add(3)
        update_state(current_step=3)

        return True

    except Exception as e:
        _set_error(f"Error loading results: {str(e)}")
        return False


def _render_analysis_progress(job_data: dict) -> None:
    """Render full-screen analysis progress view."""
    progress = job_data.get('progress', {})
    completed = progress.get('completed', 0)
    total = progress.get('total', 0)
    current_factor = progress.get('current_factor', '')

    # Full-screen centered progress view
    st.markdown("""
        <style>
        .progress-container {
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
            min-height: 70vh;
            text-align: center;
            padding: 40px;
        }
        .progress-title {
            font-size: 28px;
            font-weight: 600;
            color: #333;
            margin-bottom: 40px;
        }
        .progress-spinner {
            width: 64px;
            height: 64px;
            border: 4px solid #f3f3f3;
            border-top: 4px solid #2196F3;
            border-radius: 50%;
            animation: spin 1s linear infinite;
            margin-bottom: 40px;
        }
        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }
        .progress-factor {
            font-size: 18px;
            color: #666;
            margin-bottom: 20px;
            max-width: 500px;
            word-wrap: break-word;
        }
        .progress-factor-name {
            font-weight: 600;
            color: #2196F3;
        }
        .progress-count {
            font-size: 42px;
            font-weight: 700;
            color: #333;
        }
        </style>
    """, unsafe_allow_html=True)

    if total > 0:
        factor_display = f'Analyzing: <span class="progress-factor-name">{current_factor}</span>' if current_factor else 'Processing...'
        st.markdown(f"""
            <div class="progress-container">
                <div class="progress-title">Running Factor Analysis</div>
                <div class="progress-spinner"></div>
                <div class="progress-factor">{factor_display}</div>
                <div class="progress-count">{completed} / {total}</div>
            </div>
        """, unsafe_allow_html=True)
    else:
        st.markdown("""
            <div class="progress-container">
                <div class="progress-title">Starting Analysis</div>
                <div class="progress-spinner"></div>
                <div class="progress-factor">Initializing worker process...</div>
            </div>
        """, unsafe_allow_html=True)


def render() -> None:
    state = get_state()
    # Explicit placeholders so we can clear previously rendered UI
    progress_slot = st.empty()
    content_slot = st.empty()

    # Check if there's an active job FIRST - if so, show progress screen instead
    job_id = state.current_job_id

    if job_id:
        # Poll for job status
        job_data = _check_job_status(job_id)

        if job_data is None:
            # Job file not found - maybe it was cleaned up
            update_state(current_job_id=None)
            st.rerun()

        elif job_data['status'] == 'completed':
            # Job finished - load results (keep job file for refresh persistence)
            if _load_job_results(job_data):
                update_state(current_job_id=None)
                st.rerun()
            else:
                # Error loading results - clean up in this case
                delete_job(job_id)
                update_state(current_job_id=None)
                st.rerun()

        elif job_data['status'] == 'error':
            # Job failed
            error_msg = job_data.get('error', 'Unknown error')
            _set_error(f"Analysis failed: {error_msg.split(chr(10))[0]}")
            delete_job(job_id)
            update_state(current_job_id=None)
            st.rerun()

        else:
            # Render full-screen progress view ONLY - nothing else
            content_slot.empty()  # ensure previous tables/buttons are removed
            with progress_slot.container():
                _render_analysis_progress(job_data)

            # Poll every 1 seconds then rerun
            time.sleep(1)
            st.rerun()

    # Normal review content when no job is running
    # load preview data on demand (works after browser refresh)
    progress_slot.empty()
    with content_slot.container():
        reader = get_data_reader(state.dataset_path, file_type=state.file_type)
        preview_df = reader.read_preview(num_rows=10)
        metadata = reader.get_metadata()
        actual_row_count = metadata.get('num_rows', len(preview_df) if preview_df is not None else 0)
        unique_dates = metadata.get('unique_dates')

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
                    _set_error(f"Error starting analysis: {str(e)}")
