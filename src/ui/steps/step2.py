import streamlit as st
import pandas as pd

from src.core.context import get_state, update_state
from src.core.utils import format_date
from src.ui.components import (
    render_formulas_grid,
    render_dataset_preview,
    render_job_progress,
    render_dataset_statistics
)
from src.services.readers import ParquetDataReader
from src.services.processing import start_step2_analysis, process_step2_completion, _merge_worker_logs
from src.workers.manager import read_job, delete_job


def _on_run_analysis() -> None:
    """Callback for Run Analysis button. Streamlit handles rerun automatically."""
    st.session_state['step2_error'] = None
    _, error = start_step2_analysis()
    if error:
        st.session_state['step2_error'] = error


def _on_job_completed(job_data: dict) -> None:
    error = process_step2_completion(job_data)
    if error:
        st.session_state['step2_error'] = error
    st.rerun()


def _on_job_error(job_id: str, job_data: dict) -> None:
    # Merge worker logs before deleting job
    _merge_worker_logs(job_data)

    error_msg = job_data.get('error', '')
    display_msg = error_msg.split('\n')[0] if error_msg else 'Unknown error'
    st.session_state['step2_error'] = f"Analysis failed: {display_msg}"
    delete_job(job_id)
    update_state(current_job_id=None)
    st.rerun()


def _render_review_content() -> None:
    state = get_state()

    reader = ParquetDataReader(state.dataset_path)
    preview_df = reader.read_preview(num_rows=10)
    metadata = reader.get_metadata()

    if preview_df is None or preview_df.empty:
        st.error("No data available for preview")
        return

    actual_row_count = metadata.get('num_rows', len(preview_df))
    unique_dates = metadata.get('unique_dates')
    dates = pd.to_datetime(preview_df['Date'])
    stats = {
        'num_rows': actual_row_count,
        'num_columns': len(preview_df.columns),
        'num_dates': unique_dates if unique_dates is not None else dates.nunique(),
        'min_date': format_date(dates.min()),
        'max_date': format_date(dates.max()),
    }
    render_dataset_statistics(stats, state.benchmark_ticker)

    tab1, tab2 = st.tabs(["Formulas", "Dataset Preview"])

    with tab1:
        if state.formulas_data is not None:
            render_formulas_grid(state.formulas_data)
        else:
            st.info("No formulas data available")

    with tab2:
        render_dataset_preview(preview_df)

    if st.session_state.get('step2_error'):
        st.error(st.session_state['step2_error'])

    _, _, col3 = st.columns([2, 1, 1])
    with col3:
        st.button("Run Analysis", type="primary", use_container_width=True, on_click=_on_run_analysis)

def render() -> None:
    state = get_state()
    job_id = state.current_job_id

    # No active job - show review content directly (no placeholders)
    if not job_id:
        _render_review_content()
        return

    # Quick check if job exists (handles edge case of deleted job)
    job_data = read_job(job_id)
    if job_data is None:
        update_state(current_job_id=None)
        st.rerun()

    # Job running - fragment handles status checking, callbacks, and auto-polling
    render_job_progress(job_id, _on_job_completed, _on_job_error)
