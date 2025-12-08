import streamlit as st
import pandas as pd
import time

from src.core.context import get_state, update_state
from src.core.utils import format_date
from src.ui.components import (
    section_header,
    render_formulas_grid,
    render_dataset_preview
)
from src.services.readers import ParquetDataReader
from src.services.processing import start_step2_analysis, process_step2_completion
from src.workers.manager import read_job, delete_job
from src.core.constants import JobStatus


def _on_run_analysis() -> None:
    st.session_state['step2_error'] = None
    _, error = start_step2_analysis()
    if error:
        st.session_state['step2_error'] = error
    else:
        st.rerun()


def _on_job_completed(job_data: dict) -> None:
    error = process_step2_completion(job_data)
    if error:
        st.session_state['step2_error'] = error
    st.rerun()


def _on_job_error(job_id: str, error_msg: str) -> None:
    display_msg = error_msg.split('\n')[0] if error_msg else 'Unknown error'
    st.session_state['step2_error'] = f"Analysis failed: {display_msg}"
    delete_job(job_id)
    update_state(current_job_id=None)
    st.rerun()


def _render_job_progress(job_data: dict) -> None:
    progress = job_data.get('progress', {})
    completed = progress.get('completed', 0)
    total = progress.get('total', 0)
    current_factor = progress.get('current_factor', '')

    _, center_col, _ = st.columns([1, 2, 1])

    with center_col:
        st.space(100)
        st.subheader("Running Factor Analysis")

        if total > 0:
            st.progress(completed / total, text=f"{completed} / {total} factors analyzed")
        else:
            st.progress(0, text="Initializing...")

        if current_factor:
            st.info(f"Analyzing: **{current_factor}**")
        else:
            st.info("Starting worker process...")

    # show updates every .5 seconds
    time.sleep(0.5)
    st.rerun()


def _render_dataset_statistics(stats: dict, benchmark: str) -> None:
    section_header("Dataset Statistics")

    cols = st.columns([1, 1, 1, 2, 1], gap="small")
    stat_style = "margin-top: -10px; font-size: 1.25rem; font-weight: 600;"

    stat_items = [
        ("Rows", stats['num_rows']),
        ("Dates", stats['num_dates']),
        ("Columns", stats['num_columns']),
        ("Period", f"{stats['min_date']} - {stats['max_date']}"),
        ("Benchmark", benchmark or "N/A"),
    ]

    for col, (label, value) in zip(cols, stat_items):
        with col:
            st.badge(label)
            st.markdown(f"<p style='{stat_style}'>{value}</p>", unsafe_allow_html=True)


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
    _render_dataset_statistics(stats, state.benchmark_ticker)

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
        if st.button("Run Analysis", type="primary", use_container_width=True):
            _on_run_analysis()

def render() -> None:
    state = get_state()
    job_id = state.current_job_id

    content_placeholder = st.empty()
    progress_placeholder = st.empty()

    # No active job - show review content
    if not job_id:
        progress_placeholder.empty()
        with content_placeholder.container():
            _render_review_content()
        return

    job_data = read_job(job_id)

    if job_data is None:
        update_state(current_job_id=None)
        st.rerun()

    status = job_data['status']

    if status == JobStatus.COMPLETED:
        _on_job_completed(job_data)

    elif status == JobStatus.ERROR:
        _on_job_error(job_id, job_data.get('error', ''))

    else:
        # job running - clear review content and show only progress UI
        content_placeholder.empty()
        with progress_placeholder.container():
            _render_job_progress(job_data)
