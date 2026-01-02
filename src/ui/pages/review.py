import streamlit as st
import pandas as pd

from src.core.context import get_state, update_state
from src.core.utils import format_date
from src.ui.components import (
    render_formulas_grid,
    render_dataset_preview,
    render_dataset_statistics
)
from src.workers.manager import read_job, delete_job
from src.core.constants import JobStatus
from src.services.readers import ParquetDataReader
from src.services.processing import start_step2_analysis, process_step2_completion, _merge_worker_logs


def _on_run_analysis() -> None:
    update_state(step2_error=None)
    _, error = start_step2_analysis()
    if error:
        update_state(step2_error=error)


def _on_job_completed(job_data: dict) -> None:
    error = process_step2_completion(job_data)
    if error:
        update_state(step2_error=error)
    st.rerun()


def _on_job_error(job_id: str, job_data: dict) -> None:
    # Merge worker logs before deleting job
    _merge_worker_logs(job_data)

    error_msg = job_data.get('error', '')
    display_msg = error_msg.split('\n')[0] if error_msg else 'Unknown error'
    update_state(step2_error=f"Analysis failed: {display_msg}")
    delete_job(job_id)
    update_state(current_job_id=None)
    st.rerun()


@st.fragment(run_every="0.5s")
def _render_job_progress(job_id: str) -> None:
    job_data = read_job(job_id)

    if job_data is None:
        return

    status = job_data.get('status')

    if status == JobStatus.COMPLETED:
        _on_job_completed(job_data)
        return

    if status == JobStatus.ERROR:
        _on_job_error(job_id, job_data)
        return

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


def _render_review_content() -> None:
    state = get_state()

    reader = ParquetDataReader(state.dataset_path)
    preview_df = reader.read_preview(num_rows=10)
    metadata = reader.get_metadata()

    if preview_df is not None and not preview_df.empty:
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
    else:
        st.error("Unable to load dataset preview and statistics. Showing available metadata only.")

    tab1, tab2 = st.tabs(["Formulas", "Dataset Preview"])

    with tab1:
        if state.formulas_data is not None:
            render_formulas_grid(state.formulas_data)
        else:
            st.info("No formulas data available")

    with tab2:
        if preview_df is not None and not preview_df.empty:
            render_dataset_preview(preview_df)
        else:
            st.warning("Preview unavailable")

    state = get_state()
    if state.step2_error:
        st.error(state.step2_error)

    with st.columns([4, 1])[1]:
        st.button("Run Analysis", type="primary", use_container_width=True, on_click=_on_run_analysis)

def render() -> None:
    state = get_state()
    running_job = state.current_job_id

    # no active job - show review content
    if not running_job:
        _render_review_content()
        return

    _render_job_progress(running_job)
