import streamlit as st
import pandas as pd
import time
import uuid

from src.core.context import get_state, update_state
from src.core.utils import format_date, serialize_dataframe
from src.ui.components import (
    section_header,
    render_formulas_grid,
    render_dataset_preview
)
from src.data.readers import get_data_reader
from src.jobs.manager import (
    read_job,
    delete_job,
    start_analysis_job,
    get_job_results,
)


def _on_run_analysis() -> None:
    """Handle Run Analysis button click."""
    state = get_state()
    st.session_state['step2_error'] = None

    # Use factor_list_uid for internal mode; generate a unique ID for external mode
    job_id = state.factor_list_uid or uuid.uuid4().hex

    try:
        params = {
            'top_pct': state.top_x_pct,
            'bottom_pct': state.bottom_x_pct,
            'min_alpha': state.min_alpha,
            'benchmark_data': serialize_dataframe(state.benchmark_data),
            'benchmark_ticker': state.benchmark_ticker,
            'dataset_path': str(state.dataset_path) if state.dataset_path else None,
            'file_type': state.file_type,
        }
        start_analysis_job(job_id, params)
        update_state(current_job_id=job_id)
        st.rerun()
    except Exception as e:
        _set_error(f"Error starting analysis: {str(e)}")


def _on_job_completed(job_data: dict) -> None:
    """Handle job completion - load results and navigate."""
    state = get_state()

    try:
        metrics_df, corr_matrix = get_job_results(job_data)

        state.completed_steps.add(2)
        state.completed_steps.add(3)
        update_state(
            all_metrics=metrics_df,
            all_corr_matrix=corr_matrix,
            current_step=3,
            current_job_id=None
        )
    except Exception as e:
        _set_error(f"Error loading results: {str(e)}")
        delete_job(state.current_job_id)
        update_state(current_job_id=None)

    st.rerun()


def _on_job_error(job_id: str, error_msg: str) -> None:
    """Handle job failure - clean up and show error."""
    display_msg = error_msg.split('\n')[0] if error_msg else 'Unknown error'
    _set_error(f"Analysis failed: {display_msg}")
    delete_job(job_id)
    update_state(current_job_id=None)
    st.rerun()


def _set_error(message: str) -> None:
    """Set error message in session state."""
    st.session_state['step2_error'] = message



def _calculate_dataset_stats(preview_df: pd.DataFrame, metadata: dict) -> dict:
    """Calculate dataset statistics from preview data."""
    actual_row_count = metadata.get('num_rows', len(preview_df))
    unique_dates = metadata.get('unique_dates')
    dates = pd.to_datetime(preview_df['Date'])

    return {
        'num_rows': actual_row_count,
        'num_columns': len(preview_df.columns),
        'num_dates': unique_dates if unique_dates is not None else dates.nunique(),
        'min_date': format_date(dates.min()),
        'max_date': format_date(dates.max()),
    }


def _render_job_progress(job_data: dict) -> None:
    """Render job progress screen."""
    progress = job_data.get('progress', {})
    completed = progress.get('completed', 0)
    total = progress.get('total', 0)
    current_factor = progress.get('current_factor', '')

    _, center_col, _ = st.columns([1, 2, 1])

    with center_col:
        st.markdown("<div style='height: 100px'></div>", unsafe_allow_html=True)
        st.subheader("Running Factor Analysis")

        if total > 0:
            st.progress(completed / total, text=f"{completed} / {total} factors analyzed")
        else:
            st.progress(0, text="Initializing...")

        if current_factor:
            st.info(f"Analyzing: **{current_factor}**")
        else:
            st.info("Starting worker process...")


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

    reader = get_data_reader(state.dataset_path, file_type=state.file_type)
    preview_df = reader.read_preview(num_rows=10)
    metadata = reader.get_metadata()

    if preview_df is None or preview_df.empty:
        st.error("No data available for preview")
        return

    stats = _calculate_dataset_stats(preview_df, metadata)
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

    # No active job - show review content
    if not job_id:
        _render_review_content()
        return

    job_data = read_job(job_id)

    if job_data is None:
        update_state(current_job_id=None)
        st.rerun()

    status = job_data['status']

    if status == 'completed':
        _on_job_completed(job_data)

    elif status == 'error':
        _on_job_error(job_id, job_data.get('error', ''))

    else:
        # Job running - show progress and poll
        _render_job_progress(job_data)
        time.sleep(1)
        st.rerun()
