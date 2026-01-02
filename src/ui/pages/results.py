import streamlit as st
import pandas as pd
from src.core.context import get_state, update_state
from src.core.constants import JobStatus
from src.ui.components import section_header, render_results_table, copy_to_clipboard_button, add_formula_column
from src.core.calculations import select_best_features as _select_best_features
from src.workers.manager import delete_job, read_job
from src.services.processing import process_step2_completion, _merge_worker_logs


@st.cache_data
def select_best_features_cached(
    job_id: str,
    _all_metrics,
    _all_corr_matrix,
    n_features: int,
    correlation_threshold: float,
    min_alpha: float,
):
    # using job_id to invalidate cache on a new analysis.

    return _select_best_features(
        _all_metrics,
        _all_corr_matrix,
        N=n_features,
        correlation_threshold=correlation_threshold,
        a_min=min_alpha,
    )


def _on_job_completed(job_data: dict) -> None:
    error = process_step2_completion(job_data)
    if error:
        update_state(analysis_error=error)
    st.rerun()


def _on_job_error(job_id: str, job_data: dict) -> None:
    _merge_worker_logs(job_data)
    error_msg = job_data.get('error', '')
    display_msg = error_msg.split('\n')[0] if error_msg else 'Unknown error'
    update_state(
        analysis_error=f"Analysis failed: {display_msg}",
        current_job_id=None,
    )
    delete_job(job_id)
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


@st.fragment
def _render_filter_and_results() -> None:
    state = get_state()

    col1, col2, _ = st.columns([1, 1, 2])

    with col1:
        correlation_threshold = st.slider(
            "Correlation Threshold",
            min_value=0.0,
            max_value=1.0,
            value=state.filter_correlation,
            key="filter_correlation",
            step=0.05,
        )

    with col2:
        n_features = st.number_input(
            "Number of Features",
            min_value=1,
            max_value=100,
            value=state.filter_n_features,
            key="filter_n_features",
            step=1,
        )

    if (
        correlation_threshold != state.correlation_threshold
        or n_features != state.n_features
    ):
        update_state(correlation_threshold=correlation_threshold, n_features=n_features)

    best_features = select_best_features_cached(
        state.current_job_id,
        state.all_metrics,
        state.all_corr_matrix,
        n_features,
        correlation_threshold,
        state.min_alpha,
    )

    section_header("Best Performing Factors")

    filtered_best_features = render_results_table(
        best_features, state.all_metrics
    )

    _render_action_buttons(filtered_best_features)


def _prepare_download_csv(display_df: pd.DataFrame) -> str:
    state = get_state()
    download_df = add_formula_column(display_df, state.formulas_data)
    return download_df.to_csv(index=False)


def _render_action_buttons(display_df: pd.DataFrame | None) -> None:
    state = get_state()

    if display_df is None or display_df.empty:
        return

    _, col1, col2 = st.columns([3, 1, 1])

    # tab delimited for copy to clipboard (without Formula)
    csv_to_copy = display_df.to_csv(index=False, sep="\t")

    # comma delimited for file download (with Formula in second position)
    csv_to_download = _prepare_download_csv(display_df)

    with col1:
        copy_to_clipboard_button(csv_to_copy, key="copy_clipboard_btn")

    with col2:
        st.download_button(
            type="primary",
            label="Download CSV",
            data=csv_to_download,
            file_name=f"{state.factor_list_uid}_best_features.csv",
            mime="text/csv",
            width="stretch"
        )


def render() -> None:
    state = get_state()

    if state.analysis_error:
        st.error(state.analysis_error)
        return

    if state.current_job_id:
        job_data = read_job(state.current_job_id)
        if job_data and job_data.get('status') in (JobStatus.PENDING, JobStatus.RUNNING):
            _render_job_progress(state.current_job_id)
            return

    _render_filter_and_results()
