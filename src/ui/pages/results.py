import streamlit as st
import pandas as pd
from src.core.types.models import AnalysisProgress, AnalysisStatus
from src.ui.components.common import render_info_item, get_card_header_html
from src.ui.components.tables import render_results_table
from src.ui.components.datasets import render_dataset_card
from src.ui.components.analyses import render_analysis_notes, show_analysis_logs_modal
from st_clipboard import copy_to_clipboard, copy_to_clipboard_unsecured
from src.core.utils.common import deserialize_dataframe, format_runtime, format_timestamp
from src.core.calculations import select_best_features
from src.workers.analysis_service import analysis_service
from src.services.dataset_service import dataset_service


@st.fragment(run_every="0.5s")
def _render_analysis_progress(fl_id: str, analysis_id: str) -> None:
    analysis = analysis_service.get(fl_id, analysis_id)

    if analysis and analysis.status == AnalysisStatus.SUCCESS:
        st.rerun(scope="app")

    if analysis and analysis.status == AnalysisStatus.FAILED:
        st.error((analysis.error or "Analysis failed").split("\n")[0])
        return

    progress = (
        analysis.progress
        if analysis
        else AnalysisProgress(completed=0, total=0, current_factor="-")
    )
    with st.columns([1, 2, 1])[1]:
        st.space(100)
        st.subheader("Running Factor Analysis")

        progress_value = (
            (progress.completed / progress.total)
            if (progress and progress.total > 0)
            else 0
        )
        progress_text = (
            f"{progress.completed} / {progress.total} factors analyzed"
            if (progress and progress.total > 0)
            else "Preparing analysis..."
        )
        st.progress(progress_value, text=progress_text)

        if progress:
            st.info(f"Analyzing: **{progress.current_factor}**")
        else:
            st.info("Starting...")


def results() -> None:
    fl_id = st.query_params.get("fl_id")
    if not (analysis_id := st.query_params.get("id")):
        st.error("Missing analysis id")
        return

    analysis = analysis_service.get(fl_id, analysis_id)
    if not analysis:
        st.error("Analysis not found")
        return

    try:
        dataset_metadata = dataset_service(fl_id).get_metadata(analysis.dataset_version)
        st.session_state.formulas_data = pd.DataFrame(dataset_metadata.formulas)
    except Exception as e:
        st.error(f"Failed to load dataset metadata: {e}")
        return
    created_on = format_timestamp(analysis.created_at)
    is_complete = analysis.status == AnalysisStatus.SUCCESS
    runtime_html = ""
    if is_complete:
        runtime = format_runtime(analysis.started_at, analysis.finished_at)
        runtime_html = f'<span style="font-size: 0.875rem; font-weight: 400; color: #666;">Run Time: {runtime}</span>'

    st.html(
        f'<p style="font-size: 1.5rem; font-weight: 700; margin: 0; display: flex; justify-content: space-between; align-items: baseline;">'
        f'<span>Analysis Results <span style="font-size: 0.875rem; font-weight: 400; color: #666; margin-left: 12px;">{created_on}</span></span>'
        f'{runtime_html}'
        f"</p>"
    )

    render_dataset_card(dataset_metadata)

    if analysis.status == AnalysisStatus.FAILED:
        st.subheader("Analysis Failed")
        st.error((analysis.error or "Analysis failed").split("\n")[0])
        return

    if analysis.status in (AnalysisStatus.PENDING, AnalysisStatus.RUNNING):
        _render_analysis_progress(fl_id, analysis_id)
        return

    col_left, col_right = st.columns(2)

    with col_left:
        with st.container(border=True):
            st.html(get_card_header_html("Best Factors"))
            param_items = [
                render_info_item("Max. Factors", f"{analysis.params.n_factors}"),
                render_info_item("Min Annual. Alpha", f"{analysis.params.min_alpha}%"),
                render_info_item(
                    "Max Correlation", f"{analysis.params.correlation_threshold}"
                ),
            ]
            st.html(
                f'<div style="display: flex; gap: 24px;">{"".join(param_items)}</div>'
            )

    with col_right:
        with st.container(border=True):
            st.html(get_card_header_html("Factor Portfolio"))
            param_items = [
                render_info_item("Benchmark", f"{analysis.params.benchmark_ticker}"),
                render_info_item("Top X (Long)", f"{analysis.params.top_pct}%"),
                render_info_item("Bottom X (Short)", f"{analysis.params.bottom_pct}%"),
            ]
            st.html(
                f'<div style="display: flex; gap: 24px;">{"".join(param_items)}</div>'
            )

    render_analysis_notes(analysis)

    best_factors_tab, all_factors_tab = st.tabs(["Best Factors", "All Factors"])

    with best_factors_tab:
        all_metrics_df = deserialize_dataframe(analysis.results.all_metrics)
        corr_matrix_df = deserialize_dataframe(analysis.results.all_corr_matrix)

        best_feature_names, factor_classifications = select_best_features(
            metrics_df=all_metrics_df,
            correlation_matrix=corr_matrix_df,
            N=analysis.params.n_factors,
            correlation_threshold=analysis.params.correlation_threshold,
            a_min=analysis.params.min_alpha,
        )

        header_left, header_right = st.columns([6, 1])
        with header_left:
            st.caption(
                "Best factors ranked by absolute annualized alpha (highest first)"
            )
        with header_right:
            if st.button("Logs", type="primary", width="stretch"):
                show_analysis_logs_modal(analysis.logs)

        render_results_table(
            all_metrics_df[all_metrics_df["column"].isin(best_feature_names)],
            key="best_factors",
        )

        if best_feature_names:
            st.divider()
            st.subheader("Correlation Matrix (Best Factors)")
            best_corr_matrix = corr_matrix_df.loc[
                best_feature_names, best_feature_names
            ]
            st.dataframe(
                best_corr_matrix.round(4),
                height=min(400, 50 + len(best_feature_names) * 35),
                width="stretch",
            )

            _, col1, col2 = st.columns([3, 1, 1])
            corr_csv_copy = best_corr_matrix.round(4).to_csv(sep="\t")
            corr_csv_download = best_corr_matrix.round(4).to_csv()

            with col1:
                if st.button(
                    type="primary",
                    label="Copy to Clipboard",
                    width="stretch",
                    key="corr_matrix_copy",
                ):
                    copy_to_clipboard_unsecured(corr_csv_copy)
                    copy_to_clipboard(corr_csv_copy)
                    st.toast("Correlation matrix copied to clipboard")

            with col2:
                file_name = (
                    f"{fl_id}_correlation_matrix.csv"
                    if fl_id
                    else "correlation_matrix.csv"
                )
                st.download_button(
                    type="primary",
                    label="Download CSV",
                    data=corr_csv_download,
                    file_name=file_name,
                    mime="text/csv",
                    width="stretch",
                    key="corr_matrix_download",
                )

    with all_factors_tab:
        header_left, header_right = st.columns([6, 1])
        with header_left:
            st.caption(
                "All factors ranked by absolute annualized alpha (highest first)"
            )
        with header_right:
            if st.button(
                "Logs", type="primary", width="stretch", key="all_factors_logs"
            ):
                show_analysis_logs_modal(analysis.logs)

        render_results_table(
            all_metrics_df,
            factor_classifications=factor_classifications,
            key="all_factors",
        )
