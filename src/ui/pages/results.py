import streamlit as st
import pandas as pd
from src.core.types.models import AnalysisStatus
from src.core.config.environment import P123_BASE_URL
from src.ui.components.common import render_info_item, get_card_header_html
from src.ui.components.tables import render_results_table, render_correlation_matrix
from src.ui.components.datasets import render_dataset_card
from src.ui.components.analyses import (
    render_analysis_notes,
    show_analysis_logs_modal,
    render_analysis_progress,
)
from src.core.utils.common import (
    deserialize_dataframe,
    format_runtime,
    format_timestamp,
)
from src.core.calculations import select_best_features
from src.workers.analysis_service import analysis_service
from src.services.dataset_service import BackupDatasetService


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
        dataset_metadata = BackupDatasetService(fl_id).get_metadata(analysis.dataset_version)
        st.session_state.formulas_data = dataset_metadata.formulas_df
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
        f"{runtime_html}"
        f"</p>"
    )

    if analysis.status == AnalysisStatus.FAILED:
        st.subheader("Analysis Failed")
        error_msg = (analysis.error or "Analysis failed").split("\n")[0]
        st.error(error_msg)

        if "No column found with formula:" in error_msg:
            factors_url = f"{P123_BASE_URL}/sv/factorList/{fl_id}/factors"
            generate_url = f"{P123_BASE_URL}/sv/factorList/{fl_id}/generate"
            st.error(
                f"Click on [Add Missing]({factors_url}) to add the required formulas. "
                f"If you have already added them, make sure to [generate a new dataset]({generate_url})."
            )
        return

    if analysis.status in (AnalysisStatus.PENDING, AnalysisStatus.RUNNING):
        render_analysis_progress(fl_id, analysis_id)
        return

    all_metrics_df = deserialize_dataframe(analysis.results.all_metrics)
    corr_matrix_df = deserialize_dataframe(analysis.results.all_corr_matrix)

    # add rank column
    all_metrics_df = all_metrics_df.sort_values(
        by="annualized alpha %", key=abs, ascending=False
    ).reset_index(drop=True)
    all_metrics_df["rank"] = range(1, len(all_metrics_df) + 1)

    best_feature_names, factor_classifications = select_best_features(
        metrics_df=all_metrics_df,
        correlation_matrix=corr_matrix_df,
        N=analysis.params.n_factors,
        correlation_threshold=analysis.params.correlation_threshold,
        a_min=analysis.params.min_alpha,
    )

    settings_tab, best_factors_tab, all_factors_tab = st.tabs(
        ["Settings", "Best Factors", "All Factors"]
    )

    with settings_tab:
        render_dataset_card(dataset_metadata)
        
        st.markdown("#### Analysis Settings", unsafe_allow_html=True)

        col_left, col_right = st.columns(2)

        
        with col_left:
            with st.container(border=True):
                st.html(get_card_header_html("Best Factors"))
                param_items = [
                    render_info_item("Max. Factors", f"{analysis.params.n_factors}"),
                    render_info_item(
                        "Min. Annual Alpha", f"{analysis.params.min_alpha}%"
                    ),
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
                    render_info_item(
                        "Benchmark", f"{analysis.params.benchmark_ticker}"
                    ),
                    render_info_item("Top X (Long)", f"{analysis.params.top_pct}%"),
                    render_info_item(
                        "Bottom X (Short)", f"{analysis.params.bottom_pct}%"
                    ),
                ]
                st.html(
                    f'<div style="display: flex; gap: 24px;">{"".join(param_items)}</div>'
                )

        render_analysis_notes(analysis)

    with best_factors_tab:
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
            best_corr_matrix = corr_matrix_df.loc[
                best_feature_names, best_feature_names
            ]
            render_correlation_matrix(
                corr_matrix_df=best_corr_matrix,
                title="Correlation Matrix (Best Factors)",
                file_prefix=fl_id,
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
