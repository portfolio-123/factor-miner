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
        dataset_metadata = BackupDatasetService(fl_id).get_metadata(
            analysis.dataset_version
        )
        st.session_state.formulas_data = dataset_metadata.formulas_df
    except Exception as e:
        st.error(f"Failed to load dataset metadata: {e}")
        return
    created_on = format_timestamp(analysis.created_at)
    is_complete = analysis.status == AnalysisStatus.SUCCESS

    header_left, header_right = st.columns([8, 1])
    with header_left:
        st.html(
            f'<p style="font-size: 1.5rem; font-weight: 700; margin: 0;">'
            f'Analysis Results <span style="font-size: 0.875rem; font-weight: 400; color: #666; margin-left: 12px;">{created_on}</span>'
            f"</p>"
        )
    with header_right:
        if is_complete or analysis.status == AnalysisStatus.FAILED:
            if st.button("Logs", type="primary", key="header_logs", width="stretch"):
                show_analysis_logs_modal(analysis.logs)

    if analysis.status == AnalysisStatus.FAILED:
        error_msg = (analysis.error or "Analysis failed").split("\n")[0]
        st.error(f"Analysis failed: {error_msg}")

        if "No column found with formula:" in error_msg:
            factors_url = f"{P123_BASE_URL}/sv/factorList/{fl_id}/factors"
            generate_url = f"{P123_BASE_URL}/sv/factorList/{fl_id}/generate"
            st.info(
                f"Click on [Add Missing]({factors_url}) to add the required formulas. "
                f"If you have already added them, make sure to [generate a new dataset]({generate_url})."
            )
        return

    if analysis.status in (AnalysisStatus.PENDING, AnalysisStatus.RUNNING):
        render_analysis_progress(fl_id, analysis_id)
        return

    all_metrics_df = deserialize_dataframe(analysis.results.all_metrics)
    corr_matrix_df = deserialize_dataframe(analysis.results.all_corr_matrix)
    rank_by = getattr(analysis.params, "rank_by", "Alpha")

    # add rank column
    sort_col = "IC" if rank_by == "IC" else "annualized alpha %"
    all_metrics_df = all_metrics_df.sort_values(
        by=sort_col, key=abs, ascending=False
    ).reset_index(drop=True)
    all_metrics_df["rank"] = range(1, len(all_metrics_df) + 1)

    best_feature_names = analysis.results.best_feature_names
    factor_classifications = analysis.results.factor_classifications
    na_excluded_count = sum(1 for c in factor_classifications.values() if c == "high_na")
    st.success(
        f"Analysis completed in {format_runtime(analysis.started_at, analysis.finished_at)}. "
        f"Found **{len(best_feature_names)}** of **{analysis.params.n_factors}** requested Best Factors. "
        f"Number of factors excluded by NAs: {na_excluded_count}."
    )

    settings_tab, best_factors_tab, all_factors_tab = st.tabs(
        ["Settings", "Best Factors", "All Factors"]
    )

    with settings_tab:
        render_dataset_card(dataset_metadata)

        with st.container(border=True):
            st.markdown("#### Analysis Settings", unsafe_allow_html=True)
            p = analysis.params
            clean_min_alpha = 0 if p.min_alpha < 1e-9 else p.min_alpha
            settings = [
                ("Rank By", rank_by),
                ("Max. Factors", p.n_factors),
                ("Min. IC", p.min_ic) if rank_by == "IC" else ("Min. Annual Alpha", f"{clean_min_alpha}%"),
                ("Max Correlation", p.correlation_threshold),
                ("Max NA", f"{p.max_na_pct}%"),
                ("Benchmark", dataset_metadata.benchmark),
                ("Top X (Long)", f"{p.top_pct}%"),
                ("Bottom X (Short)", f"{p.bottom_pct}%"),
            ]
            items_html = "".join(render_info_item(label, value) for label, value in settings)
            st.html(f'<div style="display: flex; gap: 24px; flex-wrap: wrap;">{items_html}</div>')

        render_analysis_notes(analysis)

    with best_factors_tab:
        if best_feature_names:
            metric_label = "IC" if rank_by == "IC" else "absolute annualized alpha"
            st.caption(f"Best factors ranked by {metric_label} (highest first)")

            render_results_table(
                all_metrics_df[all_metrics_df["column"].isin(best_feature_names)],
                key="best_factors",
                rank_by=rank_by,
            )

            st.divider()
            best_corr_matrix = corr_matrix_df.loc[
                best_feature_names, best_feature_names
            ]
            render_correlation_matrix(
                corr_matrix_df=best_corr_matrix,
                title="Correlation Matrix (Best Factors)",
                file_prefix=fl_id,
            )
        else:
            st.info(
                "No factors met all the selection criteria. "
                "You can view all factors and their classifications in the **All Factors** tab."
            )

    with all_factors_tab:
        metric_label = "IC" if rank_by == "IC" else "absolute annualized alpha"
        st.caption(f"All factors ranked by {metric_label} (highest first)")

        render_results_table(
            all_metrics_df,
            factor_classifications=factor_classifications,
            key="all_factors",
            rank_by=rank_by,
        )
