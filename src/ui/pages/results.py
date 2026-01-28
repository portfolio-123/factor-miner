import streamlit as st
import pandas as pd
from src.core.types import AnalysisStatus
from src.ui.components.common import render_info_item, section_header
from src.ui.components.tables import render_results_table
from src.ui.components.datasets import render_dataset_card
from src.ui.components.analyses import render_analysis_notes, show_analysis_logs_modal
from src.core.utils import deserialize_dataframe
from src.workers.manager import read_analysis
from src.services.dataset_service import get_dataset_metadata


@st.fragment(run_every="0.5s")
def _render_analysis_progress(fl_id: str, analysis_id: str) -> None:
    analysis = read_analysis(fl_id, analysis_id)
    
    if analysis.status == AnalysisStatus.SUCCESS:
        st.rerun(scope="app")

    if analysis.status == AnalysisStatus.FAILED:
        st.error((analysis.error or "Analysis failed").split("\n")[0])
        return

    progress = analysis.progress

    with st.columns([1, 2, 1])[1]:
        st.space(100)
        st.subheader("Running Factor Analysis")

        completed = progress.get("completed", 0)
        total = progress.get("total")
        current_factor = progress.get("current_factor")

        st.progress(
            completed / total if total > 0 else 0,
            text=f"{completed} / {total} factors analyzed",
        )

        if current_factor:
            st.info(f"Analyzing: **{current_factor}**")
        else:
            st.info("Starting...")

def results() -> None:
    fl_id = st.query_params.get("fl_id")
    if not (analysis_id := st.query_params.get("id")):
        st.error("Missing analysis id")
        return

    analysis = read_analysis(fl_id, analysis_id)
    if not analysis:
        st.error("Analysis not found")
        return

    try:
        dataset_metadata = get_dataset_metadata(fl_id, analysis.dataset_version)
        st.session_state.formulas_data = pd.DataFrame(dataset_metadata.formulas)
    except Exception as e:
        st.error(f"Failed to load dataset metadata: {e}")
        return

    render_dataset_card(dataset_metadata)

    if analysis.status == AnalysisStatus.FAILED:
        st.subheader("Analysis Failed")
        st.error((analysis.error or "Analysis failed").split("\n")[0])
        return

    if analysis.status in (AnalysisStatus.PENDING, AnalysisStatus.RUNNING):
        _render_analysis_progress(fl_id, analysis_id)
        return

    render_analysis_notes(analysis)

    if st.button("View Logs", icon=":material/description:"):
        show_analysis_logs_modal(analysis.logs)

    section_header("Analysis Parameters")

    param_items = [
        render_info_item("Min Alpha", f"{analysis.params.min_alpha}%"),
        render_info_item("Top X", f"{analysis.params.top_pct}%"),
        render_info_item("Bottom X", f"{analysis.params.bottom_pct}%"),
    ]
    st.html(f'<div style="display: flex; gap: 24px;">{"".join(param_items)}</div>')

    section_header("Factors Sorted by Abs. Annual Alpha")

    render_results_table(
        deserialize_dataframe(analysis.results["all_metrics"])
    )
