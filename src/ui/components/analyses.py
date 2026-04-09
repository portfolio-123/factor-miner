import re
import streamlit as st

from src.core.types.models import Analysis, AnalysisProgress, AnalysisStatus
from src.internal.errors import format_analysis_error
from src.ui.components.common import section_header
from src.workers.analysis_service import AnalysisService


@st.dialog("Analysis Logs", width="large")
def show_analysis_logs_modal(analysis_id: str):
    fl_id = st.query_params["fl_id"]
    user_uid = st.session_state.get("user_uid")
    logs = AnalysisService(user_uid).get_logs(fl_id, analysis_id)
    if not logs:
        st.info("No logs available for this analysis.")
        return

    log_lines = [re.sub(r"(\[.*?\])", r'<span style="color: #2196F3;">\1</span>', log) for log in logs]

    log_html = "<br>".join(log_lines)
    st.html(
        f'<div style="font-family: monospace; font-size: 13px; '
        f"background: #f5f5f5; color: #333; padding: 12px; "
        f'border-radius: 4px; max-height: 400px; overflow-y: auto;">'
        f"{log_html}</div>"
    )


@st.fragment
def render_analysis_notes(analysis: Analysis):
    section_header("Note")

    with st.form(key=f"notes_form_{analysis.id}", enter_to_submit=True, border=False):
        col_input, col_btn = st.columns([8, 1])

        with col_input:
            notes_value = st.text_input(
                "Notes", value=analysis.notes or "", placeholder="Add notes about this analysis...", label_visibility="collapsed"
            )

        with col_btn:
            submitted = st.form_submit_button("Save", width="stretch")

        if submitted and notes_value != (analysis.notes or ""):
            user_uid = st.session_state.get("user_uid")
            AnalysisService(user_uid).save(analysis, {"notes": notes_value})


def _render_progress_bar(progress: AnalysisProgress | None):
    if progress:
        progress_value = progress.completed / progress.total
        progress_text = f"{progress.completed} / {progress.total} factors analyzed"
    else:
        progress_value = 0
        progress_text = "Preparing analysis..."

    with st.columns([1, 2, 1])[1]:
        st.space(100)
        st.subheader("Running Factor Analysis")
        st.progress(progress_value, text=progress_text)


@st.fragment(run_every="3s")
def render_analysis_progress(fl_id: str, analysis_id: str):
    user_uid = st.session_state.get("user_uid")
    analysis = AnalysisService(user_uid).get(fl_id, analysis_id)

    if analysis and analysis.status == AnalysisStatus.SUCCESS:
        st.rerun(scope="app")

    if analysis and analysis.status == AnalysisStatus.FAILED:
        st.error(format_analysis_error(analysis.error, analysis.error_type))
        return

    _render_progress_bar(analysis.progress if analysis else None)
