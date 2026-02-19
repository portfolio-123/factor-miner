import re
import streamlit as st

from src.core.types.models import Analysis, AnalysisProgress, AnalysisStatus
from src.core.config.environment import P123_BASE_URL
from src.ui.components.common import section_header
from src.workers.analysis_service import analysis_service


def show_analysis_logs_modal(logs: list[str] | None) -> None:
    @st.dialog("Analysis Logs", width="large")
    def _render() -> None:
        if not logs:
            st.info("No logs available for this analysis.")
            return

        log_lines = [
            re.sub(r"(\[.*?\])", r'<span style="color: #2196F3;">\1</span>', log)
            for log in logs
        ]

        log_html = "<br>".join(log_lines)
        st.html(
            f'<div style="font-family: monospace; font-size: 13px; '
            f"background: #f5f5f5; color: #333; padding: 12px; "
            f'border-radius: 4px; max-height: 400px; overflow-y: auto;">'
            f"{log_html}</div>"
        )

    _render()


@st.fragment
def render_analysis_notes(analysis: Analysis) -> None:
    section_header("Note")

    with st.form(key=f"notes_form_{analysis.id}", enter_to_submit=True, border=False):
        col_input, col_btn = st.columns([8, 1])

        with col_input:
            notes_value = st.text_input(
                "Notes",
                value=analysis.notes or "",
                placeholder="Add notes about this analysis...",
                label_visibility="collapsed",
            )

        with col_btn:
            submitted = st.form_submit_button("Save", width="stretch")

        if submitted and notes_value != (analysis.notes or ""):
            analysis_service.save(analysis, notes=notes_value)


def _render_failure_message(fl_id: str, error: str | None) -> None:
    error_msg = (error or "Analysis failed").split("\n")[0]
    st.error(error_msg)

    generate_url = f"{P123_BASE_URL}/sv/factorList/{fl_id}/generate"

    if "No column found with formula:" in error_msg:
        factors_url = f"{P123_BASE_URL}/sv/factorList/{fl_id}/factors"
        st.error(
            f"Click on [Add Missing]({factors_url}) to add the required formulas. "
            f"If you have already added them, make sure to [generate a new dataset]({generate_url})."
        )
    elif "single-date" in error_msg:
        st.error(
            f"Datasets of type Single Date are not supported. "
            f"Please [generate a new dataset]({generate_url}) using Period."
        )


def _render_progress_bar(progress: AnalysisProgress | None) -> None:
    has_progress = progress and progress.total > 0
    progress_value = (progress.completed / progress.total) if has_progress else 0
    progress_text = (
        f"{progress.completed} / {progress.total} factors analyzed"
        if has_progress
        else "Preparing analysis..."
    )

    with st.columns([1, 2, 1])[1]:
        st.space(100)
        st.subheader("Running Factor Analysis")
        st.progress(progress_value, text=progress_text)


@st.fragment(run_every="0.5s")
def render_analysis_progress(fl_id: str, analysis_id: str) -> None:
    analysis = analysis_service.get(fl_id, analysis_id)

    if analysis and analysis.status == AnalysisStatus.SUCCESS:
        st.rerun(scope="app")

    if analysis and analysis.status == AnalysisStatus.FAILED:
        _render_failure_message(fl_id, analysis.error)
        return

    _render_progress_bar(analysis.progress if analysis else None)

