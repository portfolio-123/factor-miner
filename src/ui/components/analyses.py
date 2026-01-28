import re
import streamlit as st

from src.core.types import Analysis
from src.ui.components.common import section_header
from src.workers.analysis_service import analysis_service


def show_analysis_logs_modal(logs: list[str] | None) -> None:
    @st.dialog("Analysis Logs", width="large")
    def _render() -> None:
        if not logs:
            st.info("No logs available for this analysis.")
            return

        log_lines = []
        for log in logs:
            formatted = re.sub(
                r"(\[.*?\])", r'<span style="color: #2196F3;">\1</span>', log
            )
            log_lines.append(formatted)

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
    section_header("Notes")

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
