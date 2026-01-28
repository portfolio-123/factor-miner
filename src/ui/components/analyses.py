import streamlit as st

from src.core.types import Analysis
from src.ui.components.common import section_header
from src.workers.manager import update_analysis


@st.fragment
def render_analysis_notes(analysis: Analysis) -> None:
    section_header("Notes")

    def _save_notes():
        notes_value = st.session_state.get(f"notes_{analysis.id}", "")
        if notes_value != (analysis.notes or ""):
            update_analysis(analysis, notes=notes_value)

    col_input, col_btn = st.columns([8, 1])

    with col_input:
        st.text_input(
            "Notes",
            value=analysis.notes or "",
            placeholder="Add notes about this analysis...",
            label_visibility="collapsed",
            key=f"notes_{analysis.id}",
            on_change=_save_notes,
        )

    with col_btn:
        if st.button("Save", key=f"save_notes_{analysis.id}", width="stretch"):
            _save_notes()
