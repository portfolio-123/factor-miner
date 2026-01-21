import streamlit as st
from src.core.context import get_state
from src.ui.components.common import section_header
from src.ui.components.analyses import render_analysis_card
from src.workers.manager import list_all_analyses


def history() -> None:
    state = get_state()

    if not state.factor_list_uid:
        st.warning(
            "No Factor List selected. Please select a Factor List to view analysis history."
        )
        return

    all_analyses = list_all_analyses(state.factor_list_uid)

    if not all_analyses:
        st.info("No past analyses found for this Factor List.")
        return

    section_header("Past Analyses")
    for analysis in all_analyses:
        render_analysis_card(analysis)
