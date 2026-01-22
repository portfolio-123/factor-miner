import streamlit as st
from src.ui.components.analyses import render_analysis_card
from src.workers.manager import list_all_analyses


def history() -> None:

    if not st.query_params.get("fl_id"):
        st.warning(
            "No Factor List selected. Please select a Factor List to view analysis history."
        )
        return

    st.title("Your Results")

    all_analyses = list_all_analyses(st.query_params.get("fl_id"))

    if not all_analyses:
        st.info("No past analyses found for this Factor List.")
        return

    for analysis in all_analyses:
        render_analysis_card(analysis)
