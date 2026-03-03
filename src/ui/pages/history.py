import streamlit as st
from src.ui.components.tables import render_history_table
from src.workers.analysis_service import AnalysisService


def history() -> None:

    if not st.query_params.get("fl_id"):
        st.warning(
            "No Factor List selected. Please select a Factor List to view analysis history."
        )
        return

    st.title("Your Results")

    user_uid = st.session_state.get("user_uid")
    all_analyses = AnalysisService(user_uid).list_all(st.query_params.get("fl_id"))

    if not all_analyses:
        fl_id = st.query_params.get("fl_id")
        st.info(
            f"No past analyses found for this Factor List. "
            f"[Create an analysis here](/create?fl_id={fl_id})"
        )
        return

    render_history_table(all_analyses)
