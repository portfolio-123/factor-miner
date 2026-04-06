import streamlit as st
from src.ui.components.tables import render_history_table
from src.workers.analysis_service import AnalysisService


def history() -> None:
    fl_id = st.query_params.get("fl_id")
    user_uid = st.session_state.get("user_uid")

    if not fl_id:
        st.warning(
            "No Factor List selected. Please select a Factor List to view analysis history."
        )
        return

    st.title("Your Results")

    all_analyses = AnalysisService(user_uid).list_all(fl_id)

    if not all_analyses:
        st.info(
            f"No past analyses found for this Factor List. "
            f"[Create an analysis here](/create?fl_id={fl_id})"
        )
        return

    render_history_table(all_analyses)
