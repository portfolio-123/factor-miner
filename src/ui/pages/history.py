import streamlit as st
from src.ui.components.tables import render_history_table
from src.workers.analysis_service import analysis_service


def history() -> None:

    if not st.query_params.get("fl_id"):
        st.warning(
            "No Factor List selected. Please select a Factor List to view analysis history."
        )
        return

    st.title("Your Results")

    all_analyses = analysis_service.list_all(st.query_params.get("fl_id"))

    if not all_analyses:
        fl_id = st.query_params.get("fl_id")
        st.info(
            f"No past analyses found for this Factor List. "
            f"[Create an analysis here](/create?fl_id={fl_id})"
        )
        return

    render_history_table(all_analyses)
