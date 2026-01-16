import streamlit as st

from src.core.context import (
    get_state,
    update_state,
    sync_url_for_results,
    sync_url_for_new_analysis,
    sync_url_for_history,
)
from src.core.analysis_restore import restore_analysis_state
from src.ui.pages import render_new_analysis, render_results, render_history

ROUTES = {
    "history": render_history,
    "new_analysis": render_new_analysis,
    "results": render_results,
}


def render_content():
    qp_analysis_id = st.query_params.get("analysis_id")

    if qp_analysis_id and restore_analysis_state(qp_analysis_id):
        update_state(page="results")
        sync_url_for_results(qp_analysis_id)
    elif st.query_params.get("new_analysis"):
        step = int(st.query_params.get("step", 1))
        update_state(page="new_analysis", current_step=step)
        sync_url_for_new_analysis(step)
    else:
        update_state(page="history")
        sync_url_for_history()

    ROUTES.get(get_state().page)()
