import streamlit as st

from src.core.context import get_state, update_state
from src.core.job_restore import restore_job_state
from src.ui.pages import render_new_analysis, render_results, render_history

ROUTES = {
    "history": render_history,
    "new_analysis": render_new_analysis,
    "results": render_results,
}

def render_content():
    qp_job_id = st.query_params.get("job_id")

    if qp_job_id and restore_job_state(qp_job_id):
        update_state(page="results")
    elif st.query_params.get("new_analysis"):
        step = st.query_params.get("step", 1)
        update_state(page="new_analysis", current_step=int(step))
    else:
        update_state(page="history")

    ROUTES.get(get_state().page)()
