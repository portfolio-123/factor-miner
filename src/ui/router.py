import streamlit as st

from pages.history import history
from pages.create import create_form
from pages.results import results


def render_content():
    fl_id = st.query_params.get("fl_id")
    analysis_id = st.query_params.get("id")

    if fl_id and analysis_id:
        results(fl_id, analysis_id)
    elif "create" in st.query_params:
        create_form()
    else:
        history()
