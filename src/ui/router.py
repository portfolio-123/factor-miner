import streamlit as st

from src.ui.pages.history import history
from src.ui.pages.create import create_form
from src.ui.pages.results import results


def render_content():
    if analysis_id := st.query_params.get("analysis_id"):
        results(analysis_id)
    elif "create" in st.query_params:
        create_form()
    else:
        history()
