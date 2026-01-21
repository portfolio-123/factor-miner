import streamlit as st

from src.ui.styles import load_global_css


def init() -> None:
    if not st.query_params.get("fl_id"):
        st.error("No Factor List ID provided in URL.")
        st.stop()

    load_global_css()
