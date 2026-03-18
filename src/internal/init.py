import streamlit as st


def validate_fl_id():
    if not st.query_params.get("fl_id"):
        st.error("No Factor List ID provided in URL.")
        st.stop()
