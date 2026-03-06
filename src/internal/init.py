import streamlit as st


def validate_fl_id() -> None:
    """Validate that fl_id is provided in URL (required for internal mode)."""
    if not st.query_params.get("fl_id"):
        st.error("No Factor List ID provided in URL.")
        st.stop()
