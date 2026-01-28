import streamlit as st


def init() -> None:
    st.set_page_config(
        page_title="Factor Miner - Portfolio123",
        page_icon="assets/favicon.png",
        layout="wide",
    )

    if not st.query_params.get("fl_id"):
        st.error("No Factor List ID provided in URL.")
        st.stop()

    st.html(
        """
    <style>
    .stMainBlockContainer,
    .block-container,
    [data-testid="stMainBlockContainer"] {
        max-width: 1250px !important;
        margin: 0 auto !important;
        padding: 0.5rem 2rem 2rem 2rem !important;
    }

    div[data-testid="stVerticalBlockBorderWrapper"] > div {
        padding-top: 0.5rem;
        padding-bottom: 0.5rem;
    }
    </style>
    """
    )
