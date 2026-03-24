import streamlit as st


def init() -> None:
    st.set_page_config(
        page_title="FactorMiner - Portfolio123",
        page_icon="assets/favicon.png",
        layout="wide",
    )

    st.html(
        """
    <style>
    header[data-testid="stHeader"] {
        background: transparent !important;
        height: auto !important;
        min-height: 0 !important;
    }

    .stMainBlockContainer,
    .block-container,
    [data-testid="stMainBlockContainer"] {
        max-width: 1350px !important;
        margin: 0 auto !important;
        padding: 0.5rem 2rem 2rem 2rem !important;
    }

    div[data-testid="stVerticalBlockBorderWrapper"] > div {
        padding-top: 0.5rem;
        padding-bottom: 0.5rem;
    }

    [data-testid="stDataFrame"] [data-testid="stElementToolbar"] {
        display: none !important;
    }
    </style>
    """
    )
