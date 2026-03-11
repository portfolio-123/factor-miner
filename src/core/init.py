import streamlit as st

from src.core.config.environment import INTERNAL_MODE
from src.services.dataset_service import DatasetService
from src.internal.init import validate_fl_id


def init() -> None:
    st.set_page_config(
        page_title="FactorMiner - Portfolio123",
        page_icon="assets/favicon.png",
        layout="wide",
    )

    if INTERNAL_MODE:  # internal
        validate_fl_id()
    else:  # external
        if not (fl_id := st.query_params.get("fl_id")):
            available = DatasetService.list_datasets()
            if not available:
                st.warning("No datasets found. Add .parquet datasets to the data directory.")
                st.stop()
            fl_id = available[0]
            st.query_params["fl_id"] = fl_id
        st.session_state.setdefault("fl_name", fl_id)

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
