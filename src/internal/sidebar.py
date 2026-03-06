import streamlit as st

from src.internal.p123_client import verify_factor_list_access
from src.workers.analysis_service import AnalysisService


def get_selector_options() -> tuple[list[str], str, str]:
    user_uid = st.session_state.get("user_uid")
    options = AnalysisService.list_factor_lists(user_uid) if user_uid else []
    return options, "Factor Lists", "fl_selector"


def update_fl_name_on_select(selected: str) -> None:
    if token := st.session_state.get("access_token"):
        try:
            st.session_state.fl_name = verify_factor_list_access(selected, token).get("name", selected)
        except PermissionError:
            st.session_state.access_token = None
