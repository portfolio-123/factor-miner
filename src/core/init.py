import streamlit as st

from src.core.context import update_state
from src.ui.styles import load_global_css


def init() -> None:
    fl_id = st.session_state.get("fl_id")

    if fl_id:
        update_state(factor_list_uid=fl_id)

    load_global_css()
