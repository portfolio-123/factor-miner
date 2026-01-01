import streamlit as st

from src.core.context import update_state
from src.core.utils import locate_factor_list_file


def init_state() -> None:
    fl_id = st.query_params.get("fl_id")
    update_state(factor_list_uid=fl_id)

    try:
        if dataset_path := locate_factor_list_file(fl_id):
            update_state(dataset_path=dataset_path)
    except ValueError as e:
        st.error(str(e))
        st.stop()
