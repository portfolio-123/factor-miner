import os
from pathlib import Path

import streamlit as st

from src.core.context import update_state
from src.ui.styles import load_global_css


def init() -> None:
    if not (fl_id := st.query_params.get("fl_id")):
        st.error("No Factor List ID provided in URL.")
        st.stop()

    update_state(factor_list_uid=fl_id)

    try:
        path = Path(os.environ["FACTOR_LIST_DIR"]) / fl_id
        if path.exists():
            update_state(dataset_path=str(path))
    except KeyError:
        st.error("Error locating factor list dataset")
        st.stop()

    load_global_css()
