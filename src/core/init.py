import streamlit as st

from src.core.environment import FACTOR_LIST_DIR
from src.core.context import update_state
from src.services.dataset_service import get_file_mtime, find_version_for_timestamp
from src.ui.styles import load_global_css


def init() -> None:
    if not (fl_id := st.query_params.get("fl_id")):
        st.error("No Factor List ID provided in URL.")
        st.stop()

    update_state(factor_list_uid=fl_id)

    path = FACTOR_LIST_DIR / fl_id
    if path.exists():
        update_state(active_dataset_file=str(path))
        timestamp = get_file_mtime(str(path))
        backup_version = find_version_for_timestamp(fl_id, timestamp)
        update_state(active_backup_version=backup_version)

    load_global_css()
