from datetime import datetime
import streamlit as st
from src.core.context import get_state, update_state
from src.core.types import AnalysisParams, SettingsForm
from src.workers.manager import start_analysis
from src.services.dataset_service import get_file_mtime, create_version_dir_name


def submit_settings(settings: SettingsForm) -> None:
    update_state(analysis_settings=settings)
    st.query_params["step"] = "2"
    st.rerun()


def submit_analysis_creation() -> None:
    state = get_state()
    fl_id = state.factor_list_uid

    # if there's already a backup from the active dataset, use it
    if state.active_backup_version:
        dataset_version = state.active_backup_version
    else:
        # if not, create a new one
        timestamp = get_file_mtime(state.active_dataset_file)
        # get version number and timestamp formatted like the other dirs, 1_134123412341234
        dataset_version = create_version_dir_name(fl_id, timestamp)

    analysis_ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    analysis_id = f"{fl_id}/{dataset_version}/{analysis_ts}.json"

    try:
        settings = state.analysis_settings
        params = AnalysisParams(
            **settings.model_dump(),
            active_dataset_file=state.active_dataset_file,
            access_token=state.access_token,
        )
        start_analysis(analysis_id, params)
        update_state(analysis_id=analysis_id, active_backup_version=dataset_version)

        st.query_params.from_dict({"fl_id": fl_id, "analysis_id": analysis_id})
        st.rerun()
    except Exception as e:
        st.toast(f"Error starting analysis: {e}")
