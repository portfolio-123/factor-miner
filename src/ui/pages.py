import streamlit as st
from src.core.context import get_state
from src.ui.steps.step0 import render as render_history
from src.ui.steps import render_step
from src.ui.steps.step3 import render_analysis_name_input
from src.ui.components import (
    header_with_navigation,
    render_current_dataset_header,
    header_simple_back,
    show_formulas_modal
)
from src.services.readers import ParquetDataReader
from src.workers.manager import get_dataset_formulas_from_backup
import os


def history_page():
    render_history()


def analysis_page():
    state = get_state()

    # Handle view formulas modal
    if "view_formulas_ds_ver" in st.query_params:
        ds_ver = st.query_params["view_formulas_ds_ver"]
        del st.query_params["view_formulas_ds_ver"]
        st.session_state.show_formulas_modal = True
        st.session_state.formulas_fl_id = state.factor_list_uid
        st.session_state.formulas_ds_ver = ds_ver
        st.rerun()

    if st.session_state.get("show_formulas_modal"):
        formulas_fl_id = st.session_state.get("formulas_fl_id")
        formulas_ds_ver = st.session_state.get("formulas_ds_ver")
        if formulas_fl_id and formulas_ds_ver:
            try:
                # Check if current dataset matches the version
                is_current = False
                if state.dataset_path and os.path.exists(state.dataset_path):
                     ts = os.path.getmtime(state.dataset_path)
                     current_ver = str(int(ts))
                     if current_ver == formulas_ds_ver:
                         is_current = True
                
                if is_current:
                    reader = ParquetDataReader(state.dataset_path)
                    formulas_df = reader.get_formulas_df()
                else:
                    formulas_df = get_dataset_formulas_from_backup(formulas_fl_id, formulas_ds_ver)
                
                show_formulas_modal(formulas_df)
            except Exception:
                st.session_state.show_formulas_modal = False

    # if we have a current_job_id, we are viewing an existing analysis
    if state.current_job_id:
        header_simple_back()
        render_current_dataset_header()
        render_analysis_name_input()
        render_step(state.current_step)
    else:
        # Creating new analysis
        header_with_navigation()
        render_current_dataset_header()
        render_analysis_name_input()
        render_step(state.current_step)
