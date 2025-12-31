import streamlit as st
from src.core.context import get_state
from src.ui.steps.step0 import render as render_history
from src.ui.steps import render_step
from src.ui.components import (
    header_with_navigation,
    header_simple_back,
    show_formulas_modal,
)
from src.ui.header import render_current_dataset_header
from src.workers.manager import get_formulas_df_for_version


def history_page():
    render_history()


def analysis_page():
    state = get_state()

    formulas_ds_ver = st.session_state.get("formulas_ds_ver")
    if formulas_ds_ver:
        formulas_df = get_formulas_df_for_version(
            state.factor_list_uid, formulas_ds_ver
        )
        if formulas_df is not None:
            show_formulas_modal(formulas_df)
        else:
            st.session_state.formulas_ds_ver = None

    if state.current_job_id:
        header_simple_back()
    else:
        header_with_navigation()

    render_current_dataset_header()
    render_step(state.current_step)
