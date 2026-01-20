import streamlit as st
from src.core.utils import format_dataset_option
from src.core.context import get_state, update_state
from src.ui.components.common import section_header
from src.ui.components.datasets import render_dataset_card, render_description_editor
from src.ui.components.analyses import render_analysis_card
from src.services.history_service import get_history_data
from src.services.dataset_service import (
    get_active_dataset_metadata,
    get_backup_dataset_metadata,
)
from src.workers.manager import list_analyses_for_version


def start_new_analysis() -> None:
    update_state(analysis_settings=None)
    st.query_params["create"] = "true"
    st.query_params["step"] = "1"


def history() -> None:
    state = get_state()

    if not state.factor_list_uid:
        st.warning(
            "No Factor List selected. Please select a Factor List to view analysis history."
        )
        return

    data = get_history_data()

    if not data:
        st.info("No past analysis found for this Factor List.")
        return

    selected_ver = st.selectbox(
        "Datasets",
        data,
        index=0,  # "active" is first when dataset loaded
        placeholder="Select a dataset",
        key="selected_dataset_ver",
        format_func=format_dataset_option,
    )
    print(selected_ver + " VERSION")
    is_viewing_live = selected_ver == "active"
    print(is_viewing_live, "LIVE")
    update_state(is_viewing_live_dataset=is_viewing_live)

    try:
        selected_dataset_metadata = (
            get_active_dataset_metadata()
            if is_viewing_live
            else get_backup_dataset_metadata(state.factor_list_uid, selected_ver)
        )
    except Exception as e:
        st.error(f"Failed to load dataset metadata: {e}")
        return

    if not is_viewing_live:
        st.info("You can only create a new analysis with your latest dataset")

    selected_analyses = list_analyses_for_version(
        state.factor_list_uid,
        (
            state.active_backup_version
            if is_viewing_live and state.active_backup_version
            else selected_ver
        ),
    )

    render_description_editor(selected_dataset_metadata.description, selected_ver)

    render_dataset_card(selected_dataset_metadata)

    # let users create new analysis if they have selected the active dataset
    if is_viewing_live:
        cols = st.columns([5, 1])
        with cols[1]:
            st.button(
                "New Analysis",
                type="primary",
                key="new_analysis_btn",
                width="stretch",
                on_click=start_new_analysis,
            )

    if not selected_analyses:
        st.html(
            "<p style='text-align: center; color: gray;'>No analyses yet for this dataset version</p>"
        )
        return

    section_header("Past Analyses")
    for analysis in selected_analyses:
        render_analysis_card(analysis)
