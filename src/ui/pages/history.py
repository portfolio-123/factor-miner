import streamlit as st
from functools import partial

from src.core.utils import format_dataset_option
from src.core.context import get_state, reset_analysis_state, update_state
from src.ui.components import (
    render_dataset_header,
    render_job_card,
    section_header,
)
from src.ui.dialogs import show_edit_dialog
from src.services.readers import get_history_page_data


def render() -> None:
    state = get_state()
    fl_id = state.factor_list_uid

    if not fl_id:
        st.warning(
            "No Factor List selected. Please select a Factor List to view analysis history."
        )
        return

    active_version, versions, version_metadata, jobs_by_version = get_history_page_data(
        fl_id, state.dataset_path
    )

    col1, col2 = st.columns([6, 1], vertical_alignment="bottom")

    with col1:
        selected_ver = st.selectbox(
            "Datasets",
            versions,
            index=versions.index(active_version) if active_version in versions else 0,
            placeholder="Select a dataset",
            format_func=partial(
                format_dataset_option,
                ds_info_map=version_metadata,
                current_version=active_version,
            ),
        )

    with col2:
        if st.button(
            "Edit Details",
            type="primary",
            width="stretch",
            disabled=not selected_ver,
            key="toggle_edit_ds",
        ):
            update_state(edit_dataset_mode=True, formulas_ds_ver=None)

    if state.edit_dataset_mode and selected_ver:
        show_edit_dialog(selected_ver, version_metadata, state.dataset_path)

    if not selected_ver:
        st.info("No past analysis found for this Factor List.")
        return

    selected_jobs = jobs_by_version.get(selected_ver, [])
    selected_info = version_metadata.get(selected_ver)

    if selected_info:
        st.markdown(f"**Description:** {selected_info.description or 'No description provided'}")

    render_dataset_header(selected_info, selected_ver)

    if selected_ver == active_version:
        cols = st.columns([5, 1])
        with cols[1]:
            st.button(
                "New Analysis",
                type="primary",
                key="new_analysis_btn",
                use_container_width=True,
                on_click=reset_analysis_state,
            )

        if not selected_jobs:
            st.caption("No analyses yet for this dataset version")
            return

        section_header("Past Analyses")
        for job in selected_jobs:
            render_job_card(job)
