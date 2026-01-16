import streamlit as st
from functools import partial

from src.core.utils import format_dataset_option
from src.core.context import get_state, reset_analysis_state, update_state
from src.core.types import DatasetConfig
from src.ui.components import (
    render_dataset_info_row,
    render_analysis_card,
    section_header,
    spacer,
)
from src.ui.dialogs import show_edit_dialog
from src.services.parquet_utils import get_file_version, get_dataset_metadata
from src.services.analysis_utils import group_analyses_by_version, sort_dataset_versions
from src.workers.manager import list_analyses


def render() -> None:
    state = get_state()
    fl_id = state.factor_list_uid

    if not fl_id:
        st.warning(
            "No Factor List selected. Please select a Factor List to view analysis history."
        )
        return

    analyses_by_version = group_analyses_by_version(list_analyses(fl_id))

    all_versions = set(analyses_by_version.keys())
    active_version = get_file_version(state.dataset_path) if state.dataset_path else None
    if active_version:
        all_versions.add(active_version)

    versions = sort_dataset_versions(list(all_versions))

    if not versions:
        st.info("No past analysis found for this Factor List.")
        return

    col1, col2 = st.columns([6, 1], vertical_alignment="bottom")

    with col1:
        selected_ver = st.selectbox(
            "Datasets",
            versions,
            index=versions.index(active_version) if active_version in versions else 0,
            placeholder="Select a dataset",
            key="selected_dataset_ver",
            format_func=partial(format_dataset_option, active_version=active_version),
        )

    with col2:
        if st.button(
            "Edit Details",
            type="primary",
            width="stretch",
            key="toggle_edit_ds",
        ):
            update_state(edit_dataset_mode=True, formulas_ds_ver=None)

    selected_analyses = analyses_by_version.get(selected_ver, [])
    selected_info = get_dataset_metadata(fl_id, selected_ver, state.dataset_path)

    if state.edit_dataset_mode:
        show_edit_dialog(selected_info)

    if selected_info:
        st.markdown(f"**Description:** {selected_info.description or 'No description provided'}")

    with st.container(border=True):
        render_dataset_info_row(selected_info or DatasetConfig(), selected_ver)
        spacer(8)

    if selected_ver == active_version:
        cols = st.columns([5, 1])
        with cols[1]:
            st.button(
                "New Analysis",
                type="primary",
                key="new_analysis_btn",
                width="stretch",
                on_click=reset_analysis_state,
            )
    else:
        st.info("You can only create a new analysis with your latest dataset")

    if not selected_analyses:
        st.markdown(
            "<p style='text-align: center; color: gray;'>No analyses yet for this dataset version</p>",
            unsafe_allow_html=True,
        )
        return

    section_header("Past Analyses")
    for analysis in selected_analyses:
        render_analysis_card(analysis)
