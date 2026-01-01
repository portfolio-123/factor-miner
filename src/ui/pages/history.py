import streamlit as st
from src.core.utils import format_timestamp
from src.core.context import get_state, reset_analysis_state, update_state
from src.ui.components import (
    show_formulas_modal,
    render_dataset_history_card,
    render_job_card,
)
from src.services.readers import get_current_dataset_info
from src.workers.manager import (
    get_dataset_info_from_backup,
    get_formulas_df_for_version,
    get_grouped_jobs,
    sort_dataset_versions,
    update_dataset_info,
)


def render() -> None:
    state = get_state()
    fl_id = state.factor_list_uid

    if not fl_id:
        st.warning(
            "No Factor List selected. Please select a Factor List to view analysis history."
        )
        return

    current_version, current_dataset_info = get_current_dataset_info(state.dataset_path)
    jobs, grouped_jobs = get_grouped_jobs(fl_id)

    current_version_has_jobs = current_version and current_version in grouped_jobs

    all_versions = set(grouped_jobs.keys())
    if current_version:
        all_versions.add(current_version)

    sorted_datasets = sort_dataset_versions(list(all_versions))

    ds_info_map = {}
    if current_version and current_dataset_info:
        ds_info_map[current_version] = current_dataset_info

    for ver in sorted_datasets:
        if ver not in ds_info_map:
            info = get_dataset_info_from_backup(fl_id, ver)
            if info:
                ds_info_map[ver] = info

    def _format_dataset_option(ver: str) -> str:
        info = ds_info_map.get(ver)
        timestamp_str = format_timestamp(ver)

        name = "Unnamed"
        if info and info.name:
            name = info.name

        if ver == current_version:
            return f"🟢​ [READY] {name} - {timestamp_str}"

        return f"{name} - {timestamp_str}"

    default_index = None
    if current_version and current_version in sorted_datasets:
        default_index = sorted_datasets.index(current_version)
    elif sorted_datasets:
        default_index = 0

    col1, col2 = st.columns([6, 1], vertical_alignment="bottom")

    with col1:
        selected_ver = st.selectbox(
            "Datasets",
            sorted_datasets,
            index=default_index,
            placeholder="Select a dataset",
            format_func=_format_dataset_option,
        )

    # Check formulas_ds_ver first - if it's set, formulas modal takes priority
    should_show_formulas = state.formulas_ds_ver is not None

    # Reset edit mode if formulas modal should be shown
    if should_show_formulas:
        update_state(edit_dataset_mode=False)

    with col2:
        if st.button(
            "Edit Details",
            type="primary",
            width="stretch",
            disabled=not selected_ver,
            key="toggle_edit_ds",
        ):
            update_state(edit_dataset_mode=True, formulas_ds_ver=None)
            should_show_formulas = False

    # Only show edit dialog if formulas modal is not being shown
    if state.edit_dataset_mode and selected_ver and not should_show_formulas:

        @st.dialog("Edit Dataset Details", width="large")
        def edit_dialog():
            # Reset flag immediately - dialog is already open, so dismissing it won't retrigger
            update_state(edit_dataset_mode=False)

            curr_info = ds_info_map.get(selected_ver)
            curr_name = curr_info.name if curr_info else ""
            curr_desc = curr_info.description if curr_info else ""

            with st.form(key="edit_dataset_form", border=False):
                new_name = st.text_input(
                    "Name", value=curr_name, placeholder="Enter dataset name"
                )
                new_desc = st.text_area(
                    "Description",
                    value=curr_desc,
                    height=120,
                    placeholder="Enter dataset description",
                )

                col_cancel, col_save = st.columns([1, 1])
                with col_cancel:
                    cancelled = st.form_submit_button(
                        "Cancel", use_container_width=True
                    )
                with col_save:
                    submitted = st.form_submit_button(
                        "Save Changes", type="primary", use_container_width=True
                    )

                if cancelled:
                    st.rerun()
                elif submitted:
                    success = update_dataset_info(
                        state.dataset_path, selected_ver, {"name": new_name, "description": new_desc}
                    )
                    if success:
                        st.success("Updated!")
                        st.rerun()
                    else:
                        st.error("Failed to update.")

        edit_dialog()

    if selected_ver:
        ds_jobs = grouped_jobs.get(selected_ver, [])
        is_current_version = selected_ver == current_version

        info = ds_info_map.get(selected_ver)

        if info:
            description = (
                info.description if info.description else "No description provided"
            )
            st.markdown(f"**Description:** {description}")

        render_dataset_history_card(
            dataset_info=info,
            ds_ver=selected_ver,
            jobs=ds_jobs,
            fl_id=fl_id,
        )

        if is_current_version:
            # View Factors is now in the dataset card, so just show New Analysis button
            col_spacer, col_new = st.columns([5, 1])
            with col_spacer:
                st.empty()
            with col_new:
                st.button(
                    "New Analysis",
                    type="primary",
                    key="new_analysis_btn" if ds_jobs else "new_analysis_btn_empty",
                    use_container_width=True,
                    on_click=reset_analysis_state,
                )

            # Show title if there are jobs
            if ds_jobs:
                st.markdown(
                    "<div style='font-size: 15px; font-weight: 400; color: #60646A; padding-top: 16px;'>PAST ANALYSES</div>",
                    unsafe_allow_html=True,
                )

            # Render job cards
            for job in ds_jobs:
                render_job_card(job)

            # Show "No analyses yet" message at bottom if no jobs
            if not ds_jobs:
                st.markdown(
                    "<div style='text-align: center; font-size: 14px; color: #9ca3af; font-style: italic; margin-top: 2rem; padding-top: 4px;'>No analyses yet for this dataset version</div>",
                    unsafe_allow_html=True,
                )
    elif not jobs and not current_dataset_info:
        st.info("No past analysis found for this Factor List.")

    # Show formulas modal if requested - recalculate to ensure we have latest state
    if state.formulas_ds_ver:
        formulas_df = get_formulas_df_for_version(fl_id, state.formulas_ds_ver)
        if formulas_df is not None:
            show_formulas_modal(formulas_df)
        else:
            update_state(formulas_ds_ver=None)
