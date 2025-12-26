import streamlit as st
from src.core.context import get_state
from src.ui.components import show_formulas_modal, render_dataset_history_card
from src.services.readers import get_current_dataset_info
from src.workers.manager import (
    get_dataset_info_from_backup,
    get_formulas_df_for_version,
    get_grouped_jobs,
    sort_dataset_versions,
)


def render() -> None:
    state = get_state()
    fl_id = state.factor_list_uid

    if not fl_id:
        st.warning(
            "No Factor List selected. Please select a Factor List to view analysis history."
        )
        return

    st.markdown(
        "<div style='font-size:24px;font-weight:700;color:#212529;margin:0 0 16px 0;padding:0;'>"
        "Factor Evaluator - Analysis History"
        "</div>",
        unsafe_allow_html=True,
    )
    current_version, current_dataset_info = get_current_dataset_info(fl_id)
    jobs, grouped_jobs = get_grouped_jobs(fl_id)

    current_version_has_jobs = current_version and current_version in grouped_jobs

    sorted_datasets = sort_dataset_versions(list(grouped_jobs.keys()))

    # render current dataset card if it doesn't have jobs in it yet. if it does, it will be rendered in the loop below
    if current_version and current_dataset_info and not current_version_has_jobs:
        render_dataset_history_card(
            dataset_info=current_dataset_info,
            ds_ver=current_version,
            jobs=[],
            fl_id=fl_id,
            is_current=True,
        )

    # render existing dataset cards
    for ds_ver in sorted_datasets:
        ds_jobs = grouped_jobs[ds_ver]
        is_current_version = ds_ver == current_version

        render_dataset_history_card(
            dataset_info=current_dataset_info if is_current_version else get_dataset_info_from_backup(fl_id, ds_ver),
            ds_ver=ds_ver,
            jobs=ds_jobs,
            fl_id=fl_id,
            is_current=is_current_version,
        )

    if not jobs and not current_dataset_info:
        st.info("No past analysis found for this Factor List.")

    formulas_ds_ver = st.session_state.get("formulas_ds_ver")
    if formulas_ds_ver:
        formulas_df = get_formulas_df_for_version(fl_id, formulas_ds_ver)
        if formulas_df is not None:
             show_formulas_modal(formulas_df)
        else:
            st.session_state.formulas_ds_ver = None
