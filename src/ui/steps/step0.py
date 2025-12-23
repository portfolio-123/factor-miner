import streamlit as st
from src.core.context import get_state
from src.core.utils import locate_factor_list_file
from src.core.job_restore import restore_job_state
from src.ui.components import show_formulas_modal, render_dataset_history_card
from src.services.readers import ParquetDataReader, get_current_dataset_info
from src.workers.manager import (
    get_dataset_info_from_backup,
    get_dataset_formulas_from_backup,
    get_grouped_jobs,
    sort_dataset_versions,
)


def _handle_job_click(job_id: str) -> None:
    if not restore_job_state(job_id):
        st.session_state["_job_restore_error"] = job_id


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

    if "select_job_id" in st.query_params:
        job_id = st.query_params["select_job_id"]
        del st.query_params["select_job_id"]
        _handle_job_click(job_id)
        st.rerun()

    if "view_formulas_ds_ver" in st.query_params:
        ds_ver = st.query_params["view_formulas_ds_ver"]
        del st.query_params["view_formulas_ds_ver"]
        st.session_state.show_formulas_modal = True
        st.session_state.formulas_fl_id = fl_id
        st.session_state.formulas_ds_ver = ds_ver
        st.rerun()

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

        if is_current_version and current_dataset_info:
            dataset_info = current_dataset_info
        else:
            dataset_info = get_dataset_info_from_backup(fl_id, ds_ver)

        if dataset_info:
            render_dataset_history_card(
                dataset_info=dataset_info,
                ds_ver=ds_ver,
                jobs=ds_jobs,
                fl_id=fl_id,
                is_current=is_current_version,
            )

    if not jobs and not current_dataset_info:
        st.info("No past analysis found for this Factor List.")

    if st.session_state.get("show_formulas_modal"):
        formulas_fl_id = st.session_state.get("formulas_fl_id")
        formulas_ds_ver = st.session_state.get("formulas_ds_ver")
        if formulas_fl_id and formulas_ds_ver:
            try:
                is_current = formulas_ds_ver == current_version
                if is_current:
                    dataset_path = locate_factor_list_file(formulas_fl_id)
                    reader = ParquetDataReader(dataset_path)
                    formulas_df = reader.get_formulas_df()
                else:
                    formulas_df = get_dataset_formulas_from_backup(formulas_fl_id, formulas_ds_ver)
                show_formulas_modal(formulas_df)
            except Exception:
                st.session_state.show_formulas_modal = False
