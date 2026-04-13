from collections.abc import Callable
from typing import Any

import streamlit as st

from src.core.config.environment import INTERNAL_MODE
from src.core.types.models import DatasetDetails
from src.internal.sidebar import list_user_datasets
from src.services.dataset_service import DatasetService
from src.ui.pages.about import about
from src.ui.pages.history import history
from src.ui.pages.create import create_form
from src.ui.pages.results import results


def sidebar():
    fl_id = st.query_params.get("fl_id")

    # get available datasets first to determine default fl_id
    if INTERNAL_MODE:
        name_map = list_user_datasets(st.session_state["user_uid"])
        get_name: Callable[[Any], str] = name_map.get  # type: ignore
        options = list(name_map)
    else:
        get_name: Callable[[Any], str] = lambda x: x
        options = DatasetService.list_datasets()

    # set default fl_id if not provided
    if not fl_id:
        if not options:
            st.warning("No datasets available. Please generate a dataset in Portfolio123.")
            st.stop()

        fl_id = options[0]
        st.query_params["fl_id"] = fl_id

    # set fl_name in session state
    st.session_state["fl_name"] = get_name(fl_id)

    dataset_details = st.session_state.get("dataset_details")
    if dataset_details is None or dataset_details.fl_id != fl_id:
        st.session_state["dataset_details"] = DatasetDetails(fl_id=fl_id, user_uid=st.session_state.get("user_uid"))

    history_page = st.Page(history, title="Your Results", icon=":material/list:", default=True)
    create_page = st.Page(create_form, title="New Analysis", icon=":material/add:", url_path="create")
    results_page = st.Page(results, title="Results", url_path="results")
    about_page = st.Page(about, title="About", icon=":material/info:", url_path="about")

    new_pages = {"history": history_page, "create": create_page, "results": results_page, "about": about_page}

    with st.sidebar:
        st.html("<h1 style='padding: 0; margin: 0;'>FactorMiner</h1>")

        if options:
            try:
                current_index = options.index(fl_id)
            except ValueError:
                current_index = 0

            selected = st.selectbox(
                "Datasets", options=options, index=current_index, format_func=get_name
            )  # why not use bind="query-params"?

            if selected and selected != fl_id:
                st.session_state["fl_name"] = get_name(selected)
                id = st.query_params.get("id")
                if id is not None:  # if in results page, switch to history page
                    st.switch_page(history_page, query_params=(("fl_id", selected),))
                else:
                    st.query_params["fl_id"] = selected
                    st.rerun()
        elif not INTERNAL_MODE:
            st.warning("No datasets found. Place .parquet files in the data directory.")

        st.session_state["pages"] = new_pages

        st.html("<hr style='margin: 0.5rem 0;'>")

        query_params = (("fl_id", fl_id),)
        st.page_link(history_page, label="Your Results", icon=":material/analytics:", query_params=query_params)
        st.page_link(create_page, label="New Analysis", icon=":material/add:", query_params=query_params)
        st.page_link(about_page, label="About", icon=":material/info:", query_params=query_params)

    return st.navigation([history_page, create_page, results_page, about_page], position="hidden")
