import streamlit as st

from src.core.config.environment import INTERNAL_MODE
from src.core.types.models import DatasetDetails
from src.internal.links import p123_link
from src.internal.sidebar import list_user_datasets
from src.services.dataset_service import DatasetService
from src.ui.pages.about import about
from src.ui.pages.history import history
from src.ui.pages.create import create_form
from src.ui.pages.results import results


def sidebar() -> st.navigation:
    fl_id = st.query_params.get("fl_id")

    # get available datasets first to determine default fl_id
    if INTERNAL_MODE:
        name_map = list_user_datasets(st.session_state.get("user_uid"))
        options = list(name_map)
    else:
        name_map = {}
        options = DatasetService.list_datasets()

    # set default fl_id if not provided
    if not fl_id and options:
        fl_id = options[0]
        st.query_params["fl_id"] = fl_id

    # set fl_name in session state
    if fl_id:
        st.session_state.fl_name = name_map.get(fl_id) or fl_id

    if fl_id and (
        "dataset_details" not in st.session_state
        or st.session_state.dataset_details.fl_id != fl_id
    ):
        st.session_state.dataset_details = DatasetDetails(
            fl_id=fl_id, user_uid=st.session_state.get("user_uid")
        )

    history_page = st.Page(
        history, title="Your Results", icon=":material/list:", default=True
    )
    create_page = st.Page(
        create_form, title="New Analysis", icon=":material/add:", url_path="create"
    )
    results_page = st.Page(results, title="Results", url_path="results")
    about_page = st.Page(about, title="About", icon=":material/info:", url_path="about")

    new_pages = {
        "history": history_page,
        "create": create_page,
        "results": results_page,
        "about": about_page,
    }

    with st.sidebar:
        st.html("<h1 style='padding: 0; margin: 0;'>FactorMiner</h1>")

        if options:
            current_index = options.index(fl_id) if fl_id in options else 0

            selected = st.selectbox(
                "Datasets",
                options=options,
                index=current_index,
                format_func=lambda v: name_map.get(v) or v,
            )

            if selected and selected != fl_id:
                st.session_state.fl_name = name_map.get(selected) or selected
                if (
                    "id" in st.query_params
                ):  # if in results page, switch to history page
                    st.switch_page(history_page, query_params={"fl_id": selected})
                else:
                    st.query_params["fl_id"] = selected
                    st.rerun()
        elif not INTERNAL_MODE:
            st.warning("No datasets found. Place .parquet files in the data directory.")

        st.session_state["pages"] = new_pages

        st.html("<hr style='margin: 0.5rem 0;'>")

        st.page_link(
            history_page,
            label="Your Results",
            icon=":material/analytics:",
            query_params={"fl_id": fl_id},
        )
        st.page_link(
            create_page,
            label="New Analysis",
            icon=":material/add:",
            query_params={"fl_id": fl_id},
        )
        st.page_link(
            about_page,
            label="About",
            icon=":material/info:",
            query_params={"fl_id": fl_id},
        )

    return st.navigation(
        [history_page, create_page, results_page, about_page], position="hidden"
    )
