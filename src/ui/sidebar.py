import streamlit as st

from src.core.config.environment import INTERNAL_MODE
from src.internal.links import p123_link
from src.internal.sidebar import list_user_datasets
from src.services.dataset_service import DatasetService
from src.ui.pages.about import about
from src.ui.pages.history import history
from src.ui.pages.create import create_form
from src.ui.pages.results import results


def sidebar() -> st.navigation:
    fl_id = st.query_params.get("fl_id")

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
        st.markdown(
            "<h1 style='padding: 0; margin: 0;'>FactorMiner</h1>",
            unsafe_allow_html=True,
        )

        if INTERNAL_MODE:
            fl_name = st.session_state.get("fl_name", "Dataset")
            link = p123_link(fl_id)
            display_name = f"{fl_name} ({fl_id})" if fl_id else fl_name
            header = (
                f"<a href='{link}' target='_blank' style='text-decoration: underline;'>{display_name}</a>"
                if link
                else f"<span style='color: #666;'>{display_name}</span>"
            )
            st.markdown(header, unsafe_allow_html=True)

            user_uid = st.session_state.get("user_uid")
            datasets = list_user_datasets(user_uid) if user_uid else []
            options = [fl_id for fl_id, _ in datasets]
            name_map = {fl_id: name for fl_id, name in datasets}
            label = "Factor Lists"
            format_func = lambda x: (
                f"{name_map.get(x)} ({x})"
                if name_map.get(x) and str(name_map.get(x)) != str(x)
                else str(x)
            )
        else:
            options = DatasetService.list_datasets()
            label = "Datasets"
            format_func = str

        if options:
            try:
                current_index = options.index(fl_id or "")
            except ValueError:
                current_index = 0

            selected = st.selectbox(
                label, options=options, index=current_index, format_func=format_func
            )

            if selected and selected != fl_id:
                # update fl_name
                if INTERNAL_MODE:
                    st.session_state.fl_name = name_map.get(selected, selected)
                else:
                    st.session_state.fl_name = selected

                # navigate
                if "id" in st.query_params:
                    st.switch_page(history_page, query_params={"fl_id": selected})
                else:
                    st.query_params["fl_id"] = selected
                    st.rerun()
        elif not INTERNAL_MODE:
            st.warning("No datasets found. Place .parquet files in the data directory.")

        st.session_state["pages"] = new_pages

        st.markdown(
            "<hr style='margin: 0.5rem 0;'>",
            unsafe_allow_html=True,
        )

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
