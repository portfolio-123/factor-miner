import streamlit as st

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

    st.session_state["pages"] = {
        "history": history_page,
        "create": create_page,
        "results": results_page,
        "about": about_page,
    }

    with st.sidebar:
        fl_name = st.session_state.get("fl_name", "Factor List")
        st.markdown(
            "<h1 style='padding: 0; margin: 0;'>FactorMiner</h1>",
            unsafe_allow_html=True,
        )
        st.markdown(
            f"<h4 style='padding: 0; margin: 0;'>{fl_name} ({fl_id})</h4>",
            unsafe_allow_html=True,
        )

        st.divider()

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
