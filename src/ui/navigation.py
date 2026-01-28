import streamlit as st

from src.ui.pages.about import about
from src.ui.pages.history import history
from src.ui.pages.create import create_form
from src.ui.pages.results import results


def navigation() -> st.navigation:
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

    return st.navigation(
        [history_page, create_page, results_page, about_page], position="hidden"
    )
