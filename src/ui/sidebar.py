import streamlit as st

from src.ui.components.common import spacer


def render_sidebar() -> None:
    fl_id = st.query_params.get("fl_id")
    pages = st.session_state.get("pages", {})

    with st.sidebar:
        fl_name = st.session_state.get("fl_name", fl_id)
        st.markdown(
            "<h1 style='padding: 0; margin: 0;'>Factor Miner</h1>",
            unsafe_allow_html=True,
        )
        st.markdown(
            f"<h4 style='padding: 0; margin: 0;'>{fl_name} ({fl_id})</h4>",
            unsafe_allow_html=True,
        )
        spacer(1)
        st.divider()
        spacer(1)

        if history_page := pages.get("history"):
            st.page_link(
                history_page,
                label="Your Results",
                icon=":material/analytics:",
                query_params={"fl_id": fl_id},
            )
        if create_page := pages.get("create"):
            st.page_link(
                create_page,
                label="New Analysis",
                icon=":material/add:",
                query_params={"fl_id": fl_id},
            )
