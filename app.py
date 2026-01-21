import streamlit as st
from dotenv import load_dotenv

from src.core.auth import login
from src.core.init import init
from src.ui.components.headers import render_page_header
from src.ui.pages.history import history
from src.ui.pages.create import create_form
from src.ui.pages.results import results

load_dotenv()

st.set_page_config(
    page_title="Factor Miner - Portfolio123",
    page_icon="assets/favicon.png",
    layout="wide",
)

# Initialize fl_id from URL or existing session state
initial_fl_id = st.query_params.get("fl_id") or st.session_state.get("fl_id", "")

# Visible widget that persists fl_id across pages
# Place in sidebar so it's always visible
with st.sidebar:
    fl_id = st.text_input(
        "Factor List ID",
        value=initial_fl_id,
        key="fl_id",
        disabled=True,  # Read-only display
    )

# Validate fl_id exists
if not fl_id:
    st.error("No Factor List ID provided in URL.")
    st.stop()

# Keep URL in sync for shareability
if st.query_params.get("fl_id") != fl_id:
    st.query_params["fl_id"] = fl_id

# Continue with init and navigation
init()
# login()

render_page_header()

# Handle Results page separately (not in sidebar)
if analysis_id := st.query_params.get("analysis_id"):
    results(analysis_id)
    st.stop()

# Define pages with grouped navigation
pages = {
    "Dashboard": [
        st.Page(history, title="History", icon=":material/list:", default=True),
    ],
    "Create": [
        st.Page(create_form, title="New Analysis", icon=":material/add:"),
    ],
}

pg = st.navigation(pages)
pg.run()
