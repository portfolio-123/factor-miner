import streamlit as st
from dotenv import load_dotenv

from src.core.init import init
from pages.history import history
from pages.create import create_form
from pages.results import results
from src.ui.components.common import spacer
load_dotenv()

st.set_page_config(
    page_title="Factor Miner - Portfolio123",
    page_icon="assets/favicon.png",
    layout="wide",
)


init()
# login()

history_page = st.Page(history, title="Your Results", icon=":material/list:", default=True)
create_page = st.Page(create_form, title="New Analysis", icon=":material/add:", url_path="create")
results_page = st.Page(results, title="Results", url_path="results")

pg = st.navigation([history_page, create_page, results_page], position="hidden")


fl_id = st.query_params.get("fl_id")
with st.sidebar:
    st.markdown("<h1 style='padding: 0; margin: 0;'>Factor Miner</h1>", unsafe_allow_html=True)
    st.markdown(f"<h4 style='padding: 0; margin: 0;'>Factor List: {fl_id}</h4>", unsafe_allow_html=True)
    spacer(1)
    st.divider()
    spacer(1)
    st.page_link(history_page, label="Your Results", icon=":material/analytics:", query_params={"fl_id": fl_id})
    st.page_link(create_page, label="New Analysis", icon=":material/add:", query_params={"fl_id": fl_id})

pg.run()
