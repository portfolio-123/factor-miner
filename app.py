import streamlit as st
from dotenv import load_dotenv

from src.core.auth import login
from src.core.init import init
from src.ui.navigation import setup_navigation
from src.ui.sidebar import render_sidebar

load_dotenv()

st.set_page_config(
    page_title="Factor Miner - Portfolio123",
    page_icon="assets/favicon.png",
    layout="wide",
)

init()
# login()

pg = setup_navigation()
render_sidebar()
pg.run()
