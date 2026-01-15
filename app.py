import streamlit as st
from dotenv import load_dotenv

from src.core.auth import login
from src.core.init import init
from src.ui.components import render_page_header
from src.ui.router import render_content

load_dotenv()

st.set_page_config(
    page_title="Factor Miner - Portfolio123",
    page_icon="assets/favicon.png",
    layout="wide",
)


init()
login()

render_page_header()
render_content()
