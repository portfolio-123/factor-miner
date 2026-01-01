import streamlit as st
from dotenv import load_dotenv

from src.core.auth import authenticate_user
from src.core.init import init_state
from src.ui.components import render_page_header
from src.ui.styles import load_global_css
from src.ui.router import render_content

load_dotenv()

st.set_page_config(
    page_title="Factor Miner - Portfolio123",
    page_icon="assets/favicon.png",
    layout="wide",
)


def main() -> None:
    init_state()
    authenticate_user()
    load_global_css()

    render_page_header()
    render_content()


if __name__ == "__main__":
    main()
