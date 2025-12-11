import streamlit as st
from src.core.context import get_state
from src.ui.steps.step0 import render as render_history
from src.ui.steps import render_step
from src.ui.components import header_with_navigation

def history_page():
    render_history()

def analysis_page():
    state = get_state()
    header_with_navigation()
    
    render_step(state.current_step)
