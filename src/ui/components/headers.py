import re

import streamlit as st

from src.core.environment import P123_BASE_URL
from src.core.context import get_state, update_state, clear_debug_logs


def navbar(show_logs: bool = False, show_back_button: bool = False) -> None:
    state = get_state()
    col_back, col_nav, col_logs = st.columns([1, 2, 1], vertical_alignment="center")

    if show_back_button:
        with col_back:
            if st.button("Back", type="secondary", key="back_btn"):
                update_state(analysis_id=None)
                st.query_params.from_dict({"fl_id": state.factor_list_uid})
                st.rerun()

    if show_logs:
        with col_logs:
            _, logs_btn_col = st.columns([2, 1])
            with logs_btn_col:
                if st.button("Logs", key="debug_btn", type="primary"):
                    _show_debug_modal()

@st.dialog("Debug Logs", width="large")
def _show_debug_modal():

    @st.fragment
    def _logs_content():
        state = get_state()

        if state.debug_logs:
            log_lines = []
            for log in state.debug_logs[-100:]:
                formatted = re.sub(
                    r"(\[.*?\])", r'<span style="color: #2196F3;">\1</span>', log
                )
                log_lines.append(formatted)
            log_html = "<br>".join(log_lines)
            st.html(
                f'<div style="font-family: monospace; font-size: 13px; '
                f"background: #f5f5f5; color: #333; padding: 12px; "
                f"border-radius: 4px; max-height: 400px; overflow-y: auto; "
                f'margin-bottom: 16px;">'
                f"{log_html}</div>"
            )

        with st.columns([6, 1])[1]:
            if st.button(
                "Clear Logs", key="modal_clear_logs", width="stretch", type="primary"
            ):
                clear_debug_logs()
                st.rerun()

    _logs_content()
