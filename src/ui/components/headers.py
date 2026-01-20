import re

import streamlit as st

from src.core.environment import P123_BASE_URL
from src.core.context import get_state, update_state, clear_debug_logs


def navbar(show_steps: bool = False, show_logs: bool = False) -> None:
    state = get_state()
    col_back, col_nav, col_logs = st.columns([1, 2, 1], vertical_alignment="center")

    with col_back:
        if st.button("Back", type="secondary", key="back_btn"):
            update_state(analysis_id=None)
            st.query_params.from_dict({"fl_id": state.factor_list_uid})
            st.rerun()

    if show_steps:
        with col_nav:
            btn_cols = st.columns([1, 1])
            for i, (step_num, step_name) in enumerate([(1, "Settings"), (2, "Review")]):
                is_available = step_num == 1 or state.analysis_settings is not None
                with btn_cols[i]:
                    st.button(
                        step_name,
                        key=f"step_btn_{step_num}",
                        type="primary" if step_num == int(st.query_params.get("step", 1)) else "secondary",
                        disabled=not is_available,
                        width="stretch",
                        on_click=lambda s=step_num: st.query_params.update(step=str(s)),
                    )

    if show_logs:
        with col_logs:
            if st.button("Logs", key="debug_btn", type="primary"):
                _show_debug_modal()


def render_breadcrumb(steps: list[tuple[str, str | None]]) -> None:
    html_code = """
    <div class="breadcrumb">
    """

    for i, (label, link) in enumerate(steps):
        if i > 0:
            html_code += " &gt; "

        if link:
            html_code += f"<a href='{link}' target='_blank'>{label}</a>"
        else:
            html_code += f"<span>{label}</span>"

    html_code += "</div>"

    st.html(html_code)


def render_page_header() -> None:
    state = get_state()
    steps = [
        ("Factor List", f"{P123_BASE_URL}/sv/factorList/{state.factor_list_uid}/download"),
        ("FactorMiner", None),
    ]

    render_breadcrumb(steps)
    st.title(f"{state.fl_name} ({state.factor_list_uid})")


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
