import re

import streamlit as st

from src.core.environment import P123_BASE_URL
from src.core.context import get_state, update_state, clear_debug_logs, sync_url_for_history, sync_url_for_new_analysis


def _navigate_to_step(step: int) -> None:
    update_state(current_step=step)
    sync_url_for_new_analysis(step)


def header_back() -> None:
    col_back, _ = st.columns([1, 11])
    with col_back:
        if st.button("Back", type="secondary", key="back_btn"):
            update_state(page="history", current_analysis_id=None)
            sync_url_for_history()
            st.rerun()


def header_analysis() -> None:
    state = get_state()

    # Viewing existing analysis: just show back button
    if state.current_analysis_id:
        header_back()
        return

    # New analysis flow: back + steps + logs
    col_back, col_nav, col_logs = st.columns([1, 2, 1], vertical_alignment="center")

    with col_back:
        with st.columns([1, 1])[0]:
            if st.button("Back", type="secondary", key="back_btn_analysis"):
                update_state(page="history", current_analysis_id=None)
                sync_url_for_history()
                st.rerun()

    with col_nav:
        btn_cols = st.columns([1, 1])
        nav_steps = [(1, "Settings"), (2, "Review")]

        for i, (step_num, step_name) in enumerate(nav_steps):
            is_available = step_num == 1 or state.config_completed
            is_current = step_num == state.current_step

            with btn_cols[i]:
                btn_type = "primary" if is_current else "secondary"
                st.button(
                    step_name,
                    key=f"step_btn_{step_num}",
                    type=btn_type,
                    disabled=not is_available,
                    width="stretch",
                    on_click=_navigate_to_step if is_available else None,
                    kwargs={"step": step_num} if is_available else None,
                )

    with col_logs:
        with st.columns([1, 1])[1]:
            if st.button(
                "Logs",
                key="debug_btn_analysis",
                width="stretch",
                type="primary",
            ):
                update_state(show_debug_modal=True)

    if state.show_debug_modal:
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
    fl_id = state.factor_list_uid
    steps = [
        ("Factor List", f"{P123_BASE_URL}/sv/factorList/{fl_id}/download"),
        ("FactorMiner", None),
    ]

    fl_name = state.fl_name or "Unknown"

    render_breadcrumb(steps)
    st.title(f"{fl_name} ({fl_id})")


@st.dialog("Debug Logs", width="large")
def _show_debug_modal():
    update_state(show_debug_modal=False)

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

        _, col1 = st.columns([6, 1])
        with col1:
            if st.button(
                "Clear Logs", key="modal_clear_logs", width="stretch", type="primary"
            ):
                clear_debug_logs()
                st.rerun()

    _logs_content()
