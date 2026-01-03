import os
import re

import streamlit as st

from src.core.context import get_state, update_state, clear_debug_logs
from src.services.readers import get_current_dataset_info


def header_back() -> None:
    col_back, _ = st.columns([1, 11])
    with col_back:
        if st.button("Back", type="secondary", key="back_btn"):
            update_state(page="history", current_job_id=None)
            st.rerun()


def header_history() -> None:
    state = get_state()

    col_brand, _, col_logs = st.columns([2.5, 4.5, 0.8])

    with col_brand:
        st.markdown(
            """
            <div style="padding: 5px 0; display: flex; flex-direction: column;">
                <span style="font-size: 24px; font-weight: 700; color: #333;">Portfolio123</span>
                <span style="font-size: 16px; font-weight: 400; color: #666;">Factor Evaluator</span>
            </div>
        """,
            unsafe_allow_html=True,
        )

    with col_logs:
        if st.button("Logs", key="debug_btn", width="stretch"):
            update_state(show_debug_modal=True)

    if state.show_debug_modal:
        _show_debug_modal()


def header_analysis() -> None:
    """Analysis page header. Shows step nav only for new analyses."""
    state = get_state()

    # Viewing existing job: just show back button
    if state.current_job_id:
        header_back()
        return

    # New analysis flow: back + steps + logs
    col_back, col_nav, col_logs = st.columns([1, 2, 1], vertical_alignment="center")

    with col_back:
        with st.columns([1, 1])[0]:
            if st.button("Back", type="secondary", key="back_btn_analysis"):
                update_state(page="history", current_job_id=None)
                st.rerun()

    with col_nav:
        btn_cols = st.columns([1, 1])
        nav_steps = [(1, "Settings"), (2, "Review")]

        for i, (step_num, step_name) in enumerate(nav_steps):
            is_available = step_num == 1 or state.config_completed

            with btn_cols[i]:
                btn_type = "primary" if step_num == state.current_step else "secondary"
                st.button(
                    step_name,
                    key=f"step_btn_{step_num}",
                    type=btn_type,
                    disabled=not is_available,
                    use_container_width=True,
                    on_click=update_state if is_available else None,
                    kwargs={"current_step": step_num} if is_available else None,
                )

    with col_logs:
        with st.columns([1, 1])[0]:
            if st.button(
                "Logs",
                key="debug_btn_analysis",
                use_container_width=True,
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

    st.markdown(html_code, unsafe_allow_html=True)


def render_page_header() -> None:
    state = get_state()
    fl_id = state.factor_list_uid
    base_url = os.getenv("P123_BASE_URL")
    steps = [
        ("Factor List", f"{base_url}/sv/factorList/{fl_id}/download"),
        ("FactorMiner", None),
    ]

    _, dataset_info = get_current_dataset_info(state.dataset_path)

    render_breadcrumb(steps)
    st.title(f"{dataset_info.flName if dataset_info else 'Unknown'} ({fl_id})")


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
            st.markdown(
                f'<div style="font-family: monospace; font-size: 13px; '
                f"background: #f5f5f5; color: #333; padding: 12px; "
                f"border-radius: 4px; max-height: 400px; overflow-y: auto; "
                f'margin-bottom: 16px;">'
                f"{log_html}</div>",
                unsafe_allow_html=True,
            )

        _, col1 = st.columns([6, 1])
        with col1:
            if st.button(
                "Clear Logs", key="modal_clear_logs", width="stretch", type="primary"
            ):
                clear_debug_logs()
                st.rerun()

    _logs_content()
