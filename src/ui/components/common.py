from collections.abc import Callable

import streamlit as st
from st_clipboard import copy_to_clipboard, copy_to_clipboard_unsecured

from src.core.utils.common import escape_html


def copy_download_buttons(
    render_csv_copy: Callable[[], str],
    render_csv_download: Callable[[], str],
    file_name: str,
    key_prefix: str,
    toast_msg="Copied to clipboard",
):
    """Render copy-to-clipboard and download CSV buttons."""

    def sanitize(text: str) -> str:
        return text.replace("−", "-")

    _, col1, col2 = st.columns([3, 1, 1])
    with col1:
        if st.button(type="primary", label="Copy to Clipboard", width="stretch", key=f"{key_prefix}_copy"):
            csv_copy = render_csv_copy()
            copy_to_clipboard_unsecured(csv_copy)
            copy_to_clipboard(csv_copy)
            st.toast(toast_msg)
    with col2:
        csv_data = sanitize(render_csv_download())

        st.download_button(
            type="primary",
            label="Download CSV",
            data=csv_data,
            file_name=file_name,
            mime="text/csv",
            width="stretch",
            key=f"{key_prefix}_download",
        )


def section_header(title: str):
    st.html(f"""
        <div style="font-size: 14px; font-weight: 600; color: #2196F3;
                    margin: 15px 0 8px 0; padding-bottom: 5px;
                    border-bottom: 2px solid #2196F3;">
            {title}
        </div>
    """)


def render_info_item(label: str, value: str | int | float, muted=False):
    value_color = "#6c757d" if muted else "#212529"
    return f"""<div style="display: flex; flex-direction: column;">
        <div style="font-size: 11px; color: #6c757d; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 4px;">{label}</div>
        <div style="font-size: 14px; font-weight: 500; color: {value_color};">{escape_html(value)}</div>
    </div>"""


def render_big_info_item(label: str, value: str | int | float):
    return f"""<div style="display: flex; flex-direction: column;">
        <div style="font-size: 12px; font-weight: 600; color: #2196F3; margin-bottom: 1px; text-transform: uppercase;">{label}</div>
        <div style="font-size: 18px; font-weight: 600; color: #1a1a1a; line-height: 1.2;">{escape_html(value)}</div>
    </div>"""


def render_section_label_html(title: str):
    return f'<div style="font-size: 14px; font-weight: 600; color: #212529; letter-spacing: 0.5px; margin-bottom: 4px;">{title}</div>'


def render_card_header_html(title: str):
    return f'<p style="font-size: 1rem; font-weight: 600; margin: 0 0 12px 0;">{title}</p>'


def spacer(height=16):
    st.html(f'<div style="height: {height}px;"></div>')
