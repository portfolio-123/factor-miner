import streamlit as st
from st_clipboard import copy_to_clipboard, copy_to_clipboard_unsecured


def render_copy_download_buttons(
    csv_copy: str,
    csv_download: str,
    file_name: str,
    key_prefix: str,
    toast_msg: str = "Copied to clipboard",
) -> None:
    """Render copy-to-clipboard and download CSV buttons."""
    _, col1, col2 = st.columns([3, 1, 1])
    with col1:
        if st.button(
            type="primary",
            label="Copy to Clipboard",
            width="stretch",
            key=f"{key_prefix}_copy",
        ):
            copy_to_clipboard_unsecured(csv_copy)
            copy_to_clipboard(csv_copy)
            st.toast(toast_msg)
    with col2:
        st.download_button(
            type="primary",
            label="Download CSV",
            data=csv_download,
            file_name=file_name,
            mime="text/csv",
            width="stretch",
            key=f"{key_prefix}_download",
        )


def section_header(title: str) -> None:
    st.html(
        f"""
        <div style="font-size: 14px; font-weight: 600; color: #2196F3;
                    margin: 15px 0 8px 0; padding-bottom: 5px;
                    border-bottom: 2px solid #2196F3;">
            {title}
        </div>
    """
    )


def render_info_item(label: str, value: str, muted: bool = False) -> str:
    value_color = "#6c757d" if muted else "#212529"
    return f'''<div style="display: flex; flex-direction: column;">
        <div style="font-size: 11px; color: #6c757d; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 4px;">{label}</div>
        <div style="font-size: 14px; font-weight: 500; color: {value_color};">{value}</div>
    </div>'''


def render_big_info_item(label: str, value: str) -> str:
    return f'''<div style="display: flex; flex-direction: column;">
        <div style="font-size: 12px; font-weight: 600; color: #2196F3; margin-bottom: 1px; text-transform: uppercase;">{label}</div>
        <div style="font-size: 18px; font-weight: 600; color: #1a1a1a; line-height: 1.2;">{value}</div>
    </div>'''


def get_section_label_html(title: str) -> str:
    return f'<div style="font-size: 14px; font-weight: 600; color: #212529; letter-spacing: 0.5px; margin-bottom: 4px;">{title}</div>'


def get_card_header_html(title: str) -> str:
    return f'<p style="font-size: 1rem; font-weight: 600; margin: 0 0 12px 0;">{title}</p>'


def spacer(height: int = 16) -> None:
    st.html(f'<div style="height: {height}px;"></div>')
