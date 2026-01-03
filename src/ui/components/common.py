import os

import streamlit as st
import streamlit.components.v1 as components
from streamlit_extras.stylable_container import stylable_container


def copy_to_clipboard_button(
    text: str,
    label: str = "Copy to Clipboard",
    key: str = "copy_btn",
    button_type: str = "secondary",
) -> None:
    escaped = text.replace("\\", "\\\\").replace("`", "\\`").replace("${", "\\${")
    container_key = f"{key}_container"

    with stylable_container(key=container_key, css_styles=""):
        st.button(label, key=key, type=button_type, use_container_width=True)

    components.html(
        f"""
        <script>
            const csv = `{escaped}`;
            const btn = window.parent.document.querySelector('[data-key="{container_key}"] button');
            if (btn && !btn._copyAttached) {{
                btn._copyAttached = true;
                btn.addEventListener('click', (e) => {{
                    e.preventDefault();
                    navigator.clipboard.writeText(csv);
                    const txt = btn.querySelector('p') || btn;
                    const orig = txt.textContent;
                    txt.textContent = 'Copied!';
                    setTimeout(() => txt.textContent = orig, 1000);
                }});
            }}
        </script>
        """,
        height=0,
    )


def render_session_expired(fl_id: str | None) -> None:
    st.markdown("<div style='height: 25vh'></div>", unsafe_allow_html=True)
    _, col, _ = st.columns([1, 2, 1])
    with col:
        st.warning(
            "**Session expired or invalid**\n\n"
            "Access this tool via the main website."
        )
        if fl_id:
            base_url = os.getenv("P123_BASE_URL")
            st.markdown(
                f"<div style='text-align: center; margin-top: 10px;'>"
                f"<a href='{base_url}/sv/factorList/{fl_id}/download'>Return to Factor List</a>"
                f"</div>",
                unsafe_allow_html=True,
            )


def section_header(title: str) -> None:
    st.markdown(
        f"""
        <div style="font-size: 14px; font-weight: 600; color: #2196F3;
                    margin: 15px 0 8px 0; padding-bottom: 5px;
                    border-bottom: 2px solid #2196F3;">
            {title}
        </div>
    """,
        unsafe_allow_html=True,
    )


def render_info_item(label: str, value: str, muted: bool = False) -> str:
    value_class = "value muted" if muted else "value"
    return f'<div class="dataset-info-item"><div class="label">{label}</div><div class="{value_class}">{value}</div></div>'


def render_big_info_item(label: str, value: str) -> str:
    return f'<div class="dataset-info-item big"><div class="label">{label}</div><div class="value">{value}</div></div>'


def render_section_label(title: str) -> None:
    st.markdown(
        f'<div class="dataset-info-item"><div class="label" style="margin-bottom: 8px; font-size: 14px; font-weight: 600; color: #212529; letter-spacing: 0.5px; text-transform: none;">{title}</div></div>',
        unsafe_allow_html=True,
    )


def spacer(height: int = 16) -> None:
    st.markdown(f'<div style="height: {height}px;"></div>', unsafe_allow_html=True)
