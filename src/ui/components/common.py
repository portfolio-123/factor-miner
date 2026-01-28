import html
import streamlit as st


def copy_button(
    text: str,
    label: str = "Copy",
    success_label: str = "Copied!",
    button_type: str = "primary",
    width: str = "auto",
) -> None:
    escaped_text = html.escape(text).replace("`", "\\`").replace("$", "\\$")
    width_class = "stretch" if width == "stretch" else ""
    width_style = f"width: {width};" if width not in ("auto", "stretch") else ""

    button_id = f"copy-btn-{id(text)}"

    st.html(f"""
        <button
            id="{button_id}"
            class="copy-btn {button_type} {width_class}"
            style="{width_style}"
            onclick="
                navigator.clipboard.writeText(`{escaped_text}`).then(() => {{
                    const btn = document.getElementById('{button_id}');
                    const originalText = btn.textContent;
                    btn.textContent = '{success_label}';
                    btn.classList.add('copied');
                    setTimeout(() => {{
                        btn.textContent = originalText;
                        btn.classList.remove('copied');
                    }}, 2000);
                }});
            "
        >{label}</button>
    """)


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
    value_class = "value muted" if muted else "value"
    return f'<div class="dataset-info-item"><div class="label">{label}</div><div class="{value_class}">{value}</div></div>'


def render_big_info_item(label: str, value: str) -> str:
    return f'<div class="dataset-info-item big"><div class="label">{label}</div><div class="value">{value}</div></div>'


def get_section_label_html(title: str) -> str:
    return f'<div class="dataset-info-item"><div class="label" style="margin-bottom: 4px; font-size: 14px; font-weight: 600; color: #212529; letter-spacing: 0.5px; text-transform: none;">{title}</div></div>'


def spacer(height: int = 16) -> None:
    st.html(f'<div style="height: {height}px;"></div>')
