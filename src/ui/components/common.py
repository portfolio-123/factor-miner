import streamlit as st


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


def render_section_label(title: str) -> None:
    st.html(
        f'<div class="dataset-info-item"><div class="label" style="margin-bottom: 8px; font-size: 14px; font-weight: 600; color: #212529; letter-spacing: 0.5px; text-transform: none;">{title}</div></div>'
    )


def spacer(height: int = 16) -> None:
    st.html(f'<div style="height: {height}px;"></div>')
