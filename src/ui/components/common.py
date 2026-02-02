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
