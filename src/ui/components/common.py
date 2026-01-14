import streamlit as st


def render_auth_form() -> None:
    _, col, _ = st.columns([1, 2, 1])
    with col:
        st.markdown("### Login")
        st.caption("Enter your API credentials to access this Factor List.")

        with st.form(key="login_form", border=False):
            api_id = st.text_input("API ID", placeholder="Enter your API ID")
            api_key = st.text_input(
                "API Key",
                type="password",
                placeholder="Enter your API Key",
            )

            submitted = st.form_submit_button(
                "Login", type="primary", use_container_width=True
            )

            if submitted:
                if api_id and api_key:
                    st.session_state["login_api_id"] = api_id
                    st.session_state["login_api_key"] = api_key
                    st.rerun()
                else:
                    st.error("Please enter both API ID and API Key")


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
