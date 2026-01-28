import streamlit as st

from src.core.constants import AUTH_COOKIE_KEY
from src.core.types import TokenPayload
from src.core.cookie_utils import get_cookie, set_cookie
from src.core.jwt_utils import decrypt_token
from src.services.p123_client import (
    authenticate as get_access_token,
    verify_factor_list_access,
)


def _authenticate(token: str, save_cookie: bool = True) -> None:
    fl_id = st.query_params.get("fl_id")
    fl_info = verify_factor_list_access(fl_id, token)
    st.session_state.access_token = token
    st.session_state.fl_name = fl_info.get("name", fl_id)
    if save_cookie:
        set_cookie(AUTH_COOKIE_KEY, token, days=1)


def login():
    if st.session_state.get("access_token"):
        return

    # url token (webapp redirect)
    if url_token := st.query_params.get("token"):
        try:
            del st.query_params["token"]
            token = get_access_token(decrypt_token(url_token))
            _authenticate(token)
            return
        except (ValueError, PermissionError, FileNotFoundError) as e:
            st.error(str(e))
            st.stop()

    # token already present in cookies
    if cookie_token := get_cookie(AUTH_COOKIE_KEY):
        try:
            _authenticate(cookie_token, save_cookie=False)
            return
        except PermissionError:
            pass

    # otherwise, normal login form
    login_container = st.empty()
    with login_container.container():
        token = _render_auth_form() 

    if token:
        login_container.empty()
        set_cookie(AUTH_COOKIE_KEY, token, days=1)
        return

    st.stop()


def _render_auth_form() -> str | None:
    with st.columns([1, 2, 1])[1]:
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
                "Login",
                type="primary",
                width="stretch",
            )

            if submitted:
                try:
                    token = get_access_token(
                        TokenPayload(apiId=int(api_id), apiKey=api_key)
                    )
                    fl_info = verify_factor_list_access(st.query_params.get("fl_id"), token)
                    st.session_state.access_token = token
                    st.session_state.fl_name = fl_info.get("name")
                    return token
                except PermissionError as e:
                    st.error(str(e))

    return None
