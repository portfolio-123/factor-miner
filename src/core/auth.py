import streamlit as st

from src.core.config.constants import AUTH_COOKIE_KEY
from src.core.types.models import TokenPayload
from src.core.utils.cookie_utils import clear_cookie, get_cookie, set_cookie
from src.core.utils.jwt_utils import decrypt_token
from src.services.p123_client import (
    authenticate as get_access_token,
    verify_factor_list_access,
)


def _authenticate(token: str, save_cookie: bool = True) -> None:
    fl_id = st.query_params.get("fl_id")
    fl_info = verify_factor_list_access(fl_id, token)
    st.session_state.access_token = token
    st.session_state.fl_name = fl_info.get("name", fl_id)
    st.session_state.user_uid = fl_info.get("userUid")
    if save_cookie:
        set_cookie(AUTH_COOKIE_KEY, token, days=1)


def login():
    # validate token if existing
    if existing_token := st.session_state.get("access_token"):
        try:
            fl_id = st.query_params.get("fl_id")
            verify_factor_list_access(fl_id, existing_token)
            return  # token still valid
        except PermissionError:
            # token expired, clear it and continue to re-auth
            st.session_state.access_token = None

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
            clear_cookie(AUTH_COOKIE_KEY)  # Clear invalid cookie

    # otherwise, normal login form
    login_container = st.empty()
    with login_container.container():
        token = _render_auth_form() 

    if token:
        login_container.empty()
        try:
            _authenticate(token)
            return
        except PermissionError as e:
            st.error(str(e))
            st.stop()

    st.stop()


def _render_auth_form() -> str | None:
    with st.columns([1, 2, 1])[1]:
        st.markdown("### Login")
        st.caption("Enter your API credentials to access this Factor List.")

        with st.form(key="login_form", border=False):
            api_id = st.text_input("API ID", placeholder="Enter your API ID")
            api_key = st.text_input(
                "API Key",
                placeholder="Enter your API Key",
            )

            submitted = st.form_submit_button(
                "Login",
                type="primary",
                width="stretch",
            )

            if submitted:
                try:
                    return get_access_token(
                        TokenPayload(apiId=int(api_id), apiKey=api_key)
                    )
                except PermissionError as e:
                    st.error(str(e))

    return None
