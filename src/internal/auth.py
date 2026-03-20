import streamlit as st

from src.core.config.constants import AUTH_COOKIE_KEY
from src.core.utils.cookie_utils import clear_cookie, get_cookie, set_cookie
from src.core.utils.jwt_utils import decrypt_token
from src.internal.links import p123_auth_link
from src.internal.p123_client import (
    authenticate as get_access_token,
    verify_factor_list_access,
)


def _authenticate(token: str, save_cookie=True) -> None:
    fl_id = st.query_params.get("fl_id")
    fl_info = verify_factor_list_access(fl_id, token)
    st.session_state.access_token = token
    st.session_state.fl_name = fl_info.get("name", fl_id)
    user_uid = fl_info.get("userUid")
    if not user_uid:
        raise ValueError("User UID not found in API response")
    st.session_state.user_uid = str(user_uid)
    if save_cookie:
        set_cookie(AUTH_COOKIE_KEY, token, days=1)


def login():
    # validate token if existing
    if existing_token := st.session_state.get("access_token"):
        try:
            verify_factor_list_access(st.query_params.get("fl_id"), existing_token)
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

    fl_id = st.query_params.get("fl_id")
    with st.columns([1, 2, 1])[1]:
        st.info(
            "You don't have a valid session. Please authenticate via Portfolio123 to access your datasets."
        )
        st.link_button(
            "Go to Portfolio123", p123_auth_link(fl_id), type="primary", width="stretch"
        )
    st.stop()
