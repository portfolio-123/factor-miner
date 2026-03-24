import streamlit as st

from src.core.config.constants import AUTH_COOKIE_KEY
from src.core.utils.cookie_utils import clear_cookie, get_cookie, set_cookie
from src.core.utils.jwt_utils import decrypt_token
from src.internal.links import p123_auth_link
from src.internal.p123_client import authenticate as get_access_token


def _authenticate(jwt_token: str, save_cookie=True) -> None:
    payload = decrypt_token(jwt_token)
    if not payload.user_uid:
        raise ValueError("User UID not found in token")
    access_token = get_access_token(apiId=payload.apiId, apiKey=payload.apiKey)
    st.session_state["access_token"] = access_token
    st.session_state["user_uid"] = payload.user_uid
    if save_cookie:
        set_cookie(AUTH_COOKIE_KEY, jwt_token, days=1)


def log_in():
    # already authenticated this session
    if st.session_state.get("access_token") and st.session_state.get("user_uid"):
        return

    # url token (webapp redirect) or cookie
    qp_token = st.query_params.get("token")
    jwt_token = qp_token or get_cookie(AUTH_COOKIE_KEY)
    if jwt_token:
        try:
            if qp_token:
                del st.query_params["token"]
            _authenticate(jwt_token)
            return
        except (ValueError, PermissionError, FileNotFoundError) as e:
            clear_cookie(AUTH_COOKIE_KEY)
            st.error(str(e))
            st.stop()

    fl_id = st.query_params.get("fl_id")
    with st.columns([1, 2, 1])[1]:
        st.info(
            "You don't have a valid session. Please authenticate via Portfolio123 to access your datasets."
        )
        st.link_button("Login", p123_auth_link(fl_id), type="primary", width="stretch")
    st.stop()
