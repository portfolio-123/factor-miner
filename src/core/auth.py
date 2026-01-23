import streamlit as st
import p123api

from src.core.constants import AUTH_COOKIE_KEY
from src.core.cookie_utils import get_cookie, set_cookie
from src.services.p123_client import create_client, verify_factor_list_access


def _store_credentials(
    api_id: int, api_key: str, token: str, save_cookie: bool = True
) -> None:
    st.session_state.api_id = api_id
    st.session_state.api_key = api_key
    st.session_state.access_token = token
    if save_cookie:
        set_cookie(AUTH_COOKIE_KEY, f"{api_id}:{api_key}", days=1)


def _authenticate_and_store(
    api_id: int, api_key: str, token: str, save_cookie: bool = True
) -> None:
    fl_id = st.query_params.get("fl_id")
    fl_info = verify_factor_list_access(fl_id, token)
    st.session_state.fl_name = fl_info.get("name", fl_id)
    _store_credentials(api_id, api_key, token, save_cookie)


def login():
    if st.session_state.get("api_id") and st.session_state.get("api_key"):
        return

    # credentials already present in cookies
    if cookie_value := get_cookie(AUTH_COOKIE_KEY):
        try:
            api_id_str, api_key = cookie_value.split(":", 1)
            api_id = int(api_id_str)
            client = create_client(api_id, api_key)
            client.auth()
            token = client.get_token()
            _authenticate_and_store(api_id, api_key, token, save_cookie=False)
            return
        except (ValueError, p123api.ClientException, PermissionError):
            pass

    _render_auth_form()
    st.stop()


def _render_auth_form() -> None:
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
                    client = create_client(int(api_id), api_key)
                    client.auth()
                    token = client.get_token()
                    _authenticate_and_store(int(api_id), api_key, token)
                    st.rerun()
                except (ValueError, p123api.ClientException) as e:
                    st.error(f"Authentication failed: {e}")
                except PermissionError as e:
                    st.error(str(e))
