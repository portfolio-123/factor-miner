import os
import json
import streamlit as st
from jose import jwe
from streamlit_cookies_controller import CookieController

from src.core.constants import AUTH_COOKIE_KEY
from src.core.context import get_state, update_state, clear_credentials
from src.ui.components import render_auth_form
from src.services.p123_client import authenticate, verify_factor_list_access


@st.cache_resource
def _load_secret():
    secret_path = os.getenv("JWT_SECRET_PATH")
    if not secret_path or not os.path.exists(secret_path):
        return None
    with open(secret_path, "r", encoding="utf-8") as f:
        return f.read().strip().encode("utf-8")


def _decrypt_token(token: str, secret_key: bytes):
    try:
        decrypted = jwe.decrypt(token, secret_key)
        return json.loads(decrypted)
    except Exception:
        return None


def _verify_api_credentials(api_id: str, api_key: str, factor_list_uid: str) -> dict:
    access_token = authenticate(api_id, api_key)
    if not access_token:
        raise ValueError("Authentication failed. Invalid API credentials.")

    factor_list_data = verify_factor_list_access(factor_list_uid, access_token)

    return {
        "accessToken": access_token,
        "factorListUid": factor_list_uid,
        "factorListName": factor_list_data.get("name", "Unknown"),
    }


def authenticate_user():
    state = get_state()
    fl_id = state.factor_list_uid
    cookies = CookieController()

    if state.user_payload and state.user_payload.get("accessToken"):
        return

    url_token = st.query_params.get("token")
    if url_token:
        del st.query_params["token"]
        secret_key = _load_secret()
        if secret_key:
            payload = _decrypt_token(url_token, secret_key)
            if payload:
                api_key = payload.get("apiKey")
                api_id = payload.get("apiId")
                if api_key and api_id:
                    try:
                        result = _verify_api_credentials(api_id, api_key, fl_id)
                        update_state(user_payload=result)
                        cookies.set(AUTH_COOKIE_KEY, result["accessToken"])
                        return
                    except ValueError as e:
                        st.error(str(e))
                        st.stop()
        st.error("Invalid or expired token")
        st.stop()

    if state.api_id and state.api_key:
        clear_credentials()
        try:
            result = _verify_api_credentials(state.api_id, state.api_key, fl_id)
            update_state(user_payload=result)
            cookies.set(AUTH_COOKIE_KEY, result["accessToken"])
            return
        except ValueError as e:
            st.error(str(e))

    cookie_token = cookies.get(AUTH_COOKIE_KEY)
    if cookie_token:
        try:
            factor_list_data = verify_factor_list_access(fl_id, cookie_token)
            update_state(
                user_payload={
                    "accessToken": cookie_token,
                    "factorListUid": fl_id,
                    "factorListName": factor_list_data.get("name", "Unknown"),
                }
            )
            return
        except ValueError:
            cookies.remove(AUTH_COOKIE_KEY)

    render_auth_form()
    st.stop()
