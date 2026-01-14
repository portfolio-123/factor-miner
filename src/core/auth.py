import os
import json
import streamlit as st
from jose import jwe

from src.core.context import get_state, update_state
from src.ui.components import render_auth_form
from src.services.p123_client import authenticate, fetch_factor_list


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


def _verify_api_credentials(api_id: str, api_key: str, factor_list_uid: str):
    access_token = authenticate(api_id, api_key)
    if not access_token:
        return None, "Authentication failed. Invalid API credentials."

    factor_list_data, error = fetch_factor_list(factor_list_uid, access_token)
    if error:
        return None, error

    return {
        "accessToken": access_token,
        "factorListUid": factor_list_uid,
        "factorListData": factor_list_data,
    }, None


def authenticate_user():
    state = get_state()
    fl_id = state.factor_list_uid

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
                    result, error = _verify_api_credentials(api_id, api_key, fl_id)
                    if result:
                        update_state(user_payload=result)
                        return
                    st.error(error)
                    st.stop()
        st.error("Invalid or expired token")
        st.stop()

    if "login_api_key" in st.session_state and "login_api_id" in st.session_state:
        api_key = st.session_state.pop("login_api_key", None)
        api_id = st.session_state.pop("login_api_id", None)
        if api_key and api_id:
            result, error = _verify_api_credentials(api_id, api_key, fl_id)
            if result:
                update_state(user_payload=result)
                return
            st.error(error)

    if not state.auth_check_complete:
        update_state(auth_check_complete=True)
        st.stop()

    render_auth_form()
    st.stop()
