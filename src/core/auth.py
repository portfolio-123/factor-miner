import os
from jose import jwe
import streamlit as st
import json
from extra_streamlit_components import CookieManager

from src.core.context import get_state, update_state
from src.ui.components import render_session_expired


@st.cache_resource
def load_secret():
    secret_path = os.getenv("JWT_SECRET_PATH")
    if not secret_path or not os.path.exists(secret_path):
        return None

    with open(secret_path, "r", encoding="utf-8") as f:
        return f.read().strip().encode("utf-8")


def _decrypt_token(token, secret_key):
    try:
        decrypted = jwe.decrypt(token, secret_key)
        return json.loads(decrypted)
    except Exception:
        return None


def _validate_fl_access(payload, fl_id):
    token_fl_id = payload.get("factorListUid")
    if not token_fl_id or str(token_fl_id) != str(fl_id):
        st.error("Unauthorized: Your session does not have access to this Factor List.")
        st.stop()


def authenticate_user():
    state = get_state()
    fl_id = state.factor_list_uid

    if state.user_payload:
        _validate_fl_access(state.user_payload, fl_id)
        return

    secret_key = load_secret()
    if not secret_key:
        st.error("Error loading JWT secret")
        st.stop()

    cookie_manager = CookieManager(key="auth_manager")
    url_token = st.query_params.get("token")
    if url_token:
        cookie_manager.set("jwt_token", url_token)
        del st.query_params["token"]

    token = cookie_manager.get("jwt_token")
    if token:
        payload = _decrypt_token(token, secret_key)
        if payload:
            update_state(user_payload=payload)
            _validate_fl_access(payload, fl_id)
            return

    if not state.auth_check_complete:
        update_state(auth_check_complete=True)
        st.stop()

    render_session_expired(fl_id)
    st.stop()
