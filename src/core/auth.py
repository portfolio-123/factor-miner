import os
import time
from jose import jwe
import streamlit as st
from src.core.utils import get_local_storage
import json


def load_secret():
    secret_filename = os.getenv("JWT_SECRET_PATH")

    if not secret_filename:
        st.error("Environment variable for JWT_SECRET_PATH is missing.")
        st.stop()

    secret_path = os.path.abspath(secret_filename)

    if not os.path.exists(secret_path):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(os.path.dirname(current_dir))

        secret_path = os.path.join(project_root, secret_filename)

    if not os.path.exists(secret_path):
        st.error(f"JWT verification failed. Checked: {secret_filename}")
        st.stop()

    with open(secret_path, "r", encoding="utf-8") as f:
        return f.read().strip().encode("utf-8")


def _decrypt_token(token, secret_key):
    try:
        decrypted = jwe.decrypt(token, secret_key)
        return json.loads(decrypted)
    except Exception:
        return None


def authenticate_user():
    if st.session_state.get("user_payload"):
        return st.session_state["user_payload"]

    secret_key = load_secret()
    token = st.query_params.get("token") or get_local_storage().getItem("jwt_token")

    if token:
        payload = _decrypt_token(token, secret_key)
        if payload:
            st.session_state["user_payload"] = payload
            return payload
        return None

    st.warning(
        "No access token provided. Please access this tool via the main website."
    )
    return None
