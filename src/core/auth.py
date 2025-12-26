import os
from jose import jwe
import streamlit as st
import json
from extra_streamlit_components import CookieManager


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
    cookie_manager = CookieManager(key="auth_manager")

    url_token = st.query_params.get("token")
    if url_token:
        cookie_manager.set("jwt_token", url_token)
        del st.query_params["token"]

    token = cookie_manager.get("jwt_token")

    if token:
        payload = _decrypt_token(token, secret_key)
        if payload:
            st.session_state["user_payload"] = payload
            return payload

    if "auth_check_complete" not in st.session_state:
        st.session_state["auth_check_complete"] = True
        st.stop()

    fl_id = st.query_params.get("fl_id")

    st.markdown("<div style='height: 25vh'></div>", unsafe_allow_html=True)

    _, col, _ = st.columns([1, 2, 1])

    with col:
        st.warning(
            "**Session expired or invalid**\n\n"
            'Access this tool via the main website with the "Factor Evaluator" button.',
        )

        if fl_id:
            base_url = os.getenv("P123_BASE_URL")
            st.markdown(
                f"<div style='text-align: center; margin-top: 10px;'><a href='{base_url}/sv/factorList/{fl_id}/download'>Return to Factor List</a></div>",
                unsafe_allow_html=True,
            )

    return None
