import json
from typing import Any

import streamlit as st
from streamlit_js import st_js, st_js_blocking


def get_local_storage(key: str) -> str | None:
    return st_js_blocking(f"return localStorage.getItem('{key}')")


def set_local_storage(key: str, value: Any) -> None:
    if not isinstance(value, str):
        value = json.dumps(value)
    st_js(f"localStorage.setItem('{key}', '{value}')")


def clear_local_storage(key: str) -> None:
    st_js(f"localStorage.removeItem('{key}')")
