import json
from typing import Any

from streamlit_local_storage import LocalStorage


def get_local_storage(key: str) -> str | None:
    ls = LocalStorage()
    return ls.getItem(key)


def set_local_storage(key: str, value: Any) -> None:
    ls = LocalStorage()
    if not isinstance(value, str):
        value = json.dumps(value)
    ls.setItem(key, value)


def clear_local_storage(key: str) -> None:
    ls = LocalStorage()
    ls.deleteItem(key)
