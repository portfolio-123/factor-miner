# taken from here: https://gist.github.com/palomena/d567d4bdb64a38ddafd181c3606716cc

import streamlit as st
import streamlit.components.v1 as components


def get_cookie(key: str) -> str | None:
    return st.context.cookies.get(key)


def set_cookie(name: str, value: str, days: int = 1):
    components.html(f"""<script>
        document.cookie = "{name}={value}; path=/; max-age={days * 86400}";
    </script>""", height=0)


def clear_cookie(name: str):
    components.html(f"""<script>
        document.cookie = "{name}=; path=/; max-age=0";
    </script>""", height=0)
