import os
import json

import streamlit as st
from jose import jwe, JWEError
from pydantic import ValidationError

from src.core.types import TokenPayload


@st.cache_resource
def _load_secret() -> bytes:
    secret_path = os.getenv("JWT_SECRET_PATH")
    if not secret_path or not os.path.exists(secret_path):
        raise FileNotFoundError("Missing secret key")
    with open(secret_path, "r", encoding="utf-8") as f:
        return f.read().strip().encode("utf-8")


def decrypt_token(token: str) -> TokenPayload:
    try:
        decrypted = jwe.decrypt(token, _load_secret())
        return TokenPayload(**json.loads(decrypted))
    except (JWEError, json.JSONDecodeError, ValidationError) as e:
        raise ValueError("Invalid token") from e
