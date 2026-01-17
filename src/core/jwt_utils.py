import json

import streamlit as st
from jose import jwe, JWTError
from pydantic import ValidationError

from src.core.environment import JWT_SECRET_PATH
from src.core.types import TokenPayload


@st.cache_resource
def _load_secret() -> bytes:
    if not JWT_SECRET_PATH.exists():
        raise FileNotFoundError("Missing secret key")
    with open(JWT_SECRET_PATH, "r", encoding="utf-8") as f:
        return f.read().strip().encode("utf-8")


def decrypt_token(token: str) -> TokenPayload:
    try:
        decrypted = jwe.decrypt(token, _load_secret())
        return TokenPayload(**json.loads(decrypted))
    except (JWTError, json.JSONDecodeError, ValidationError) as e:
        raise ValueError("Invalid token") from e
