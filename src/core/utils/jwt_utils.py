import json

from jose import jwe, JWTError
from pydantic import ValidationError

from src.internal.config import JWT_SECRET
from src.core.types.models import TokenPayload


def decrypt_token(token: str) -> TokenPayload:
    try:
        decrypted = jwe.decrypt(token, JWT_SECRET.encode("utf-8"))
        if decrypted is None:
            raise JWTError()
        return TokenPayload.model_validate_json(decrypted)
    except (JWTError, json.JSONDecodeError, ValidationError) as e:
        raise ValueError("Invalid token") from e
