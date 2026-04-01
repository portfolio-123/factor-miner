from src.internal.config import P123_BASE_URL


def p123_link(fl_id: str, page: str | None = None) -> str:
    base = f"{P123_BASE_URL}/sv/factorList/{fl_id}"
    return f"{base}/{page}" if page else base


def p123_auth_link(fl_id: str | None) -> str:
    base = f"{P123_BASE_URL}/spr/factorList/factorMiner"
    return f"{base}/{fl_id}" if fl_id else base
