from src.internal.config import P123_BASE_URL


def p123_link(fl_id: str, page: str | None = None) -> str | None:
    base = f"{P123_BASE_URL}/sv/factorList/{fl_id}"
    return f"{base}/{page}" if page else base


def p123_auth_link(fl_id: str) -> str:
    return f"{P123_BASE_URL}/spr/factorList/factor-miner/{fl_id}"
