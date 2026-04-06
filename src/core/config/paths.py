from pathlib import Path

from src.core.config.environment import DATASET_DIR, INTERNAL_MODE


def get_user_base_dir(user_uid: str | None) -> Path:
    if INTERNAL_MODE:
        assert user_uid is not None
        return Path(DATASET_DIR, user_uid)
    return DATASET_DIR
