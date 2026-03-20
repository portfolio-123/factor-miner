from pathlib import Path

from src.core.config.environment import DATASET_DIR, INTERNAL_MODE


def get_user_base_dir(user_uid: str | None) -> Path:
    return Path(DATASET_DIR, user_uid) if INTERNAL_MODE else DATASET_DIR
