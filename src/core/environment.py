import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

FACTOR_LIST_DIR = Path(os.environ["FACTOR_LIST_DIR"])
JWT_SECRET_PATH = Path(os.environ["JWT_SECRET_PATH"])
P123_BASE_URL = os.environ["P123_BASE_URL"]
API_BASE_URL = os.environ["API_BASE_URL"]

FACTORMINER_DIR = FACTOR_LIST_DIR / "factor-miner"
