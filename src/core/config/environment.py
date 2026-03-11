import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

INTERNAL_MODE = os.environ.get("INTERNAL_MODE", "").lower() == "true"
FACTOR_LIST_DIR = Path(os.environ["FACTOR_LIST_DIR"])
