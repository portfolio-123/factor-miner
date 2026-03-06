import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

INTERNAL_MODE = os.environ.get("INTERNAL_MODE") == "True"
FACTOR_LIST_DIR = Path(os.environ["FACTOR_LIST_DIR"])
