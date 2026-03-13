import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

INTERNAL_MODE = os.environ.get("INTERNAL_MODE", "").lower() == "true"
DATASET_DIR = Path(os.environ["DATASET_DIR"])
