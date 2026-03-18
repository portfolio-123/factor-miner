from os import environ
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

INTERNAL_MODE = environ.get("INTERNAL_MODE", "").lower() == "true"
DATASET_DIR = Path(environ["DATASET_DIR"])
