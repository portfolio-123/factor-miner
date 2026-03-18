import os

from ..core.config import environment  # runs load_dotenv

JWT_SECRET = os.environ.get("JWT_SECRET", "")
P123_BASE_URL = os.environ.get("P123_BASE_URL", "")
API_BASE_URL = os.environ.get("API_BASE_URL", "")
