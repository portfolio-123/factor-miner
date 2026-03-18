from os import environ

from ..core.config import environment  # runs load_dotenv

JWT_SECRET = environ.get("JWT_SECRET", "")
P123_BASE_URL = environ.get("P123_BASE_URL", "")
API_BASE_URL = environ.get("API_BASE_URL", "")
