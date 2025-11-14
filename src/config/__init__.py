"""
`src/config/__init__.py`

Expose common configuration symbols for easy access.
"""

from .lastfm_config import API_KEY, BASE_URL, DATA_DIR, BRONZE_OUT
from .settings import COUNTRIES, CONNECTION_STRING

__all__ = ["API_KEY", "BASE_URL", "DATA_DIR", "COUNTRIES", "BRONZE_OUT", "CONNECTION_STRING"]