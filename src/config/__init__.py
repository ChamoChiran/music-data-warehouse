"""
`src/config/__init__.py`

Expose common configuration symbols for easy access.
"""

from .lastfm_config import API_KEY, BASE_URL, DATA_DIR
from .settings import COUNTRIES

__all__ = ["API_KEY", "BASE_URL", "DATA_DIR", "COUNTRIES"]