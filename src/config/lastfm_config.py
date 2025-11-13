import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv('LASTFM_API_KEY')
BASE_URL = 'https://ws.audioscrobbler.com/2.0/'
DATA_DIR = Path(__file__).parent.parent.parent / 'data' / 'raw' / 'geo'
