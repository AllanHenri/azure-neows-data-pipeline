import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent.parent

NASA_API_KEY = os.getenv("NASA_API_KEY", "DEMO_KEY")
START_DATE = os.getenv("START_DATE", "2015-09-07")
END_DATE = os.getenv("END_DATE", "2015-09-08")

AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "")
AZURE_FILE_SYSTEM = os.getenv("AZURE_FILE_SYSTEM", "raw")
AZURE_DIRECTORY = os.getenv("AZURE_DIRECTORY", "nasa/neows/year=2015/month=09/day=08")
AZURE_FILE_NAME = os.getenv("AZURE_FILE_NAME", "neo_feed.json")

LOCAL_RAW_PATH = BASE_DIR / os.getenv("LOCAL_RAW_PATH", "data/raw/neo_feed.json")
PROCESSED_OUTPUT = BASE_DIR / os.getenv("PROCESSED_OUTPUT", "data/processed/neo_flattened")
CURATED_OUTPUT = BASE_DIR / os.getenv("CURATED_OUTPUT", "data/curated/neo_metrics")