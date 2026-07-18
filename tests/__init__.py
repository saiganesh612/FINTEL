# pipeline/__init__.py
from dotenv import load_dotenv
from pathlib import Path

ENV_PATH = Path(__file__).parent.parent / ".env"

load_dotenv(dotenv_path=ENV_PATH)