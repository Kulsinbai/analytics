from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

SECRETS_DIR = BASE_DIR / "secrets"
DATA_DIR = BASE_DIR / "data"

APP_CONFIG_PATH = SECRETS_DIR / "amocrm_app.json"
AUTH_CODE_PATH = SECRETS_DIR / "auth_code.txt"
CLICKHOUSE_CONFIG_PATH = SECRETS_DIR / "clickhouse.json"