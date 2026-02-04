import json
import time
import urllib.request
from urllib.error import HTTPError, URLError
from pathlib import Path


# -------- Paths --------
BASE_DIR = Path(__file__).resolve().parent.parent
SECRETS_DIR = BASE_DIR / "secrets"

APP_CONFIG_PATH = SECRETS_DIR / "amocrm_app.json"
TOKENS_PATH = SECRETS_DIR / "amocrm_tokens.json"


class AmoClientError(Exception):
    pass


def load_json(path: Path) -> dict:
    if not path.exists():
        raise AmoClientError(f"Не найден файл: {path}")

    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        raise AmoClientError(f"Ошибка JSON в {path}: {e}")


def save_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def post_json(url: str, payload: dict) -> dict:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url=url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST"
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read().decode("utf-8")
            return json.loads(body)
    except HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise AmoClientError(f"HTTP {e.code} при POST {url}\nОтвет:\n{body}")
    except URLError as e:
        raise AmoClientError(f"Сетевая ошибка при POST {url}: {e}")
    except json.JSONDecodeError:
        raise AmoClientError("Не удалось разобрать JSON-ответ сервера.")


def get_valid_access_token() -> tuple[str, str]:
    """
    Возвращает (account_domain, access_token).
    Если access_token истёк — обновляет по refresh_token и сохраняет новые токены.
    """
    cfg = load_json(APP_CONFIG_PATH)

    required = ["account_domain", "client_id", "client_secret"]
    missing = [k for k in required if k not in cfg or not str(cfg[k]).strip()]
    if missing:
        raise AmoClientError(f"В amocrm_app.json не хватает полей: {', '.join(missing)}")

    account_domain = cfg["account_domain"].rstrip("/")
    client_id = cfg["client_id"]
    client_secret = cfg["client_secret"]

    tokens = load_json(TOKENS_PATH)
    access_token = tokens.get("access_token", "")
    refresh_token = tokens.get("refresh_token", "")
    expires_at = int(tokens.get("expires_at", 0))

    if not access_token or not refresh_token or not expires_at:
        raise AmoClientError("Файл токенов неполный. Пересоздай токены через oauth_exchange_tokens.py")

    now = int(time.time())

    # Если токен ещё жив — возвращаем его
    if now < expires_at:
        return account_domain, access_token

    # Иначе refresh
    url = f"{account_domain}/oauth2/access_token"
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "refresh_token",
        "refresh_token": refresh_token
    }

    new_tokens = post_json(url, payload)

    # expires_at пересчитываем
    expires_in = int(new_tokens.get("expires_in", 0))
    if not expires_in:
        raise AmoClientError(f"Неожиданный ответ при refresh: {new_tokens}")

    out = {
        "access_token": new_tokens.get("access_token", ""),
        "refresh_token": new_tokens.get("refresh_token", ""),
        "expires_at": int(time.time()) + expires_in - 60,
        "token_type": new_tokens.get("token_type", "Bearer")
    }

    if not out["access_token"] or not out["refresh_token"]:
        raise AmoClientError(f"В ответе refresh нет access/refresh: {new_tokens}")

    save_json(TOKENS_PATH, out)
    return account_domain, out["access_token"]


def get_json(url: str, access_token: str) -> dict:
    req = urllib.request.Request(
        url=url,
        headers={"Authorization": f"Bearer {access_token}"},
        method="GET"
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read().decode("utf-8")
            return json.loads(body)
    except HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise AmoClientError(f"HTTP {e.code} при GET {url}\nОтвет:\n{body}")
    except URLError as e:
        raise AmoClientError(f"Сетевая ошибка при GET {url}: {e}")
    except json.JSONDecodeError:
        raise AmoClientError("Не удалось разобрать JSON-ответ сервера.")
