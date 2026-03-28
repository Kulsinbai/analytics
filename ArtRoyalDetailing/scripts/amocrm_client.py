import json
import time
import urllib.request
from urllib.error import HTTPError, URLError
from pathlib import Path
import os


# -------- Paths --------
BASE_DIR = Path(__file__).resolve().parent.parent
SECRETS_DIR = BASE_DIR / "secrets"


class AmoClientError(Exception):
    pass


def _resolve_secrets_paths(client_slug: str | None) -> tuple[Path, Path]:
    """
    Multi-tenant-friendly secrets layout (recommended):
      $AMOCRM_SECRETS_DIR/<client_slug>/amocrm_app.json
      $AMOCRM_SECRETS_DIR/<client_slug>/amocrm_tokens.json

    Backward compatible legacy layout (single-tenant):
      secrets/amocrm_app.json
      secrets/amocrm_tokens.json

    Resolution order:
    - If client_slug is None -> try env AMOCRM_CLIENT_SLUG
    - If client_slug is provided (or env) -> prefer per-client subdir files
    - Else fall back to legacy files if they exist
    """
    env_slug = os.getenv("AMOCRM_CLIENT_SLUG", "").strip() or None
    slug = (client_slug or env_slug)

    secrets_dir = Path(os.getenv("AMOCRM_SECRETS_DIR", str(SECRETS_DIR))).expanduser()

    legacy_app = secrets_dir / "amocrm_app.json"
    legacy_tokens = secrets_dir / "amocrm_tokens.json"

    if slug:
        app = secrets_dir / slug / "amocrm_app.json"
        tokens = secrets_dir / slug / "amocrm_tokens.json"
        if app.exists() and tokens.exists():
            return app, tokens

    if legacy_app.exists() and legacy_tokens.exists():
        return legacy_app, legacy_tokens

    if slug:
        raise AmoClientError(
            f"Не найдены secrets для client_slug='{slug}'. Ожидаю файлы:\n"
            f"- {secrets_dir / slug / 'amocrm_app.json'}\n"
            f"- {secrets_dir / slug / 'amocrm_tokens.json'}\n"
            "Либо положи legacy-файлы (single-tenant):\n"
            f"- {legacy_app}\n"
            f"- {legacy_tokens}"
        )

    raise AmoClientError(
        "Не найден client_slug (передай в get_valid_access_token(client_slug) "
        "или установи env AMOCRM_CLIENT_SLUG), и не найдены legacy secrets:\n"
        f"- {legacy_app}\n"
        f"- {legacy_tokens}"
    )


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


def get_valid_access_token(client_slug: str | None = None) -> tuple[str, str]:
    """
    Возвращает (account_domain, access_token).
    Если access_token истёк — обновляет по refresh_token и сохраняет новые токены.
    """
    app_path, tokens_path = _resolve_secrets_paths(client_slug)
    cfg = load_json(app_path)

    required = ["account_domain", "client_id", "client_secret"]
    missing = [k for k in required if k not in cfg or not str(cfg[k]).strip()]
    if missing:
        raise AmoClientError(
            f"В {app_path} не хватает полей: {', '.join(missing)}"
        )

    account_domain = cfg["account_domain"].rstrip("/")
    client_id = cfg["client_id"]
    client_secret = cfg["client_secret"]

    tokens = load_json(tokens_path)
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

    save_json(tokens_path, out)
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
