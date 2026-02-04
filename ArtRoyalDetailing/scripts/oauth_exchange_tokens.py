import json
import sys
import time
import urllib.request
from urllib.error import HTTPError, URLError
from pathlib import Path


# -------- Paths --------
BASE_DIR = Path(__file__).resolve().parent.parent
SECRETS_DIR = BASE_DIR / "secrets"

APP_CONFIG_PATH = SECRETS_DIR / "amocrm_app.json"
AUTH_CODE_PATH = SECRETS_DIR / "auth_code.txt"
TOKENS_PATH = SECRETS_DIR / "amocrm_tokens.json"


def die(msg: str, code: int = 1) -> None:
    print(msg)
    sys.exit(code)


def load_json(path: Path) -> dict:
    if not path.exists():
        die(f"❌ Не найден файл: {path}")
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except json.JSONDecodeError as e:
        die(f"❌ Ошибка JSON в {path}: {e}")


def read_auth_code() -> str:
    if not AUTH_CODE_PATH.exists():
        die(f"❌ Не найден {AUTH_CODE_PATH}\n"
            f"Сначала запусти scripts/oauth_get_code.py и нажми 'Разрешить'.")
    code = AUTH_CODE_PATH.read_text(encoding="utf-8").strip()
    if not code:
        die(f"❌ Файл {AUTH_CODE_PATH} пуст. Получи code заново.")
    return code


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
        die(f"❌ HTTPError {e.code} при запросе {url}\nОтвет сервера:\n{body}")
    except URLError as e:
        die(f"❌ URLError при запросе {url}: {e}")
    except json.JSONDecodeError:
        die("❌ Не удалось разобрать JSON-ответ от amoCRM.")


def save_tokens(tokens: dict) -> None:
    """
    tokens из amoCRM обычно содержит:
    access_token, refresh_token, expires_in, token_type
    Мы сохраняем expires_at = текущее время + expires_in - запас
    """
    now = int(time.time())
    expires_in = int(tokens.get("expires_in", 0))
    if not expires_in:
        die("❌ В ответе нет expires_in. Ответ неожиданного формата.")

    # запас 60 секунд, чтобы не упираться в 'истёк в момент запроса'
    expires_at = now + expires_in - 60

    out = {
        "access_token": tokens.get("access_token", ""),
        "refresh_token": tokens.get("refresh_token", ""),
        "expires_at": expires_at,
        "token_type": tokens.get("token_type", "Bearer")
    }

    if not out["access_token"] or not out["refresh_token"]:
        die(f"❌ В ответе нет access_token/refresh_token. Ответ: {tokens}")

    TOKENS_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(TOKENS_PATH, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print("✅ Токены сохранены в:", TOKENS_PATH)
    print("✅ access_token жив до (unix):", out["expires_at"])


def main():
    cfg = load_json(APP_CONFIG_PATH)

    required = ["account_domain", "client_id", "client_secret", "redirect_uri"]
    missing = [k for k in required if k not in cfg or not str(cfg[k]).strip()]
    if missing:
        die(f"❌ В {APP_CONFIG_PATH} не хватает полей: {', '.join(missing)}")

    account_domain = cfg["account_domain"].rstrip("/")
    client_id = cfg["client_id"]
    client_secret = cfg["client_secret"]
    redirect_uri = cfg["redirect_uri"]

    code = read_auth_code()

    url = f"{account_domain}/oauth2/access_token"

    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri
    }

    print("🔹 Обмениваю authorization code на access/refresh tokens...")
    tokens = post_json(url, payload)
    save_tokens(tokens)
    print("Готово. Следующий шаг: сделать клиент с авто-refresh и выгрузку справочников в CSV.")


if __name__ == "__main__":
    main()
