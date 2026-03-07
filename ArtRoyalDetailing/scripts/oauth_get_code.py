import json
import sys
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs, quote
from pathlib import Path


# --------------------
# Paths (no imports)
# --------------------
BASE_DIR = Path(__file__).resolve().parent.parent
SECRETS_DIR = BASE_DIR / "secrets"

APP_CONFIG_PATH = SECRETS_DIR / "amocrm_app.json"
AUTH_CODE_PATH = SECRETS_DIR / "auth_code.txt"


def die(msg: str, code: int = 1) -> None:
    print(msg)
    sys.exit(code)


def load_config() -> dict:
    if not APP_CONFIG_PATH.exists():
        die(f"❌ Не найден файл конфига: {APP_CONFIG_PATH}\n"
            f"Создай secrets/amocrm_app.json и заполни auth_domain, client_id, redirect_uri.")

    try:
        with open(APP_CONFIG_PATH, "r", encoding="utf-8") as f:
            cfg = json.load(f)
    except json.JSONDecodeError as e:
        die(f"❌ Ошибка JSON в {APP_CONFIG_PATH}: {e}")

    required = ["auth_domain", "client_id", "redirect_uri"]
    missing = [k for k in required if k not in cfg or not str(cfg[k]).strip()]
    if missing:
        die(f"❌ В {APP_CONFIG_PATH} не хватает полей: {', '.join(missing)}")

    return cfg


class OAuthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        qs = parse_qs(parsed.query)

        if "code" not in qs:
            self.send_response(400)
            self.end_headers()
            self.wfile.write("❌ В URL нет параметра code".encode("utf-8"))
            print("❌ Пришёл запрос без code.")
            print("Проверь, что ты нажал 'Разрешить доступ' и redirect_uri совпадает с настройками интеграции.")
            return

        code = qs["code"][0]

        AUTH_CODE_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(AUTH_CODE_PATH, "w", encoding="utf-8") as f:
            f.write(code)

        self.send_response(200)
        self.end_headers()
        self.wfile.write("✅ Code получен. Можешь закрыть окно.".encode("utf-8"))

        print("\n✅ Authorization code получен:")
        print(code)
        print(f"📁 Сохранён в: {AUTH_CODE_PATH}")

    # чтобы не спамить логами на каждый запрос
    def log_message(self, format, *args):
        return


def build_auth_url(auth_domain: str, client_id: str, redirect_uri: str) -> str:
    auth_domain = auth_domain.rstrip("/")
    redirect_encoded = quote(redirect_uri, safe="")
    return (
        f"{auth_domain}/oauth"
        f"?client_id={client_id}"
        f"&redirect_uri={redirect_encoded}"
        f"&response_type=code"
    )


def main():
    cfg = load_config()

    auth_domain = cfg["auth_domain"]
    client_id = cfg["client_id"]
    redirect_uri = cfg["redirect_uri"]

    auth_url = build_auth_url(auth_domain, client_id, redirect_uri)

    print("\n1) Открой ссылку и нажми «Разрешить доступ»:")
    print(auth_url)
    print("\n2) Я жду редирект на http://localhost:8080 ...")

    try:
        opened = webbrowser.open(auth_url)
        if not opened:
            print("⚠️ Браузер не открылся автоматически. Скопируй ссылку выше и открой вручную.")
    except Exception as e:
        print(f"⚠️ Не удалось открыть браузер автоматически: {e}")
        print("Скопируй ссылку выше и открой вручную.")

    try:
        server = HTTPServer(("localhost", 8080), OAuthHandler)
    except OSError as e:
        die(f"❌ Не удалось занять порт 8080: {e}\n"
            f"Если порт занят, поменяй redirect_uri на 8090 и в коде тоже (localhost, 8090).")

    server.handle_request()  # ждём ровно один запрос (redirect)
    print("\nГотово. Следующий шаг: обменять code на oauth_exchange_tokens.py")


if __name__ == "__main__":
    main()