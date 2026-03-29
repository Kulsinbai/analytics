import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import json
import argparse
import time
from datetime import datetime, timezone
from urllib.parse import urlencode

from scripts.amocrm_client import get_valid_access_token, get_json


def _parse_since_updated_at(raw: str) -> datetime:
    """
    Парсинг нижней границы updated_at (UTC): unix-целое, ISO-8601 или 'YYYY-MM-DD HH:MM:SS'.
    """
    s = (raw or "").strip()
    if not s:
        raise ValueError("пустая строка --since-updated-at")

    if s.isdigit():
        return datetime.fromtimestamp(int(s), tz=timezone.utc)

    if "T" not in s and len(s) >= 19 and s[10:11] == " ":
        try:
            dt = datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S")
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            pass

    if s.endswith("Z"):
        s = s[:-1] + "+00:00"

    try:
        dt = datetime.fromisoformat(s)
    except ValueError as e:
        raise ValueError(
            "Ожидается unix timestamp, ISO-8601 или 'YYYY-MM-DD HH:MM:SS' (UTC), "
            f"получено: {raw!r}"
        ) from e

    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _leads_list_url(account_domain: str, since_updated_at: datetime | None) -> str:
    """
    GET /api/v4/leads с пагинацией; при since — filter[updated_at][from|to] (unix, UTC).
    """
    base = f"{account_domain.rstrip('/')}/api/v4/leads"
    params: list[tuple[str, str]] = [
        ("limit", "250"),
        ("order[updated_at]", "asc"),
    ]
    if since_updated_at is not None:
        ts_from = int(since_updated_at.timestamp())
        ts_to = int(time.time())
        params.append(("filter[updated_at][from]", str(ts_from)))
        params.append(("filter[updated_at][to]", str(ts_to)))
    q = urlencode(params)
    return f"{base}?{q}"


def _merge_next_url(account_domain: str, next_href: str | None) -> str | None:
    """
    Ссылка next от amo может быть относительной; нормализуем к абсолютному URL с тем же хостом.
    """
    if not next_href:
        return None
    href = next_href.strip()
    if href.startswith("http://") or href.startswith("https://"):
        return href
    return f"{account_domain.rstrip('/')}/{href.lstrip('/')}"


def main():
    p = argparse.ArgumentParser(description="Выгрузка лидов из amoCRM в JSON (safe multi-client).")
    p.add_argument("--client-slug", required=True, help="client_slug (для выбора secrets и путей)")
    p.add_argument("--out", dest="out_path", default=None, help="Путь к выходному JSON")
    p.add_argument(
        "--since-updated-at",
        dest="since_updated_at",
        default=None,
        help="Инкремент: только лиды с updated_at после этого момента (UTC: unix, ISO или YYYY-MM-DD HH:MM:SS). "
        "Без аргумента — полная выгрузка.",
    )
    args = p.parse_args()

    client_slug = args.client_slug
    default_out = BASE_DIR / "var" / "data" / client_slug / "add_leads_crm.json"
    out_file = Path(args.out_path) if args.out_path else default_out

    account_domain, access_token = get_valid_access_token(client_slug)

    since_dt: datetime | None = None
    if args.since_updated_at:
        since_dt = _parse_since_updated_at(args.since_updated_at)

    url = _leads_list_url(account_domain, since_dt)
    all_leads = []

    while url:
        data = get_json(url, access_token)
        leads = data.get("_embedded", {}).get("leads", [])
        all_leads.extend(leads)

        next_href = data.get("_links", {}).get("next", {}).get("href")
        url = _merge_next_url(account_domain, next_href)

    out_file.parent.mkdir(parents=True, exist_ok=True)
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(all_leads, f, ensure_ascii=False, indent=2)

    mode = "incremental (updated_at)" if since_dt is not None else "full"
    print(f"Готово. Режим: {mode}. Лидов выгружено: {len(all_leads)}")
    print("Файл:", out_file)


if __name__ == "__main__":
    main()
