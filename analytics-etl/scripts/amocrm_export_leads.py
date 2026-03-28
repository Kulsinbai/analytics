import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import json
import argparse
from pathlib import Path
from scripts.amocrm_client import get_valid_access_token, get_json

BASE_DIR = Path(__file__).resolve().parent.parent


def main():
    p = argparse.ArgumentParser(description="Выгрузка лидов из amoCRM в JSON (safe multi-client).")
    p.add_argument("--client-slug", required=True, help="client_slug (для выбора secrets и путей)")
    p.add_argument("--out", dest="out_path", default=None, help="Путь к выходному JSON")
    args = p.parse_args()

    client_slug = args.client_slug
    default_out = BASE_DIR / "var" / "data" / client_slug / "add_leads_crm.json"
    out_file = Path(args.out_path) if args.out_path else default_out

    account_domain, access_token = get_valid_access_token(client_slug)

    url = f"{account_domain}/api/v4/leads?limit=250"
    all_leads = []

    while url:
        data = get_json(url, access_token)
        leads = data.get("_embedded", {}).get("leads", [])
        all_leads.extend(leads)

        url = data.get("_links", {}).get("next", {}).get("href")

    out_file.parent.mkdir(parents=True, exist_ok=True)
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(all_leads, f, ensure_ascii=False, indent=2)

    print(f"Готово. Лидов выгружено: {len(all_leads)}")
    print("Файл:", out_file)


if __name__ == "__main__":
    main()
