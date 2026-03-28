import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import json
import csv
import argparse
from pathlib import Path
from datetime import datetime, UTC

from scripts.clients_map import get_client_id

# Берём токен и делаем запросы через твой клиент (он сам refresh делает)
from scripts.amocrm_client import get_valid_access_token, get_json


BASE_DIR = Path(__file__).resolve().parent.parent


def main():
    p = argparse.ArgumentParser(description="Выгрузка pipelines/statuses из amoCRM в CSV (safe multi-client).")
    p.add_argument("--client-slug", required=True, help="client_slug из scripts/clients_map.py")
    p.add_argument("--out", dest="out_path", default=None, help="Путь к выходному CSV")
    args = p.parse_args()

    client_slug = args.client_slug
    client_id = get_client_id(client_slug)
    default_out = BASE_DIR / "var" / "data" / client_slug / "pipelines_statuses_dim.csv"
    out_csv = Path(args.out_path) if args.out_path else default_out

    account_domain, access_token = get_valid_access_token(client_slug)

    url = f"{account_domain}/api/v4/leads/pipelines"
    data = get_json(url, access_token)

    pipelines = data.get("_embedded", {}).get("pipelines", [])
    now_utc = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")

    rows = []
    for p in pipelines:
        pipeline_id = p.get("id")
        pipeline_name = p.get("name")

        statuses = p.get("_embedded", {}).get("statuses", [])
        for s in statuses:
            rows.append({
                "client_id": client_id,
                "client_slug": client_slug,
                "pipeline_id": pipeline_id,
                "pipeline_name": pipeline_name,
                "status_id": s.get("id"),
                "status_name": s.get("name"),
                "sort": s.get("sort"),
                "is_final": s.get("is_final"),
                "is_won": s.get("is_won"),
                "is_lost": s.get("is_lost"),
                "updated_at": now_utc,
            })

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with open(out_csv, "w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(
            f,
            fieldnames=[
                "client_id", "client_slug",
                "pipeline_id", "pipeline_name",
                "status_id", "status_name",
                "sort", "is_final", "is_won", "is_lost",
                "updated_at"
            ],
            delimiter=";",
        )
        w.writeheader()
        w.writerows(rows)

    print("Готово:", out_csv, "строк:", len(rows))


if __name__ == "__main__":
    main()