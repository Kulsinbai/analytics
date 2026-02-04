import json
import csv
from pathlib import Path
from datetime import datetime

CLIENT_SLUG = "artroyal_detailing"

from scripts.clients_map import get_client_id

CLIENT_ID = get_client_id(CLIENT_SLUG)

# Берём токен и делаем запросы через твой клиент (он сам refresh делает)
from scripts.amocrm_client import get_valid_access_token, get_json


BASE_DIR = Path(__file__).resolve().parent.parent
OUT_CSV = BASE_DIR / "data" / "pipelines_statuses_dim.csv"


def main():
    account_domain, access_token = get_valid_access_token()

    url = f"{account_domain}/api/v4/leads/pipelines"
    data = get_json(url, access_token)

    pipelines = data.get("_embedded", {}).get("pipelines", [])
    now_utc = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

    rows = []
    for p in pipelines:
        pipeline_id = p.get("id")
        pipeline_name = p.get("name")

        statuses = p.get("_embedded", {}).get("statuses", [])
        for s in statuses:
            rows.append({
                "client_id": CLIENT_ID,
                "client_slug": CLIENT_SLUG,
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

    with open(OUT_CSV, "w", newline="", encoding="utf-8-sig") as f:
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

    print("Готово:", OUT_CSV, "строк:", len(rows))


if __name__ == "__main__":
    main()