import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import csv
import argparse
from pathlib import Path
from datetime import datetime, UTC

from scripts.amocrm_client import get_valid_access_token, get_json, AmoClientError
from scripts.clients_map import get_client_id

BASE_DIR = Path(__file__).resolve().parent.parent

def unix_to_dt_str(value):
    """
    amoCRM отдаёт created_at/updated_at как Unix timestamp (секунды).
    Превращаем в строку 'YYYY-MM-DD HH:MM:SS' (UTC).
    """
    if value is None or value == "":
        return ""
    ts = int(float(str(value).replace(",", ".")))
    return datetime.fromtimestamp(ts, tz=UTC).strftime("%Y-%m-%d %H:%M:%S")


def main():
    try:
        p = argparse.ArgumentParser(description="Выгрузка loss_reasons из amoCRM в CSV (safe multi-client).")
        p.add_argument(
            "--client-slug",
            required=True,
            help="client_slug клиента (PostgreSQL / client_registry; fallback — clients_map)",
        )
        p.add_argument("--out", dest="out_path", default=None, help="Путь к выходному CSV")
        args = p.parse_args()

        client_slug = args.client_slug
        client_id = get_client_id(client_slug)
        default_out = BASE_DIR / "var" / "data" / client_slug / "loss_reasons.csv"
        out_path = Path(args.out_path) if args.out_path else default_out

        account_domain, access_token = get_valid_access_token(client_slug)

        url = f"{account_domain}/api/v4/leads/loss_reasons"
        data = get_json(url, access_token)

        items = data.get("_embedded", {}).get("loss_reasons", [])
        if not items:
            print("Справочник loss_reasons пуст или не найден в ответе API.")
            print("Ответ:", data)
            return

        out_path.parent.mkdir(parents=True, exist_ok=True)

        fieldnames = [
            "client_id", "client_slug",
            "loss_reason_id", "loss_reason_name",
            "created_at", "updated_at",
            "sort"
        ]

        with open(out_path, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=fieldnames,
                delimiter=";"
            )
            writer.writeheader()

            for it in items:
                writer.writerow({
                    "client_id": client_id,
                    "client_slug": client_slug,

                    "loss_reason_id": it.get("id"),
                    "loss_reason_name": it.get("name"),
                    "created_at": unix_to_dt_str(it.get("created_at")),
                    "updated_at": unix_to_dt_str(it.get("updated_at")),
                    "sort": it.get("sort")
                })

        print("CSV выгружен:", out_path)
        print(f"Записей: {len(items)}")

    except AmoClientError as e:
        print("Ошибка клиента amoCRM:", e)


if __name__ == "__main__":
    main()
