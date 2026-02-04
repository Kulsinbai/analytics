import csv
from datetime import datetime
from pathlib import Path

import clickhouse_connect
from scripts.clients_map import get_client_id

# ===== НАСТРОЙКИ CLICKHOUSE =====
CLICKHOUSE_HOST = "217.18.63.106"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = "gen_user"
CLICKHOUSE_PASSWORD = "tucxERGS+7SLVu"
CLICKHOUSE_DB = "default_db"

# ===== НАСТРОЙКИ ЗАГРУЗКИ =====
CLIENT_SLUG = "artroyal_detailing"
CLIENT_ID = get_client_id(CLIENT_SLUG)

TABLE = "loss_reasons_dim_v2"
CSV_PATH = Path(__file__).resolve().parent.parent / "data" / "loss_reasons.csv"


def parse_datetime(value):
    """
    CSV содержит строку вида '22.04.2025 8:47'
    Приводим к datetime для clickhouse_connect
    """
    if not value:
        return None

    for fmt in ("%d.%m.%Y %H:%M", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(value, fmt)
        except ValueError:
            continue

    raise ValueError(f"Не удалось распарсить дату: {value}")


def main():
    ch = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB,
    )

    rows = []
    with CSV_PATH.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")

        required = {
            "client_id", "client_slug",
            "loss_reason_id", "loss_reason_name",
            "created_at", "updated_at",
            "sort"
        }
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"CSV missing columns: {sorted(missing)}")

        for r in reader:
            rows.append([
                int(r["client_id"]),
                r["client_slug"],
                int(r["loss_reason_id"]),
                r["loss_reason_name"],
                parse_datetime(r["created_at"]),
                parse_datetime(r["updated_at"]),
                int(r["sort"]) if str(r["sort"]).strip() else 0,
            ])

    # очищаем данные клиента
    ch.command(f"ALTER TABLE {TABLE} DELETE WHERE client_id = {CLIENT_ID}")

    # вставляем
    ch.insert(
        TABLE,
        rows,
        column_names=[
            "client_id", "client_slug",
            "loss_reason_id", "loss_reason_name",
            "created_at", "updated_at",
            "sort"
        ],
    )

    print(f"OK. Inserted {len(rows)} rows into {CLICKHOUSE_DB}.{TABLE} for client_id={CLIENT_ID}")


if __name__ == "__main__":
    main()
