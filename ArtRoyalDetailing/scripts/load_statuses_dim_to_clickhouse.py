import csv
from datetime import datetime
from pathlib import Path

import clickhouse_connect

from scripts.clients_map import get_client_id

# ====== НАСТРОЙКИ CLICKHOUSE ======
CLICKHOUSE_HOST = "217.18.63.106"
CLICKHOUSE_PORT = 8123
CLICKHOUSE_USER = "gen_user"
CLICKHOUSE_PASSWORD = "tucxERGS+7SLVu"
CLICKHOUSE_DB = "default_db"

# ====== НАСТРОЙКИ ЗАГРУЗКИ ======
CLIENT_SLUG = "artroyal_detailing"
CLIENT_ID = get_client_id(CLIENT_SLUG)

TABLE = "statuses_dim_v2"
CSV_PATH = Path(__file__).resolve().parent.parent / "data" / "pipelines_statuses_dim.csv"


def parse_uint8(value) -> int:
    v = str(value).strip().lower()
    return 1 if v in ("1", "true", "yes", "y") else 0


from datetime import datetime

def parse_datetime(value):
    if value and str(value).strip():
        return datetime.strptime(
            value.replace("T", " ").split("+")[0].strip(),
            "%Y-%m-%d %H:%M:%S"
        )
    return datetime.utcnow()


def main() -> None:
    ch = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB,
    )

    # 1) Читаем CSV (delimiter=';')
    rows = []
    with CSV_PATH.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=";")

        required = {
            "client_id", "client_slug",
            "pipeline_id", "pipeline_name",
            "status_id", "status_name",
            "sort", "is_final", "is_won", "is_lost",
            "updated_at"
        }
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"CSV missing columns: {sorted(missing)}")

        for r in reader:
            rows.append([
                int(r["client_id"]),
                r["client_slug"],
                int(r["pipeline_id"]),
                r["pipeline_name"],
                int(r["status_id"]),
                r["status_name"],
                int(r["sort"]) if str(r["sort"]).strip() else 0,
                parse_uint8(r["is_final"]),
                parse_uint8(r["is_won"]),
                parse_uint8(r["is_lost"]),
                parse_datetime(r["updated_at"]),
            ])

    # 2) Чистим строки клиента
    ch.command(f"ALTER TABLE {TABLE} DELETE WHERE client_id = {CLIENT_ID}")

    # 3) Вставляем
    ch.insert(
        TABLE,
        rows,
        column_names=[
            "client_id", "client_slug",
            "pipeline_id", "pipeline_name",
            "status_id", "status_name",
            "sort", "is_final", "is_won", "is_lost",
            "updated_at"
        ],
    )

    print(f"OK. Inserted {len(rows)} rows into {CLICKHOUSE_DB}.{TABLE} for client_id={CLIENT_ID}")


if __name__ == "__main__":
    main()
