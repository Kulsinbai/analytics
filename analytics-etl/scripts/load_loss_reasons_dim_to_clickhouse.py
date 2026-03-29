import sys
from pathlib import Path
import os
import argparse

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

import csv
from datetime import datetime
from pathlib import Path

import clickhouse_connect
from scripts.clients_map import get_client_id

DEFAULT_TABLE = "loss_reasons_dim_v2"
DEFAULT_CSV_PATH = Path(__file__).resolve().parent.parent / "data" / "loss_reasons.csv"


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


def _env_required(name: str) -> str:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        raise ValueError(f"Не задан env {name} (обязателен).")
    return v


def _clickhouse_client():
    host = _env_required("CLICKHOUSE_HOST")
    port = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    user = _env_required("CLICKHOUSE_USER")
    password = _env_required("CLICKHOUSE_PASSWORD")
    db = _env_required("CLICKHOUSE_DB")

    ch = clickhouse_connect.get_client(
        host=host,
        port=port,
        username=user,
        password=password,
        database=db,
    )
    return ch, db


def main():
    p = argparse.ArgumentParser(description="Загрузка loss_reasons_dim в ClickHouse (safe multi-client).")
    p.add_argument(
        "--client-slug",
        required=True,
        help="client_slug клиента (PostgreSQL / client_registry; fallback — clients_map)",
    )
    p.add_argument("--csv-path", default=str(DEFAULT_CSV_PATH), help="Путь к CSV loss_reasons.csv")
    p.add_argument("--ch-table", default=DEFAULT_TABLE, help="Имя таблицы ClickHouse (без БД)")
    p.add_argument("--dry-run", action="store_true", help="Не выполнять DELETE/INSERT, только проверки и план действий")
    args = p.parse_args()

    client_slug = args.client_slug
    client_id = get_client_id(client_slug)
    if not isinstance(client_id, int) or client_id <= 0:
        raise ValueError("client_id должен быть положительным целым числом.")

    csv_path = Path(args.csv_path)
    if not csv_path.exists() or csv_path.stat().st_size == 0:
        raise FileNotFoundError(f"CSV не найден или пустой: {csv_path}")

    ch, db = _clickhouse_client()
    table = args.ch_table
    full_table = f"{db}.{table}"

    rows = []
    seen_client_ids = set()
    seen_client_slugs = set()

    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
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
            try:
                cid = int(r["client_id"])
            except Exception:
                cid = -1
            cslug = str(r.get("client_slug", "")).strip()
            if cid > 0:
                seen_client_ids.add(cid)
            if cslug:
                seen_client_slugs.add(cslug)

            rows.append([
                client_id,
                client_slug,
                int(r["loss_reason_id"]),
                r["loss_reason_name"],
                parse_datetime(r["created_at"]),
                parse_datetime(r["updated_at"]),
                int(r["sort"]) if str(r["sort"]).strip() else 0,
            ])

    if not rows:
        raise ValueError(f"CSV не содержит строк данных: {csv_path}")

    if seen_client_ids and (seen_client_ids != {client_id}):
        raise ValueError(
            f"CSV содержит client_id {sorted(seen_client_ids)}, но ожидается только {client_id}. "
            "Остановлено, чтобы не повредить данные."
        )
    if seen_client_slugs and (seen_client_slugs != {client_slug}):
        raise ValueError(
            f"CSV содержит client_slug {sorted(seen_client_slugs)}, но ожидается только '{client_slug}'. "
            "Остановлено, чтобы не повредить данные."
        )

    if args.dry_run:
        print("DRY RUN")
        print("Target table:", full_table)
        print("Client:", client_slug, "id=", client_id)
        print("CSV:", csv_path, "rows=", len(rows))
        print("Planned:", f"DELETE WHERE client_id = {client_id}", "+ INSERT rows")
        return

    # очищаем данные клиента
    ch.command(f"ALTER TABLE {full_table} DELETE WHERE client_id = {client_id}")

    # вставляем
    ch.insert(
        full_table,
        rows,
        column_names=[
            "client_id", "client_slug",
            "loss_reason_id", "loss_reason_name",
            "created_at", "updated_at",
            "sort"
        ],
    )

    print(f"OK. Inserted {len(rows)} rows into {full_table} for client_id={client_id}")


if __name__ == "__main__":
    main()
