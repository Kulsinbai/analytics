import sys
from pathlib import Path
import os
import argparse

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from scripts.load_dev_env import load_local_env_files

load_local_env_files()

import pandas as pd
import clickhouse_connect
from pathlib import Path
from datetime import datetime, timezone
from scripts.clients_map import get_client_id

BASE_DIR = Path(__file__).resolve().parent.parent

DEFAULT_LEADS_CSV = BASE_DIR / "data" / "add_leads_crm_flat_datalens.csv"
DEFAULT_TABLE = "leads_fact"


def parse_dt(series: pd.Series) -> pd.Series:
    """
    Парсим 'YYYY-MM-DD HH:MM:SS' в datetime (без таймзоны),
    чтобы ClickHouse (DateTime) принял без проблем.
    """
    dt = pd.to_datetime(series, errors="coerce")
    # ClickHouse DateTime обычно без tz, убираем timezone если вдруг появилась
    try:
        dt = dt.dt.tz_localize(None)
    except Exception:
        pass
    return dt


def validate_csv(path: Path, *, allow_empty: bool = False) -> pd.DataFrame | None:
    """
    Базовая валидация CSV перед удалением данных в ClickHouse.
    Проверяем:
    - файл существует;
    - не пустой;
    - содержит обязательные колонки, которые дальше используются.
    """
    if not path.exists():
        print(f"ERROR: CSV не найден: {path}")
        return None

    if path.stat().st_size == 0:
        print(f"ERROR: CSV пустой (size=0): {path}")
        return None

    try:
        df = pd.read_csv(path, sep=";", encoding="utf-8-sig")
    except Exception as e:
        print(f"ERROR: не удалось прочитать CSV {path}: {e}")
        return None

    if df.empty:
        if allow_empty:
            return df
        print(f"ERROR: CSV прочитан, но не содержит строк: {path}")
        return None

    required_cols = [
        "id",
        "created_dt",
        "updated_dt",
        "closed_dt",
        "status_id",
        "pipeline_id",
        "account_id",
        "responsible_user_id",
        "client_slug",
        "name",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        print(
            "ERROR: в CSV отсутствуют обязательные колонки.\n"
            f"Файл: {path}\n"
            f"Нет колонок: {', '.join(missing)}"
        )
        return None

    return df


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

    client = clickhouse_connect.get_client(
        host=host,
        port=port,
        username=user,
        password=password,
        database=db,
    )
    return client, db


def _assert_csv_matches_client(df: pd.DataFrame, client_id: int, client_slug: str) -> None:
    if "client_id" in df.columns:
        try:
            ids = pd.to_numeric(df["client_id"], errors="coerce").dropna().astype("int64").unique().tolist()
        except Exception:
            ids = []
        ids = [int(x) for x in ids if int(x) > 0]
        if ids and set(ids) != {client_id}:
            raise ValueError(
                f"CSV содержит client_id {sorted(set(ids))}, но ожидается только {client_id}. "
                "Остановлено, чтобы не повредить данные."
            )

    if "client_slug" in df.columns:
        slugs = df["client_slug"].dropna().astype(str).str.strip()
        uniq = sorted(set([s for s in slugs.tolist() if s]))
        if uniq and set(uniq) != {client_slug}:
            raise ValueError(
                f"CSV содержит client_slug {uniq}, но ожидается только '{client_slug}'. "
                "Остановлено, чтобы не повредить данные."
            )


def main():
    p = argparse.ArgumentParser(description="Загрузка leads_fact в ClickHouse (safe multi-client).")
    p.add_argument(
        "--client-slug",
        required=True,
        help="client_slug клиента (PostgreSQL / client_registry; fallback — clients_map)",
    )
    p.add_argument("--csv-path", default=str(DEFAULT_LEADS_CSV), help="Путь к CSV add_leads_crm_flat_datalens.csv")
    p.add_argument("--ch-table", default=DEFAULT_TABLE, help="Имя таблицы ClickHouse (без БД)")
    p.add_argument("--dry-run", action="store_true", help="Не выполнять DELETE/INSERT, только проверки и план действий")
    p.add_argument(
        "--incremental",
        action="store_true",
        help="Инкремент: DELETE только по lead_id из CSV (этот client_id), затем INSERT; без полного DELETE по client_id.",
    )
    args = p.parse_args()

    client_slug = args.client_slug
    client_id = get_client_id(client_slug)
    if not isinstance(client_id, int) or client_id <= 0:
        raise ValueError("client_id должен быть положительным целым числом.")

    leads_csv = Path(args.csv_path)
    incremental = bool(args.incremental)
    # 0) валидация CSV перед любыми изменениями в ClickHouse
    df = validate_csv(leads_csv, allow_empty=incremental)
    if df is None:
        # Лог уже выведен в validate_csv
        return

    if incremental and df.empty:
        print("OK. Incremental: CSV без строк — DELETE/INSERT не выполняются.")
        print("Client:", client_id, "| slug:", client_slug)
        return

    _assert_csv_matches_client(df, client_id=client_id, client_slug=client_slug)

    client, db = _clickhouse_client()
    table = args.ch_table
    full_table = f"{db}.{table}"

    # 1) перезаливка данных клиента (тестовый режим)
    if args.dry_run:
        print("DRY RUN")
        print("Target table:", full_table)
        print("Client:", client_slug, "id=", client_id)
        print("CSV:", leads_csv, "rows=", len(df))
        if incremental:
            print(
                "Planned:",
                f"DELETE WHERE client_id = {client_id} AND lead_id IN (... из CSV)",
                "+ INSERT rows",
            )
        else:
            print("Planned:", f"DELETE WHERE client_id = {client_id}", "+ INSERT rows")
        return

    if incremental:
        out_ids = pd.to_numeric(df.get("id"), errors="coerce")
        lead_ids = [int(x) for x in out_ids.dropna().unique().tolist() if int(x) > 0]
        if lead_ids:
            ids_csv = ",".join(str(i) for i in lead_ids)
            client.command(
                f"ALTER TABLE {full_table} DELETE WHERE client_id = {client_id} AND lead_id IN ({ids_csv})"
            )
        # если lead_ids пуст — нечего удалять и вставлять (не должно при непустом df)
    else:
        client.command(f"ALTER TABLE {full_table} DELETE WHERE client_id = {client_id}")

    etl_loaded_at = datetime.now(timezone.utc).replace(tzinfo=None)

    # 2) строим датафрейм ровно под схему ClickHouse
    out = pd.DataFrame()
    out["client_id"] = client_id

    # CSV.id -> CH.lead_id
    out["lead_id"] = pd.to_numeric(df.get("id"), errors="coerce").fillna(0).astype("int64")

    # created_at/updated_at/closed_at в CH будем заполнять из *_dt (они уже красивые)
    out["created_at"] = parse_dt(df.get("created_dt"))
    out["updated_at"] = parse_dt(df.get("updated_dt"))
    out["closed_at"] = parse_dt(df.get("closed_dt"))

    # оставим и сами *_dt как отдельные поля (если хочешь хранить их тоже)
    out["created_dt"] = parse_dt(df.get("created_dt"))
    out["updated_dt"] = parse_dt(df.get("updated_dt"))
    out["closed_dt"] = parse_dt(df.get("closed_dt"))

    # числовые поля
    out["status_id"] = pd.to_numeric(df.get("status_id"), errors="coerce")
    out["pipeline_id"] = pd.to_numeric(df.get("pipeline_id"), errors="coerce")
    out["loss_reason_id"] = pd.to_numeric(df.get("loss_reason_id"), errors="coerce")
    out["price"] = pd.to_numeric(df.get("price"), errors="coerce")
    out["account_id"] = pd.to_numeric(df.get("account_id"), errors="coerce")
    out["created_by"] = pd.to_numeric(df.get("created_by"), errors="coerce")
    out["updated_by"] = pd.to_numeric(df.get("updated_by"), errors="coerce")
    out["score"] = pd.to_numeric(df.get("score"), errors="coerce")

    # responsible_user_id -> manager_id
    out["manager_id"] = pd.to_numeric(df.get("responsible_user_id"), errors="coerce")

    # is_deleted -> UInt8 (0/1)
    if "is_deleted" in df.columns:
        out["is_deleted"] = pd.to_numeric(df["is_deleted"], errors="coerce").fillna(0).astype("int64")
    else:
        out["is_deleted"] = 0

    # строки
    str_cols = [
        "client_slug", "name", "utm_source", "utm_medium", "utm_campaign",
        "utm_content", "utm_term", "source", "phone", "email",
        "channel", "phone_from_name", "name_clean"
    ]
    for col in str_cols:
        if col in df.columns:
            out[col] = df[col].astype("string")
        else:
            out[col] = pd.Series([None] * len(out), dtype="string")

    out["etl_loaded_at"] = etl_loaded_at

    # 3) базовые фильтры качества
    out = out[out["lead_id"] > 0].copy()

    # NaN -> None для Nullable
    out = out.where(pd.notnull(out), None)

    # client_id — NOT NULL, фиксируем ЖЁСТКО
    out["client_id"] = client_id

    # 4) вставка
    client.insert_df(full_table, out)

    print("OK. Inserted rows:", len(out))
    print("Client:", client_id, "| slug:", client_slug)


if __name__ == "__main__":
    main()
