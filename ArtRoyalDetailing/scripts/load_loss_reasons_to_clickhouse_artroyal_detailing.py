import pandas as pd
import clickhouse_connect
from pathlib import Path

from scripts.clients_map import get_client_id

# ====== настройки клиента ======
CLIENT_SLUG = "artroyal_detailing"
CLIENT_ID = get_client_id(CLIENT_SLUG)

# ====== пути ======
BASE_DIR = Path(__file__).resolve().parent.parent
CSV_PATH = BASE_DIR / "data" / "loss_reasons.csv"

# ====== ClickHouse ======
CH_HOST = "217.18.63.106"
CH_PORT = 8123
CH_USER = "gen_user"
CH_PASSWORD = "tucxERGS+7SLVu"
CH_DB = "default_db"

CH_TABLE = "loss_reasons_dim_artroyal_detailing"


def main():
    if not CSV_PATH.exists():
        raise FileNotFoundError(f"Не найден CSV: {CSV_PATH}")

    client = clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASSWORD,
        database=CH_DB,
    )

    # 1) читаем CSV
    df = pd.read_csv(CSV_PATH, sep=";", encoding="utf-8-sig")

    # 2) чистим старые данные клиента (чтобы не плодить дубли)
    client.command(f"ALTER TABLE {CH_DB}.{CH_TABLE} DELETE WHERE client_id = {CLIENT_ID}")

    # 3) собираем dataframe под схему таблицы
    out = pd.DataFrame()

    out["client_id"] = CLIENT_ID
    out["client_slug"] = CLIENT_SLUG

    out["id"] = pd.to_numeric(df.get("id"), errors="coerce").fillna(0).astype("int64")
    out["name"] = df.get("name").astype("string")

    # created_at/updated_at/sort могут быть пустыми — делаем безопасно
    out["created_at"] = pd.to_numeric(df.get("created_at"), errors="coerce").fillna(0).astype("int64")
    out["updated_at"] = pd.to_numeric(df.get("updated_at"), errors="coerce").fillna(0).astype("int64")
    out["sort"] = pd.to_numeric(df.get("sort"), errors="coerce").fillna(0).astype("int64")

    # 4) минимальная валидация
    out = out[out["id"] > 0].copy()
    out = out.where(pd.notnull(out), None)

    # client_id NOT NULL
    out["client_id"] = CLIENT_ID
    out["client_slug"] = CLIENT_SLUG

    # 5) загрузка
    client.insert_df(f"{CH_DB}.{CH_TABLE}", out)

    print(f"OK. Inserted loss reasons: {len(out)} rows")
    print(f"Table: {CH_DB}.{CH_TABLE}")
    print(f"Client: {CLIENT_SLUG} ({CLIENT_ID})")


if __name__ == "__main__":
    main()