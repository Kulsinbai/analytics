import pandas as pd
import clickhouse_connect
from pathlib import Path
from datetime import datetime, timezone

BASE_DIR = Path(__file__).resolve().parent.parent
CSV_PATH = BASE_DIR / "data" / "pipelines_statuses_dim.csv"

CH_DB = "default_db"
CH_TABLE = "statuses_dim_artroyal_detailing"

CH_HOST = "217.18.63.106"
CH_PORT = 8123
CH_USER = "gen_user"
CH_PASSWORD = "tucxERGS+7SLVu"

CLIENT_ID = 1

def main():
    client = clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASSWORD,
        database=CH_DB,
    )

    if not CSV_PATH.exists():
        raise FileNotFoundError(f"Не найден CSV: {CSV_PATH}")

    df = pd.read_csv(CSV_PATH, sep=";", encoding="utf-8-sig")

    # 1) удаляем старые строки этого клиента (чтобы не было дублей)
    client.command(f"ALTER TABLE {CH_DB}.{CH_TABLE} DELETE WHERE client_id = {CLIENT_ID}")

    # 2) приводим к схеме таблицы
    out = pd.DataFrame()
    out["client_id"] = pd.to_numeric(df.get("client_id"), errors="coerce").fillna(CLIENT_ID).astype("int64")
    out["client_slug"] = df.get("client_slug").astype("string")

    out["pipeline_id"] = pd.to_numeric(df.get("pipeline_id"), errors="coerce").fillna(0).astype("int64")
    out["pipeline_name"] = df.get("pipeline_name").astype("string")

    out["status_id"] = pd.to_numeric(df.get("status_id"), errors="coerce").fillna(0).astype("int64")
    out["status_name"] = df.get("status_name").astype("string")

    out["sort"] = pd.to_numeric(df.get("sort"), errors="coerce").fillna(0).astype("int64")

    # эти поля часто bool/0/1/None — делаем аккуратно
    for col in ["is_final", "is_won", "is_lost"]:
        if col in df.columns:
            out[col] = pd.to_numeric(df.get(col), errors="coerce")
        else:
            out[col] = None

    # 3) фильтр качества: без мусора
    out = out[(out["status_id"] > 0) & (out["pipeline_id"] > 0)].copy()

    # NaN -> None
    out = out.where(pd.notnull(out), None)

    # 4) вставка
    client.insert_df(f"{CH_DB}.{CH_TABLE}", out)

    print("OK. Inserted statuses:", len(out))


if __name__ == "__main__":
    main()